import os
import random
import socket
import string
from typing import AsyncGenerator, Generator, NamedTuple, Optional, TypeVar, Union
from urllib.parse import SplitResult, urlsplit

import pytest
from testcontainers.community.postgres import PostgresContainer

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

_ReturnType = TypeVar("_ReturnType")

# Kept on one line: scripts/check_pins.py matches it with a regex.
POSTGRES_IMAGE = "postgres:17.10@sha256:7958605b474b3d264a969cb3a123d6aa00ad1e1fe9da8a69984dabb704d93317"  # noqa: E501


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def postgres_table() -> str:
    """
    Name of a postgresql table for current test.

    :return: random string.
    """
    return "".join(
        random.choice(
            string.ascii_lowercase,
        )
        for _ in range(10)
    )


@pytest.fixture(scope="session")
def postgresql_dsn() -> Generator[str, None, None]:
    """
    DSN to PostgreSQL.

    Uses the ``POSTGRESQL_URL`` environment variable when it is set, otherwise
    starts a throwaway PostgreSQL container for the whole test session.

    :yield: dsn to PostgreSQL.
    """
    external_dsn = os.environ.get("POSTGRESQL_URL")
    if external_dsn:
        yield external_dsn
        return

    # The DSN is built from the credentials the container itself holds.
    with PostgresContainer(POSTGRES_IMAGE) as container:
        yield container.get_connection_url(driver=None)


def _closed_local_port() -> int:
    """Return a loopback port nothing listens on (never the default 5432)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _with_netloc(
    parts: SplitResult,
    *,
    password: Optional[str],
    host: str,
    port: Optional[int],
) -> SplitResult:
    """Rebuild ``parts`` with the given password and address, keeping the user."""
    userinfo = parts.username or ""
    if password is not None:
        userinfo = f"{userinfo}:{password}"
    address = host if port is None else f"{host}:{port}"
    return parts._replace(netloc=f"{userinfo}@{address}" if userinfo else address)


class FailureDsns(NamedTuple):
    """Reason -> DSN map for the connection failure-path tests."""

    # None when the session DSN carries no password: that server does not check
    # passwords, so no wrong one can be rejected and the test has to skip.
    wrong_password: Optional[str]
    missing_database: str
    closed_port: str
    invalid_scheme: str


@pytest.fixture
def failure_dsns(postgresql_dsn: str) -> FailureDsns:
    """
    DSNs that each differ from the working one in exactly one component.

    Derived at run time so every failure-path test fails for the reason its name
    says.

    :return: one DSN per failure reason.
    """
    parts = urlsplit(postgresql_dsn)
    wrong_password = (
        None
        if parts.password is None
        # Derived from the real password so it can never match it.
        else _with_netloc(
            parts,
            password=f"{parts.password}-wrong",
            host=parts.hostname or "",
            port=parts.port,
        ).geturl()
    )
    return FailureDsns(
        wrong_password=wrong_password,
        missing_database=parts._replace(
            path=f"/{parts.path.lstrip('/')}_missing",
        ).geturl(),
        closed_port=_with_netloc(
            parts,
            password=parts.password,
            host="127.0.0.1",
            port=_closed_local_port(),
        ).geturl(),
        invalid_scheme="invalid://connection/string",
    )


@pytest.fixture(params=["asyncpg", "psqlpy", "psycopg"])
def driver(request: pytest.FixtureRequest) -> str:
    """
    Name of the driver under test.

    Skips the test when the driver package is not installed.
    """
    pytest.importorskip(request.param)
    return request.param


async def _drop_and_shutdown(
    component: Union[
        PostgresqlResultBackend[_ReturnType],
        PostgresqlBroker,
        PostgresqlSchedulerSource,
    ],
    table: str,
) -> None:
    """Drop the component's table, then shut the component down."""
    async with component.driver, component.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {table}")
    await component.shutdown()


@pytest.fixture
async def result_backend(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> AsyncGenerator[PostgresqlResultBackend[_ReturnType], None]:
    backend: PostgresqlResultBackend[_ReturnType] = PostgresqlResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        driver=driver,
        run_migrations=True,
    )
    await backend.startup()
    yield backend
    await _drop_and_shutdown(backend, postgres_table)


@pytest.fixture
async def broker(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> AsyncGenerator[PostgresqlBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    broker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
        driver=driver,
        run_migrations=True,
    )
    await broker.startup()
    yield broker
    await _drop_and_shutdown(broker, postgres_table)


@pytest.fixture
async def scheduler_source(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> AsyncGenerator[PostgresqlSchedulerSource, None]:
    """Fixture to set up and tear down the scheduler source."""
    scheduler = PostgresqlSchedulerSource(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        driver=driver,
        run_migrations=True,
    )
    await scheduler.startup()
    yield scheduler
    await _drop_and_shutdown(scheduler, postgres_table)
