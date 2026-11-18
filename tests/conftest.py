import os
import random
import socket
import string
from typing import Any, AsyncGenerator, Generator, Literal, TypeVar
from urllib.parse import urlsplit, urlunsplit

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


@pytest.fixture
def failure_dsns(postgresql_dsn: str) -> dict[str, str]:
    """
    DSNs that each differ from the working one in exactly one component.

    Derived at run time so every failure-path test fails for the reason its name
    says, whatever server and credentials the session uses.

    :return: reason -> dsn, for ``wrong_password``, ``missing_database`` and
        ``closed_port``.
    """
    parts = urlsplit(postgresql_dsn)
    userinfo, _, hostport = parts.netloc.rpartition("@")
    user, sep, password = userinfo.partition(":")

    def rebuild(*, netloc: str, path: str) -> str:
        return urlunsplit((parts.scheme, netloc, path, parts.query, parts.fragment))

    return {
        # Derived from the real password so it can never match it.
        "wrong_password": rebuild(
            netloc=f"{user}:{password}-wrong@{hostport}"
            if sep
            else f"{user}:wrong@{hostport}",
            path=parts.path,
        ),
        "missing_database": rebuild(
            netloc=parts.netloc,
            path=f"/{parts.path.lstrip('/')}_missing",
        ),
        "closed_port": rebuild(
            netloc=f"{userinfo}@127.0.0.1:{_closed_local_port()}"
            if userinfo
            else f"127.0.0.1:{_closed_local_port()}",
            path=parts.path,
        ),
    }


class DriverHarness:
    """
    Driver under test plus the teardown every component fixture shares.

    Components register themselves after startup; on teardown each table is
    dropped and then its component is shut down, in that order.
    """

    def __init__(self, name: Literal["asyncpg", "psqlpy", "psycopg"]) -> None:
        self.name = name
        self._components: list[tuple[Any, str]] = []

    def track(self, component: Any, table_name: str) -> None:
        """Remember a started component so teardown can clean it up."""
        self._components.append((component, table_name))

    async def teardown(self) -> None:
        """Drop the tables and shut the components down."""
        for component, table_name in reversed(self._components):
            async with component.driver, component.driver.connection() as connection:
                await connection.execute(f"DROP TABLE {table_name}")
            await component.shutdown()


@pytest.fixture(params=["asyncpg", "psqlpy", "psycopg"])
async def driver(request: pytest.FixtureRequest) -> AsyncGenerator[DriverHarness, None]:
    """
    Driver under test.

    Skips the test when the driver package is not installed.
    """
    pytest.importorskip(request.param)
    harness = DriverHarness(request.param)
    yield harness
    await harness.teardown()


@pytest.fixture
async def result_backend(
    postgresql_dsn: str,
    postgres_table: str,
    driver: DriverHarness,
) -> AsyncGenerator[PostgresqlResultBackend[_ReturnType], None]:
    backend: PostgresqlResultBackend[_ReturnType] = PostgresqlResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        driver=driver.name,
        run_migrations=True,
    )
    await backend.startup()
    driver.track(backend, postgres_table)
    yield backend


@pytest.fixture
async def broker(
    postgresql_dsn: str,
    postgres_table: str,
    driver: DriverHarness,
) -> AsyncGenerator[PostgresqlBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    broker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
        driver=driver.name,
        run_migrations=True,
    )
    await broker.startup()
    driver.track(broker, postgres_table)
    yield broker


@pytest.fixture
async def scheduler_source(
    postgresql_dsn: str,
    postgres_table: str,
    driver: DriverHarness,
) -> AsyncGenerator[PostgresqlSchedulerSource, None]:
    """Fixture to set up and tear down the scheduler source."""
    scheduler = PostgresqlSchedulerSource(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        driver=driver.name,
        run_migrations=True,
    )
    await scheduler.startup()
    driver.track(scheduler, postgres_table)
    yield scheduler
