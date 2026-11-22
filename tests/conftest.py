import os
import random
import string
from typing import AsyncGenerator, Generator, TypeVar
from urllib.parse import urlsplit

import pytest
from testcontainers.community.postgres import PostgresContainer

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import (
    address_of,
    closed_local_port,
    drop_and_shutdown,
    with_netloc,
)

_ReturnType = TypeVar("_ReturnType")

# One pinned image per supported major. Each entry stays on one line:
# scripts/check_pins.py matches them with a regex.
POSTGRES_IMAGES = {
    "17": "postgres:17.10@sha256:7958605b474b3d264a969cb3a123d6aa00ad1e1fe9da8a69984dabb704d93317",  # noqa: E501
    "18": "postgres:18.4@sha256:a02db8cac496f15b094798a38254f14d6e00741f709360e5e00bb6668ea31636",  # noqa: E501
}
DEFAULT_POSTGRES_MAJOR = "18"


def postgres_image() -> str:
    """
    Pinned image for the major named by ``TEST_POSTGRES_MAJOR``.

    CI sets the variable to an empty string on the legs that keep the default,
    so unset and empty both mean the default major.

    :raises pytest.UsageError: when the value is not a supported major.
    :return: the pinned image reference.
    """
    major = os.environ.get("TEST_POSTGRES_MAJOR") or DEFAULT_POSTGRES_MAJOR
    if major not in POSTGRES_IMAGES:
        raise pytest.UsageError(
            f"TEST_POSTGRES_MAJOR={major!r} is not supported; "
            f"use one of {sorted(POSTGRES_IMAGES)}",
        )
    return POSTGRES_IMAGES[major]


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

    Uses the ``POSTGRESQL_URL`` environment variable when it is set (it wins
    over ``TEST_POSTGRES_MAJOR``), otherwise starts a throwaway PostgreSQL
    container for the whole test session.

    :yield: dsn to PostgreSQL.
    """
    external_dsn = os.environ.get("POSTGRESQL_URL")
    if external_dsn:
        yield external_dsn
        return

    # The DSN is built from the credentials the container itself holds.
    with PostgresContainer(postgres_image()) as container:
        yield container.get_connection_url(driver=None)


@pytest.fixture
def missing_database_dsn(postgresql_dsn: str) -> str:
    """
    DSN of a database that does not exist on the working server.

    :return: the session DSN with ``_missing`` appended to the database name.
    """
    parts = urlsplit(postgresql_dsn)
    return parts._replace(path=f"/{parts.path.lstrip('/')}_missing").geturl()


@pytest.fixture(params=["wrong_password", "closed_port", "invalid_scheme"])
def connection_failure_dsn(request: pytest.FixtureRequest, postgresql_dsn: str) -> str:
    """
    DSN that cannot connect, once per failure reason.

    ``wrong_password`` and ``closed_port`` are the session DSN with only the
    password or only the address replaced; ``invalid_scheme`` is a constant-shape
    DSN. Derived at run time so each test fails for the reason its id says.

    :return: a DSN that must raise a connection error.
    """
    parts = urlsplit(postgresql_dsn)
    if request.param == "wrong_password":
        if parts.password is None:
            pytest.skip(
                "the server under POSTGRESQL_URL does not check passwords; "
                "testcontainers always does",
            )
        # Derived from the real password so it can never match it.
        return with_netloc(
            parts,
            password=f"{parts.password}-wrong",
            address=address_of(parts),
        ).geturl()
    if request.param == "closed_port":
        return with_netloc(
            parts,
            password=parts.password,
            address=f"127.0.0.1:{closed_local_port()}",
        ).geturl()
    return "invalid://connection/string"


@pytest.fixture(params=["asyncpg", "psqlpy", "psycopg"])
def driver(request: pytest.FixtureRequest) -> str:
    """
    Name of the driver under test.

    Skips the test when the driver package is not installed.
    """
    pytest.importorskip(request.param)
    return request.param


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
    await drop_and_shutdown(backend, postgres_table)


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
    await drop_and_shutdown(broker, postgres_table)


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
    await drop_and_shutdown(scheduler, postgres_table)
