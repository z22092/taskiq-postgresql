import os
import random
import string
from typing import AsyncGenerator, Generator, TypeVar

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


@pytest.fixture(params=["asyncpg", "psqlpy", "psycopg"])
async def result_backend(
    postgresql_dsn: str,
    postgres_table: str,
    request: pytest.FixtureRequest,
) -> AsyncGenerator[PostgresqlResultBackend[_ReturnType], None]:
    backend: PostgresqlResultBackend[_ReturnType] = PostgresqlResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        driver=request.param,
        run_migrations=True,
    )
    await backend.startup()
    yield backend

    async with backend.driver, backend.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {postgres_table}")

    await backend.shutdown()


@pytest.fixture(params=["asyncpg", "psqlpy", "psycopg"])
async def broker(
    postgresql_dsn: str,
    postgres_table: str,
    request: pytest.FixtureRequest,
) -> AsyncGenerator[PostgresqlBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    broker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
        driver=request.param,
        run_migrations=True,
    )
    await broker.startup()
    yield broker

    async with broker.driver, broker.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {postgres_table}")

    await broker.shutdown()


@pytest.fixture(params=["asyncpg", "psqlpy", "psycopg"])
async def scheduler_source(
    postgresql_dsn: str,
    postgres_table: str,
    request: pytest.FixtureRequest,
) -> AsyncGenerator[PostgresqlSchedulerSource, None]:
    """Fixture to set up and tear down the scheduler source."""
    scheduler = PostgresqlSchedulerSource(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        driver=request.param,
        run_migrations=True,
    )
    await scheduler.startup()
    yield scheduler

    # Clean up: drop the table
    async with scheduler.driver, scheduler.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {postgres_table}")

    await scheduler.shutdown()
