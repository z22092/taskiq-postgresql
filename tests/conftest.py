import os
import random
import string
from typing import AsyncGenerator, TypeVar

import pytest

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

_ReturnType = TypeVar("_ReturnType")


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


@pytest.fixture
def postgresql_dsn() -> str:
    """
    DSN to PostgreSQL.

    :return: dsn to PostgreSQL.
    """
    return os.environ.get("POSTGRESQL_URL") or "postgresql://root:secret@localhost:5432"


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
    )
    await scheduler.startup()
    yield scheduler

    # Clean up: drop the table
    async with scheduler.driver, scheduler.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {postgres_table}")

    await scheduler.shutdown()
