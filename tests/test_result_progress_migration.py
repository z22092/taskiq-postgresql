"""
Step 2 of the results table: ``progress`` is added and ``result`` may be NULL.

The first step that does more than add. A results table a released version
created (step 1) gets it from ``run_migrations=True`` or from the SQL, and a
component that does not migrate refuses to start on it until it has run.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest

from taskiq_postgresql import exceptions
from taskiq_postgresql.migrations import TableSchema, table_schema
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from tests.helpers import (
    close_component,
    column_is_not_null,
    fetch_on_server,
    plan_of,
    run_script,
    script_of,
)

pytestmark = pytest.mark.anyio

# The table as step 1 leaves it, then as step 2 does, by name, type and NOT NULL.
AT_STEP_1 = [
    ("task_id", "uuid", True),
    ("result", "bytea", True),
    ("created_at", "timestamp with time zone", True),
]
AT_STEP_2 = [
    ("task_id", "uuid", True),
    ("result", "bytea", False),
    ("created_at", "timestamp with time zone", True),
    ("progress", "bytea", False),
]


@pytest.fixture
async def table(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[str, None]:
    """The name of a results table the test makes, dropped after it."""
    connection = await asyncpg.connect(postgresql_dsn)
    try:
        yield postgres_table
    finally:
        await connection.execute(f"DROP TABLE IF EXISTS {postgres_table}")
        await connection.close()


def schema_of(table: str) -> TableSchema:
    """The description of the results table ``table``."""
    return table_schema("result_backend", table_name=table)


async def columns_of(dsn: str, table: str) -> list[tuple[str, str, bool]]:
    """The columns of ``table`` as the server holds them, in table order."""
    rows = await fetch_on_server(
        dsn,
        "SELECT attname::text, format_type(atttypid, atttypmod), attnotnull"
        " FROM pg_attribute"
        " WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped"
        " ORDER BY attnum",
        table,
    )
    return [(row[0], row[1], row[2]) for row in rows]


async def step_1_table(dsn: str, driver: str, table: str) -> None:
    """Create the results table as a released version does, with one old row."""
    await run_script(dsn, driver, script_of(schema_of(table).steps[0].statements))
    await fetch_on_server(
        dsn,
        f"INSERT INTO {table} (task_id, result) VALUES ($1, 'old')",  # noqa: S608
        uuid.uuid4(),
    )


def backend_of(
    dsn: str,
    table: str,
    driver: str,
    **options: object,
) -> PostgresqlResultBackend[object]:
    """A result backend on ``table``; it checks the table unless told to migrate."""
    return PostgresqlResultBackend(
        dsn=dsn,
        table_name=table,
        driver=driver,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        **options,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
    )


async def start(backend: PostgresqlResultBackend[object]) -> None:
    """Start ``backend``, then close what it opened."""
    try:
        await backend.startup()
    finally:
        await close_component(backend)


async def test_the_table_step_1_creates_is_what_the_test_starts_from(
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    await step_1_table(postgresql_dsn, driver, table)

    assert await columns_of(postgresql_dsn, table) == AT_STEP_1


async def test_migrating_adds_progress_and_lets_result_be_null(
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    await step_1_table(postgresql_dsn, driver, table)

    await start(backend_of(postgresql_dsn, table, driver, run_migrations=True))

    assert await columns_of(postgresql_dsn, table) == AT_STEP_2
    # plan_of runs its own event loop, so it runs in a thread here.
    plan = await asyncio.to_thread(plan_of, postgresql_dsn, [schema_of(table)])
    assert plan.is_current
    # The row an older release wrote is kept, a ready result with no progress.
    (row,) = await fetch_on_server(
        postgresql_dsn,
        f"SELECT result, progress FROM {table}",  # noqa: S608
    )
    assert (bytes(row["result"]), row["progress"]) == (b"old", None)


async def test_a_second_startup_changes_nothing(
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    await step_1_table(postgresql_dsn, driver, table)
    await start(backend_of(postgresql_dsn, table, driver, run_migrations=True))
    first = await columns_of(postgresql_dsn, table)

    await start(backend_of(postgresql_dsn, table, driver, run_migrations=True))

    assert await columns_of(postgresql_dsn, table) == first == AT_STEP_2


async def test_a_new_table_is_made_by_the_two_steps(
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    await start(backend_of(postgresql_dsn, table, driver, run_migrations=True))

    assert await columns_of(postgresql_dsn, table) == AT_STEP_2


async def test_the_step_runs_twice_without_error_on_a_column_already_nullable(
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    schema = schema_of(table)
    await step_1_table(postgresql_dsn, driver, table)
    step_2 = script_of(schema.steps[1].statements)

    # DROP NOT NULL on a column that accepts NULL is a no-op, and ADD COLUMN
    # IF NOT EXISTS skips the column: the second run is silent.
    await run_script(postgresql_dsn, driver, step_2)
    await run_script(postgresql_dsn, driver, step_2)

    assert await columns_of(postgresql_dsn, table) == AT_STEP_2
    assert not await column_is_not_null(postgresql_dsn, table, "result")


@pytest.mark.parametrize("store_progress", [True, False])
async def test_without_run_migrations_a_table_of_step_1_stops_the_backend(
    store_progress: bool,
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    await step_1_table(postgresql_dsn, driver, table)
    backend = backend_of(
        postgresql_dsn,
        table,
        driver,
        run_migrations=False,
        store_progress=store_progress,
    )

    try:
        with pytest.raises(exceptions.SchemaNotReadyError) as raised:
            await backend.startup()
    finally:
        await close_component(backend)

    # The flag that gates the writes does not change what the table needs.
    (pending,) = raised.value.pending
    assert (pending.step.number, pending.missing_columns) == (2, ("progress",))
    assert pending.not_null_columns == ("result",)
    message = str(raised.value)
    for text in (
        table,
        "step 2",
        "missing columns: progress",
        "columns still NOT NULL: result",
    ):
        assert text in message
    assert "step 1" not in message
    # Only the catalog was read: nothing ran DDL.
    assert await columns_of(postgresql_dsn, table) == AT_STEP_1


async def test_without_run_migrations_the_backend_starts_once_step_2_has_run(
    postgresql_dsn: str,
    driver: str,
    table: str,
) -> None:
    schema = schema_of(table)
    await step_1_table(postgresql_dsn, driver, table)
    await run_script(postgresql_dsn, driver, script_of(schema.steps[1].statements))

    await start(backend_of(postgresql_dsn, table, driver, run_migrations=False))

    assert await columns_of(postgresql_dsn, table) == AT_STEP_2
