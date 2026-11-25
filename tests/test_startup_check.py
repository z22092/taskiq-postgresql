"""
The schema check ``startup()`` runs when a component does not migrate.

With ``run_migrations=False`` a component compares its table with the
table's description, in one read-only catalog query, before it starts. A
missing table, or a missing column that a migration step adds, stops it with
``SchemaNotReadyError``, which names what is missing and how to create it.
A missing index, or a problem no step fixes, is logged as a warning: the
component works without it.
"""

from __future__ import annotations

import logging
from typing import AsyncGenerator, Union

import asyncpg
import pytest

from taskiq_postgresql import exceptions
from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.migrations import CatalogState, render_sql, table_schema
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import run_script
from tests.test_migrations import (
    BROKER_TABLE_0_4_SQL,
    claim_schema,
    close_component,
)

pytestmark = pytest.mark.anyio

Component = Union[
    PostgresqlBroker,
    PostgresqlResultBackend[object],
    PostgresqlSchedulerSource,
]

KINDS = ("broker", "result_backend", "scheduler_source")
# What the error tells a person to run; the module path is theirs to fill in.
PLAN_COMMAND = "taskiq postgresql plan"
ALEMBIC_COMMAND = "alembic revision --autogenerate"


def build(
    kind: str,
    dsn: str,
    table: str,
    driver: str,
    *,
    run_migrations: bool = False,
) -> Component:
    """Return a component of ``kind`` on ``table``; it does not migrate by default."""
    if kind == "broker":
        return PostgresqlBroker(
            dsn=dsn,
            table_name=table,
            channel_name=f"{table}_channel",
            driver=driver,
            run_migrations=run_migrations,
        )
    if kind == "result_backend":
        return PostgresqlResultBackend(
            dsn=dsn,
            table_name=table,
            driver=driver,
            run_migrations=run_migrations,
        )
    return PostgresqlSchedulerSource(
        dsn=dsn,
        table_name=table,
        driver=driver,
        run_migrations=run_migrations,
    )


@pytest.fixture
async def admin(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[asyncpg.Connection, None]:
    """A superuser connection for setup and checks; it drops the test's tables."""
    connection = await asyncpg.connect(postgresql_dsn)
    try:
        yield connection
    finally:
        await connection.execute(
            f"DROP TABLE IF EXISTS {postgres_table}, {postgres_table}_other",
        )
        await connection.close()


@pytest.fixture
def catalog_reads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record each catalog query result read, as ``CatalogState.from_json`` reads it."""
    reads: list[str] = []
    read = CatalogState.from_json.__func__

    def counted(cls: type[CatalogState], text: str) -> CatalogState:
        reads.append(text)
        return read(cls, text)

    monkeypatch.setattr(CatalogState, "from_json", classmethod(counted))
    return reads


def package_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the warnings this package logged, without other libraries' ones."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.name.startswith("taskiq_postgresql")
    ]


async def start(component: Component, caplog: pytest.LogCaptureFixture) -> None:
    """Start ``component`` with warnings captured, then close what it opened."""
    try:
        with caplog.at_level(logging.WARNING):
            await component.startup()
    finally:
        await close_component(component)


@pytest.mark.parametrize("kind", KINDS)
async def test_a_missing_table_stops_a_component_that_does_not_migrate(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: asyncpg.Connection,
) -> None:
    component = build(kind, postgresql_dsn, postgres_table, driver)
    try:
        with pytest.raises(exceptions.SchemaNotReadyError) as raised:
            await component.startup()
    finally:
        await close_component(component)

    message = str(raised.value)
    assert raised.value.table == postgres_table
    assert all(item.table_missing for item in raised.value.pending)
    for text in (postgres_table, "step 1", PLAN_COMMAND, ALEMBIC_COMMAND):
        assert text in message
    # It only read the catalog: without run_migrations nothing runs DDL.
    assert await admin.fetchval("SELECT to_regclass($1) IS NULL", postgres_table)


async def test_a_0_4_broker_table_stops_a_broker_that_does_not_migrate(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: asyncpg.Connection,
) -> None:
    await admin.execute(BROKER_TABLE_0_4_SQL.format(table=postgres_table))
    broker = build("broker", postgresql_dsn, postgres_table, driver)
    try:
        with pytest.raises(exceptions.SchemaNotReadyError) as raised:
            await broker.startup()
    finally:
        await close_component(broker)

    # Without the check, startup() passed and the first kick() failed on
    # the missing channel column.
    message = str(raised.value)
    (pending,) = raised.value.pending
    assert (pending.step.number, pending.missing_columns) == (
        2,
        ("channel", "not_before"),
    )
    for text in (
        postgres_table,
        "step 2",
        "channel",
        "not_before",
        PLAN_COMMAND,
        ALEMBIC_COMMAND,
    ):
        assert text in message
    assert "step 1" not in message
    assert await claim_schema(admin, postgres_table) == ([], False)


@pytest.mark.parametrize("kind", KINDS)
async def test_a_missing_index_is_only_a_warning(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: asyncpg.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = table_schema(kind, table_name=postgres_table)
    index = schema.indexes[0].name
    await run_script(postgresql_dsn, driver, render_sql([schema]))
    await admin.execute(f"DROP INDEX {index}")

    await start(build(kind, postgresql_dsn, postgres_table, driver), caplog)

    (warning,) = package_warnings(caplog)
    assert postgres_table in warning
    assert index in warning


@pytest.mark.parametrize("kind", KINDS)
async def test_an_index_name_another_table_took_is_only_a_warning(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: asyncpg.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = table_schema(kind, table_name=postgres_table)
    index = schema.indexes[0].name
    await admin.execute(f"CREATE TABLE {postgres_table}_other (id INTEGER)")
    await admin.execute(f"CREATE INDEX {index} ON {postgres_table}_other (id)")
    # IF NOT EXISTS skips the index whose name the other table took.
    await run_script(postgresql_dsn, driver, render_sql([schema]))

    await start(build(kind, postgresql_dsn, postgres_table, driver), caplog)

    (warning,) = package_warnings(caplog)
    assert "index_on_other_table" in warning
    assert index in warning


@pytest.mark.parametrize("kind", KINDS)
async def test_a_complete_table_starts_quietly_after_one_catalog_read(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: asyncpg.Connection,
    caplog: pytest.LogCaptureFixture,
    catalog_reads: list[str],
) -> None:
    schema = table_schema(kind, table_name=postgres_table)
    await run_script(postgresql_dsn, driver, render_sql([schema]))

    await start(build(kind, postgresql_dsn, postgres_table, driver), caplog)

    assert package_warnings(caplog) == []
    (catalog,) = catalog_reads
    assert postgres_table in catalog


@pytest.mark.parametrize("kind", KINDS)
async def test_run_migrations_still_creates_the_table_and_reads_the_catalog_once(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: asyncpg.Connection,
    caplog: pytest.LogCaptureFixture,
    catalog_reads: list[str],
) -> None:
    await start(
        build(kind, postgresql_dsn, postgres_table, driver, run_migrations=True),
        caplog,
    )
    # The read that found the table missing.
    assert len(catalog_reads) == 1

    await start(build(kind, postgresql_dsn, postgres_table, driver), caplog)

    assert package_warnings(caplog) == []
    assert len(catalog_reads) == 2  # noqa: PLR2004
