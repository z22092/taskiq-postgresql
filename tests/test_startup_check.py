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
from functools import partial
from typing import AsyncGenerator

import asyncpg
import pytest

from taskiq_postgresql import exceptions
from taskiq_postgresql.drivers import _schema
from taskiq_postgresql.migrations import CatalogState, render_sql, table_schema
from tests.helpers import (
    Component,
    build_component,
    column_is_not_null,
    run_script,
    script_of,
)
from tests.relax_schema import relaxing_schema
from tests.test_migrations import (
    BROKER_TABLE_0_4_SQL,
    claim_schema,
    close_component,
)

pytestmark = pytest.mark.anyio

KINDS = ("broker", "result_backend", "scheduler_source")
# What the error tells a person to run; the module path is theirs to fill in.
PLAN_COMMAND = "taskiq postgresql plan"
ALEMBIC_COMMAND = "alembic revision --autogenerate"


# The components under test check their table: they do not migrate.
build = partial(build_component, run_migrations=False)


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
        build_component(kind, postgresql_dsn, postgres_table, driver),
        caplog,
    )
    # The read that found the table missing.
    assert len(catalog_reads) == 1

    await start(build(kind, postgresql_dsn, postgres_table, driver), caplog)

    assert package_warnings(caplog) == []
    assert len(catalog_reads) == 2  # noqa: PLR2004


async def test_a_step_that_relaxes_not_null_stops_a_component_until_it_has_run(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = relaxing_schema(postgres_table)
    # No released step relaxes a column, so the component checks a table that
    # a test describes: the flow is the one every component runs.
    monkeypatch.setattr(_schema, "described_table", lambda _driver: schema)
    await run_script(postgresql_dsn, driver, script_of(schema.steps[0].statements))

    component = build("result_backend", postgresql_dsn, postgres_table, driver)
    try:
        with pytest.raises(exceptions.SchemaNotReadyError) as raised:
            await component.startup()
    finally:
        await close_component(component)

    message = str(raised.value)
    (pending,) = raised.value.pending
    assert (pending.step.number, pending.not_null_columns) == (2, ("note",))
    for text in (
        postgres_table,
        "step 2",
        "columns still NOT NULL: note",
        PLAN_COMMAND,
        ALEMBIC_COMMAND,
    ):
        assert text in message
    assert "step 1" not in message
    # It only read the catalog: without run_migrations nothing runs DDL.
    assert await column_is_not_null(postgresql_dsn, postgres_table, "note")

    await run_script(postgresql_dsn, driver, script_of(schema.steps[1].statements))
    await start(build("result_backend", postgresql_dsn, postgres_table, driver), caplog)

    assert package_warnings(caplog) == []
