"""
What the Alembic integration refuses to write or to run.

A hand-built ``TableSchema`` can hold text that a revision file would run as
Python, or a table name the SQL cannot hold; the lock and the lock bound of a
step only work inside a transaction, so an autocommit connection is refused.

Every test here carries the ``alembic`` mark: ``pytest -m alembic``.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any, Callable, Iterator

import pytest
from alembic import op
from alembic.autogenerate import render_python_code
from alembic.migration import MigrationContext
from alembic.operations import Operations, ops
from sqlalchemy import Connection, create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from taskiq_postgresql.contrib.alembic import (
    TaskiqStepOp,
    alembic_format,
    upgrade_taskiq_tables,
)
from taskiq_postgresql.migrations import (
    DEFAULT_LOCK_TIMEOUT,
    FormatOptions,
    table_schema,
)
from tests.test_contrib_alembic import (
    SQLALCHEMY_DRIVERS,
    bounded,
    offline_sql,
    revision_namespace,
)
from tests.test_contrib_sqlalchemy import databases, sqlalchemy_url

pytestmark = pytest.mark.alembic

# Python that must never reach a revision file.
INJECTED = "x\nimport os"
AUTOCOMMIT_WAYS = ("connection-option", "engine-option", "autocommit-block")
TRANSACTION_NOTE = "need the revision's transaction"


@pytest.fixture
def new_database(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Create empty databases for one test, and drop them after it."""
    yield from databases(postgresql_dsn)


def hand_built(**changes: Any) -> Any:
    """The broker description with ``changes`` applied, as a user could build it."""
    return dataclasses.replace(table_schema("broker"), **changes)


def with_step_number(number: Any) -> Any:
    broker = table_schema("broker")
    first = dataclasses.replace(broker.steps[0], number=number)
    return dataclasses.replace(broker, steps=(first, *broker.steps[1:]))


# --- a revision file never holds text a step operation was given -----------------


@pytest.mark.parametrize(
    ("table", "kind", "number", "error", "message"),
    [
        (INJECTED, "broker", 1, ValueError, "table name"),
        ("", "broker", 1, ValueError, "table name"),
        ("jobs", INJECTED, 1, ValueError, "component kind"),
        ("jobs", "workers", 1, ValueError, "component kind"),
        ("jobs", "broker", 0, ValueError, "step number"),
        ("jobs", "broker", -1, ValueError, "step number"),
        ("jobs", "broker", "1\nimport os", TypeError, "step number"),
        ("jobs", "broker", 1.0, TypeError, "step number"),
        ("jobs", "broker", True, TypeError, "step number"),
    ],
    ids=[
        "table-line-break",
        "table-empty",
        "kind-line-break",
        "kind-unknown",
        "number-zero",
        "number-negative",
        "number-text",
        "number-float",
        "number-bool",
    ],
)
def test_a_step_operation_refuses_what_its_comments_cannot_hold(
    table: str,
    kind: Any,
    number: Any,
    error: type[Exception],
    message: str,
) -> None:
    step = dataclasses.replace(table_schema("broker").steps[0], number=number)

    with pytest.raises(error, match=message):
        TaskiqStepOp(table, kind, step)


@pytest.mark.parametrize(
    "schema",
    [
        hand_built(name=INJECTED),
        hand_built(kind=INJECTED),
        with_step_number("1\nimport os"),
        with_step_number(0),
    ],
    ids=["table-line-break", "kind-line-break", "number-text", "number-zero"],
)
@pytest.mark.parametrize("downgrade", ["none", "drop"])
def test_the_alembic_format_refuses_a_hand_built_table_it_cannot_write(
    schema: Any,
    downgrade: str,
) -> None:
    written: list[str] = []

    with pytest.raises((ValueError, TypeError)):
        written.append(
            alembic_format([schema], FormatOptions(extra={"downgrade": downgrade})),
        )

    assert written == []


# --- the drops of the downgrade --------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["a, users", "jobs;DROP TABLE users", "1jobs", "has space", '"quoted"'],
    ids=["list", "statement", "digit-first", "space", "quote"],
)
@pytest.mark.parametrize("downgrade", ["none", "drop"])
def test_the_alembic_format_refuses_a_name_the_sqlalchemy_path_refuses(
    name: str,
    downgrade: str,
) -> None:
    with pytest.raises(ValueError, match="not a plain SQL identifier"):
        alembic_format(
            [hand_built(name=name)],
            FormatOptions(extra={"downgrade": downgrade}),
        )


@pytest.mark.parametrize("lock_timeout", [DEFAULT_LOCK_TIMEOUT, 3, 0])
def test_the_drops_of_the_downgrade_are_bounded_by_the_lock_timeout(
    lock_timeout: int,
) -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    source = alembic_format(
        schemas,
        FormatOptions(
            lock_timeout=lock_timeout,
            extra={"revision": "x", "downgrade": "drop"},
        ),
    )

    drops = [
        "DROP TABLE IF EXISTS taskiq_results",
        "DROP TABLE IF EXISTS taskiq_messages",
    ]
    printed = offline_sql(revision_namespace(source)["downgrade"])
    assert printed == "".join(
        f"{statement};\n\n" for statement in bounded(drops, lock_timeout)
    )
    if not lock_timeout:
        assert "lock_timeout" not in printed


# --- the steps need the revision's transaction -----------------------------------


def test_each_rendered_step_says_it_needs_the_revision_transaction() -> None:
    broker = table_schema("broker")

    source = alembic_format([broker], FormatOptions(extra={"revision": "x"}))
    rendered = render_python_code(
        ops.UpgradeOps([TaskiqStepOp(broker.name, broker.kind, broker.steps[0])]),
    )

    assert source.count(TRANSACTION_NOTE) == len(broker.steps)
    assert TRANSACTION_NOTE in rendered


def run_autocommit(dsn: str, sqlalchemy_driver: str, way: str) -> None:
    """Run ``upgrade_taskiq_tables`` on an autocommit connection."""

    def in_context(connection: Connection) -> None:
        if way == "connection-option":
            connection.execution_options(isolation_level="AUTOCOMMIT")
        context = MigrationContext.configure(connection)
        with Operations.context(context):
            if way == "autocommit-block":
                # Alembic's own switch, as a revision with ALTER TYPE ... ADD
                # VALUE makes it.
                with context.begin_transaction(), context.autocommit_block():
                    upgrade_taskiq_tables(op, table_schema("broker"), through=2)
            else:
                upgrade_taskiq_tables(op, table_schema("broker"), through=2)

    options = {"isolation_level": "AUTOCOMMIT"} if way == "engine-option" else {}
    url = sqlalchemy_url(dsn, sqlalchemy_driver)
    if sqlalchemy_driver == "asyncpg":

        async def run_async() -> None:
            engine = create_async_engine(url, **options)
            try:
                async with engine.connect() as connection:
                    await connection.run_sync(in_context)
            finally:
                await engine.dispose()

        asyncio.run(run_async())
        return
    engine = create_engine(url, **options)
    try:
        with engine.connect() as connection:
            in_context(connection)
    finally:
        engine.dispose()


@pytest.mark.parametrize("way", AUTOCOMMIT_WAYS)
@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
def test_upgrade_taskiq_tables_is_refused_on_an_autocommit_connection(
    sqlalchemy_driver: str,
    way: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()

    with pytest.raises(RuntimeError, match="transaction"):
        run_autocommit(dsn, sqlalchemy_driver, way)

    # Refused before any statement ran: an autocommit connection would have
    # left the table behind.
    assert asyncio.run(_tables_of(dsn)) == []


async def _tables_of(dsn: str) -> list[str]:
    import asyncpg  # noqa: PLC0415

    connection = await asyncpg.connect(dsn)
    try:
        rows = await connection.fetch(
            "SELECT tablename FROM pg_tables WHERE tablename LIKE 'taskiq%'",
        )
    finally:
        await connection.close()
    return [row["tablename"] for row in rows]


def test_upgrade_taskiq_tables_still_prints_its_script_offline() -> None:
    broker = table_schema("broker")

    printed = offline_sql(lambda: upgrade_taskiq_tables(op, broker, through=2))

    assert "CREATE TABLE IF NOT EXISTS taskiq_messages" in printed
