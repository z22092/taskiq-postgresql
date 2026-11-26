"""
The SQL catalog against a real PostgreSQL: its types, its words and its names.

ADR 0007 declares the type of every parameter and result column of a
statement, and ADR 0008 derives the names PostgreSQL gives a table's objects
and lists the words it refuses. Each test here has the server say the same.
"""

import re
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Optional

import asyncpg
import pytest

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.core.sql.catalog import (
    ClaimByIdQuery,
    ClaimColumns,
    ClaimDueQuery,
    Column,
    catalog_read,
    database_read,
    drop_table_script,
    exists_statement,
    index_name,
    notify_statement,
)
from taskiq_postgresql.core.sql.identifier import (
    RESERVED_WORDS,
    Identifier,
    IdentifierError,
    check_table_names,
    derived_names,
)
from taskiq_postgresql.core.sql.statement import ParamType, Statement
from taskiq_postgresql.migrations import migration_statements, table_schema
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import UNUSED_DSN

pytestmark = pytest.mark.anyio

Component = Any

# The one place that says which server types stand for a ParamType. TEXT takes
# both: a VARCHAR column is character varying, an expression is text.
REGTYPES: dict[ParamType, frozenset[str]] = {
    ParamType.TEXT: frozenset({"text", "character varying"}),
    ParamType.INT4: frozenset({"integer"}),
    ParamType.INT8: frozenset({"bigint"}),
    ParamType.FLOAT8: frozenset({"double precision"}),
    ParamType.BOOL: frozenset({"boolean"}),
    ParamType.JSONB: frozenset({"jsonb"}),
    ParamType.UUID: frozenset({"uuid"}),
    ParamType.BYTEA: frozenset({"bytea"}),
    ParamType.TIMESTAMPTZ: frozenset({"timestamp with time zone"}),
    ParamType.INT4_ARRAY: frozenset({"integer[]"}),
    ParamType.UUID_ARRAY: frozenset({"uuid[]"}),
    ParamType.TEXT_ARRAY: frozenset({"text[]", "character varying[]"}),
}

TASK_ID_TYPES = ("Uuid", "VarChar", "Text")
# Names this module creates; every test drops its own table before and after.
PREFIX = "sqlcat_"


def broker_of(table: str, task_id: str = "Uuid") -> PostgresqlBroker:
    """Build a broker on ``table``; it opens no connection."""
    return PostgresqlBroker(
        dsn=UNUSED_DSN,
        table_name=table,
        channel_name="sqlcat_channel",
        field_for_task_id=task_id,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
    )


def result_backend_of(
    table: str, task_id: str = "Uuid"
) -> PostgresqlResultBackend[Any]:
    """Build a result backend on ``table``; it opens no connection."""
    return PostgresqlResultBackend(
        dsn=UNUSED_DSN,
        table_name=table,
        field_for_task_id=task_id,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
    )


def scheduler_source_of(table: str) -> PostgresqlSchedulerSource:
    """Build a schedule source on ``table``; it opens no connection."""
    return PostgresqlSchedulerSource(dsn=UNUSED_DSN, table_name=table)


@dataclass(frozen=True)
class Variant:
    """One component on one table, as the statements below are built for it."""

    label: str
    table: str
    build: Callable[[str], Component]


VARIANTS = [
    *(
        Variant(
            f"broker-{task_id}",
            f"{PREFIX}broker_{task_id.lower()}",
            lambda table, task_id=task_id: broker_of(table, task_id),
        )
        for task_id in TASK_ID_TYPES
    ),
    # A $ in a name is part of it, not a placeholder.
    Variant("broker-jobs$5", "jobs$5", broker_of),
    *(
        Variant(
            f"result_backend-{task_id}",
            f"{PREFIX}results_{task_id.lower()}",
            lambda table, task_id=task_id: result_backend_of(table, task_id),
        )
        for task_id in TASK_ID_TYPES
    ),
    Variant("scheduler_source", f"{PREFIX}schedules", scheduler_source_of),
]

Cases = dict[str, Callable[[Component], Statement]]


def _broker_claims(broker: PostgresqlBroker) -> tuple[Any, ClaimColumns, list[Column]]:
    columns = broker.columns
    claims = ClaimColumns(channel=columns.channel, not_before=columns.not_before)
    return columns.primary_key, claims, [columns.message]


def _broker_cases() -> Cases:
    def inserted(broker: PostgresqlBroker) -> list[Column]:
        columns = broker.columns
        return [
            columns.task_id,
            columns.task_name,
            columns.message,
            columns.labels,
            columns.channel,
            columns.not_before,
        ]

    return {
        "insert_returning": lambda c: c.driver.insert_query.statement(
            inserted(c),
            [c.columns.primary_key],
        ),
        "insert": lambda c: c.driver.insert_query.statement(inserted(c)),
        "delete": lambda c: c.driver.delete_query.statement(c.columns.primary_key),
        "select_where": lambda c: c.driver.select_query.statement(
            [c.columns.message],
            [c.columns.primary_key],
        ),
        "claim_due": lambda c: ClaimDueQuery(c.table_name).statement(
            *_broker_claims(c),
        ),
        "claim_by_id": lambda c: ClaimByIdQuery(c.table_name).statement(
            *_broker_claims(c),
        ),
        "notify": lambda _c: notify_statement(),
    }


def _result_backend_cases() -> Cases:
    return {
        "insert_or_update": lambda c: c.driver.insert_or_update_query.statement(
            [c.columns.primary_key, c.columns.result],
            None,
            [c.columns.primary_key],
            "UPDATE",
            [c.columns.result],
        ),
        "insert_or_update_returning": lambda c: (
            c.driver.insert_or_update_query.statement(
                [c.columns.primary_key, c.columns.result],
                [c.columns.result],
                [c.columns.primary_key],
                "UPDATE",
                [c.columns.result],
            )
        ),
        "select_where": lambda c: c.driver.select_query.statement(
            [c.columns.result],
            [c.columns.primary_key],
        ),
        "delete": lambda c: c.driver.delete_query.statement(c.columns.primary_key),
        "delete_by_date": lambda c: c.driver.delete_by_date_query.statement(
            c.driver.created_at,
        ),
    }


def _scheduler_source_cases() -> Cases:
    return {
        "insert": lambda c: c.driver.insert_query.statement(
            [c.columns.primary_key, c.columns.task_name, c.columns.schedule],
        ),
        "select_all": lambda c: c.driver.select_query.statement(
            [c.columns.primary_key, c.columns.schedule],
        ),
        "delete": lambda c: c.driver.delete_query.statement(c.columns.primary_key),
    }


def _shared_cases() -> Cases:
    return {
        "exists": lambda c: exists_statement(c.table_name, c.columns.primary_key),
        "migration_lock": lambda c: c.driver.migration_lock_query.statement(),
        "relations_exist": lambda c: c.driver.relations_exist_query.statement(
            c.driver.index_columns,
        ),
        "relations_exist_added": lambda c: c.driver.relations_exist_query.statement(
            c.driver.index_columns,
            added_columns=c.driver.added_columns,
            added_indexes=c.driver.added_indexes,
        ),
        "catalog_read": lambda c: catalog_read([_catalog_table(c)]),
        "database_read": lambda c: database_read([_catalog_table(c)]),
    }


def _catalog_table(component: Component) -> tuple[Identifier, list[str]]:
    schema = table_schema(component)
    return (
        Identifier.parse(schema.name, what="table name", warn=False),
        [index.name for index in schema.indexes],
    )


def _cases_of(variant: Variant) -> Cases:
    kind = variant.label.split("-")[0]
    specific = {
        "broker": _broker_cases,
        "result_backend": _result_backend_cases,
        "scheduler_source": _scheduler_source_cases,
    }[kind]()
    return {**specific, **_shared_cases()}


STATEMENT_CASES = [
    pytest.param(variant, case, id=f"{variant.label}-{case}")
    for variant in VARIANTS
    for case in _cases_of(variant)
]


@pytest.fixture
async def connection(postgresql_dsn: str) -> AsyncIterator[asyncpg.Connection]:
    """One raw connection to the test database."""
    conn = await asyncpg.connect(postgresql_dsn)
    try:
        yield conn
    finally:
        await conn.close()


async def migrate(conn: asyncpg.Connection, component: Component) -> None:
    """Create the table of ``component`` with the package's own statements."""
    for statement in migration_statements([table_schema(component)]):
        await conn.execute(statement)


async def drop(conn: asyncpg.Connection, table: str) -> None:
    """Drop ``table`` if it exists."""
    await conn.execute(drop_table_script(table).text)


@pytest.fixture
async def migrated(
    connection: asyncpg.Connection,
    request: pytest.FixtureRequest,
) -> AsyncIterator[Component]:
    """The component of the test's variant, with its table migrated."""
    variant: Variant = request.node.callspec.params["variant"]
    component = variant.build(variant.table)
    await drop(connection, variant.table)
    await migrate(connection, component)
    try:
        yield component
    finally:
        await drop(connection, variant.table)


def _names(types: Optional[Sequence[str]], *, void_is_empty: bool) -> list[str]:
    if not types:
        return []
    if void_is_empty and list(types) == ["void"]:
        return []
    return list(types)


def _check_types(
    what: str,
    declared: Sequence[ParamType],
    server: Sequence[str],
) -> None:
    assert len(server) == len(declared), (
        f"{what}: declared {[t.name for t in declared]}, server {list(server)}"
    )
    for position, (want, got) in enumerate(zip(declared, server, strict=True), start=1):
        assert got in REGTYPES[want], (
            f"{what} {position}: declared {want.name}, server {got}"
        )


@pytest.mark.parametrize(("variant", "case"), STATEMENT_CASES)
async def test_the_server_infers_the_declared_types(
    connection: asyncpg.Connection,
    migrated: Component,
    variant: Variant,
    case: str,
) -> None:
    """PREPARE gives each statement the parameter and column types it declares."""
    statement = _cases_of(variant)[case](migrated)
    await connection.execute(f"PREPARE sqlcat_probe AS {statement.text}")
    try:
        row = await connection.fetchrow(
            "SELECT parameter_types::text[] AS params,"
            " result_types::text[] AS results"
            " FROM pg_prepared_statements WHERE name = 'sqlcat_probe'",
        )
    finally:
        await connection.execute("DEALLOCATE sqlcat_probe")
    assert row is not None
    _check_types(
        "parameter", statement.params, _names(row["params"], void_is_empty=False)
    )
    _check_types(
        "column",
        [kind for _, kind in statement.columns],
        _names(row["results"], void_is_empty=True),
    )


async def test_every_param_type_has_a_server_type() -> None:
    """The map above leaves no ParamType out, so a new member needs a line."""
    assert set(REGTYPES) == set(ParamType)


async def test_the_server_refuses_exactly_the_reserved_words(
    connection: asyncpg.Connection,
) -> None:
    """Each word of pg_get_keywords() as a table name: the refused ones are ours."""
    words = [
        row["word"]
        for row in await connection.fetch("SELECT word FROM pg_get_keywords()")
    ]
    refused: set[str] = set()
    for word in words:
        # The words come from the server and are plain identifiers, and the
        # statement cannot take a name as a bind parameter.
        assert re.fullmatch(r"[a-z_]+", word), word
        try:
            async with connection.transaction():
                await connection.execute(f"CREATE TEMP TABLE {word} ()")
                # Rolled back with the savepoint: the next word starts clean.
                raise _RollbackError
        except _RollbackError:
            continue
        except asyncpg.PostgresSyntaxError:
            refused.add(word)
    assert refused == RESERVED_WORDS


class _RollbackError(Exception):
    """Ends a savepoint block without keeping what it did."""


def _declared_indexes(component: Component) -> list[str]:
    driver = component.driver
    return [
        *(
            index_name(driver.table_name, column)
            for column in driver.index_columns or ()
        ),
        *(index.name for index in driver.added_indexes),
    ]


def _serial_column(component: Component) -> Optional[str]:
    return next(
        (
            column.name
            for column in component.driver.create_table_query.columns
            if column.type.upper() == "SERIAL"
        ),
        None,
    )


@dataclass(frozen=True)
class OnServer:
    """What ``pg_class``, ``pg_index`` and ``pg_constraint`` hold for a table."""

    table: str
    indexes: frozenset[str]
    primary_key: Optional[str]
    primary_key_index: Optional[str]
    sequences: frozenset[str]


async def _on_server(conn: asyncpg.Connection, table: str) -> OnServer:
    rows = await conn.fetch(
        "SELECT ic.relname::text AS name, i.indisprimary AS is_primary"
        " FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid"
        " WHERE i.indrelid = to_regclass($1)",
        table,
    )
    constraint = await conn.fetchval(
        "SELECT conname::text FROM pg_constraint"
        " WHERE conrelid = to_regclass($1) AND contype = 'p'",
        table,
    )
    sequences = await conn.fetch(
        "SELECT s.relname::text AS name FROM pg_class s"
        " JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass"
        " WHERE s.relkind = 'S' AND d.refobjid = to_regclass($1)",
        table,
    )
    return OnServer(
        table=await conn.fetchval(
            "SELECT relname::text FROM pg_class WHERE oid = to_regclass($1)", table
        ),
        indexes=frozenset(row["name"] for row in rows if not row["is_primary"]),
        primary_key=constraint,
        primary_key_index=next(
            (row["name"] for row in rows if row["is_primary"]), None
        ),
        sequences=frozenset(row["name"] for row in sequences),
    )


def _of_kind(names: Sequence[tuple[str, str]], kind: str) -> set[str]:
    return {name for what, name in names if what == kind}


@dataclass(frozen=True)
class Family:
    """A component, the names its table may have, and the longest one accepted."""

    kind: str
    build: Callable[[str], Component]
    longest: int
    refused: tuple[int, ...]


FAMILIES = [
    Family("broker", broker_of, 61, (62, 63)),
    Family("result_backend", result_backend_of, 62, (63,)),
    Family("scheduler_source", scheduler_source_of, 61, (62, 63)),
]


def _table_of_length(family: Family, size: int) -> str:
    head = f"{PREFIX}{family.kind[0]}_"
    return head + "n" * (size - len(head))


ACCEPTED_LENGTHS = [
    pytest.param(family, size, id=f"{family.kind}-{size}")
    for family in FAMILIES
    for size in range(48, family.longest + 1)
]
REFUSED_LENGTHS = [
    pytest.param(family, size, id=f"{family.kind}-{size}")
    for family in FAMILIES
    for size in family.refused
]


@pytest.mark.parametrize(("family", "size"), ACCEPTED_LENGTHS)
async def test_the_derived_names_are_the_ones_the_server_gives(
    connection: asyncpg.Connection,
    family: Family,
    size: int,
) -> None:
    """Migrating a table of any accepted length ends with the names we compute."""
    table = _table_of_length(family, size)
    assert len(table) == size
    component = family.build(table)
    schema = table_schema(component)
    await drop(connection, table)
    try:
        await migrate(connection, component)
        server = await _on_server(connection, table)
    finally:
        await drop(connection, table)
    names = derived_names(
        Identifier.parse(table, what="table name", warn=False),
        index_names=[index.name for index in schema.indexes],
        serial_column=_serial_column(component),
    )
    assert server.table == table
    assert server.indexes == _of_kind(names, "index")
    assert {server.primary_key, server.primary_key_index} == _of_kind(
        names, "primary key"
    )
    assert server.sequences == _of_kind(names, "sequence")


@pytest.mark.parametrize(("family", "size"), REFUSED_LENGTHS)
async def test_a_refused_length_collides_as_predicted(
    connection: asyncpg.Connection,
    family: Family,
    size: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the check the server loses the index the check predicts."""
    table = _table_of_length(family, size)
    # The component refuses this name at construction; the check is switched
    # off for the build only, to run the DDL it would otherwise stop.
    with monkeypatch.context() as unchecked:
        unchecked.setattr(
            "taskiq_postgresql.abc.driver.check_table_names",
            lambda *args, **kwargs: None,
        )
        component = family.build(table)
    driver = component.driver
    declared = _declared_indexes(component)
    identifier = Identifier.parse(table, what="table name", warn=False)
    serial = _serial_column(component)
    with pytest.raises(IdentifierError, match="too long"):
        check_table_names(identifier, index_names=declared, serial_column=serial)
    names = derived_names(identifier, index_names=declared, serial_column=serial)
    await drop(connection, table)
    try:
        # What on_startup runs after the lock, without the check that refuses.
        await connection.execute(driver.create_table_query.make_query())
        if driver.index_columns is not None:
            await connection.execute(
                driver.create_index_query.make_query(driver.index_columns),
            )
        if driver.added_columns or driver.added_indexes:
            await connection.execute(
                driver.add_columns_query.make_query(
                    driver.added_columns,
                    driver.added_indexes,
                ),
            )
        server = await _on_server(connection, table)
    finally:
        await drop(connection, table)
    # CREATE TABLE makes the table, its primary key and its sequence; each
    # CREATE INDEX IF NOT EXISTS then skips a name that is taken, which is how
    # a collision loses an index (all of them, when the cut name is the table's).
    taken = {identifier.name, *_of_kind(names, "primary key")}
    taken |= _of_kind(names, "sequence")
    kept: set[str] = set()
    for name in (name for what, name in names if what == "index"):
        if name not in taken:
            kept.add(name)
            taken.add(name)
    assert len(kept) < len(declared)
    assert server.indexes == kept
