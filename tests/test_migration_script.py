"""
The table description in ``taskiq_postgresql.migrations`` and its SQL script.

The parity tests run the generated script on an empty database, in one call,
through each driver's own library, and compare the catalog it builds with the
one the components build with ``run_migrations=True``. They keep the script
and the runtime migration from drifting apart.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
import re
import secrets
import string
import sys
import warnings
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, AsyncGenerator, Optional
from urllib.parse import urlsplit

import asyncpg
import pytest
from taskiq import InMemoryBroker, TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

from taskiq_postgresql import migrations
from taskiq_postgresql.abc.query import MIGRATION_LOCK_NAMESPACE, Column
from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.core.sql.catalog.base import _sql_literal
from taskiq_postgresql.migrations import (
    DEFAULT_LOCK_TIMEOUT,
    ColumnSchema,
    FormatOptions,
    IndexSchema,
    MigrationStep,
    StepObjects,
    TableSchema,
    collect_components,
    collect_schemas,
    migration_statements,
    render_sql,
    table_schema,
)
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import (
    BROKER_TABLE_0_4_SQL,
    LOCK_TIMEOUT,
    UNUSED_DSN,
    WAIT_SECONDS,
    address_of,
    build_component,
    close_component,
    create_database,
    drop_database,
    in_database,
    run_script,
)
from tests.test_migrations import scram_verifier, wait_until_blocked_by

KINDS = ("broker", "result_backend", "scheduler_source")
TASK_ID_FIELDS = ("VarChar", "Text", "Uuid")
# Every table shape the package creates; the schedule table has no task_id.
SHAPES = [
    *(("broker", field) for field in TASK_ID_FIELDS),
    *(("result_backend", field) for field in TASK_ID_FIELDS),
    ("scheduler_source", None),
]
SHAPE_IDS = [f"{kind}-{field}" if field else kind for kind, field in SHAPES]


def lock_lines(script: str) -> list[str]:
    return [line for line in script.splitlines() if "pg_advisory_xact_lock" in line]


@pytest.fixture
async def empty_dsn(postgresql_dsn: str) -> AsyncGenerator[str, None]:
    """DSN of a database created empty for one test and dropped after it."""
    name = "migration_" + "".join(random.choices(string.ascii_lowercase, k=10))
    await create_database(postgresql_dsn, name)
    yield in_database(postgresql_dsn, name)
    await drop_database(postgresql_dsn, name)


@pytest.fixture
async def inspector(empty_dsn: str) -> AsyncGenerator[asyncpg.Connection, None]:
    """A superuser connection to the empty database, for setup and checks."""
    connection = await asyncpg.connect(empty_dsn)
    yield connection
    await connection.close()


COLUMNS_QUERY = (
    "SELECT column_name, data_type, udt_name, character_maximum_length,"
    " is_nullable, column_default FROM information_schema.columns"
    " WHERE table_schema = current_schema() AND table_name = $1"
    " ORDER BY ordinal_position"
)
INDEXES_QUERY = (
    "SELECT indexname, indexdef FROM pg_indexes"
    " WHERE schemaname = current_schema() AND tablename = $1"
)


async def catalog(
    connection: asyncpg.Connection,
    table: str,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    """
    Return the columns and indexes of ``table`` as the catalog holds them.

    The schema and table names are replaced by placeholders, so two tables of
    the same shape compare equal and anything else that differs still shows.
    """
    schema = await connection.fetchval("SELECT current_schema()")

    def normalized(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return value.replace(f"{schema}.", "<schema>.").replace(table, "<table>")

    columns = [
        tuple(map(normalized, row))
        for row in await connection.fetch(COLUMNS_QUERY, table)
    ]
    indexes = sorted(
        tuple(map(normalized, row))
        for row in await connection.fetch(INDEXES_QUERY, table)
    )
    return columns, indexes


# --- the description ---------------------------------------------------------


@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
def test_the_steps_are_the_sql_run_migrations_executes(
    kind: str,
    field: Optional[str],
    driver: str,
) -> None:
    component = build_component(kind, table_name="jobs", field=field, driver=driver)
    runtime = component.driver

    schema = table_schema(component)

    step, *later_steps = schema.steps
    create_table, *create_indexes = step.statements
    assert step.number == 1
    assert create_table == runtime.create_table_query.make_query()
    assert "\n".join(f"{statement};" for statement in create_indexes) == (
        runtime.create_index_query.make_query(runtime.index_columns)
    )
    # Only the broker's table got columns after its first release.
    if kind == "broker":
        (upgrade,) = later_steps
        assert upgrade.number == 2  # noqa: PLR2004
        assert "\n".join(f"{statement};" for statement in upgrade.statements) == (
            runtime.add_columns_query.make_query(
                runtime.added_columns,
                runtime.added_indexes,
            )
        )
    else:
        assert later_steps == []
        assert (runtime.added_columns, runtime.added_indexes) == ((), ())
    # A driver no component owns cannot compare, and probes instead: exactly
    # the relations the steps create.
    assert [schema.name, *(index.name for index in schema.indexes)] == (
        runtime.relations_exist_query.relation_names(
            runtime.index_columns,
            added_indexes=runtime.added_indexes,
        )
    )
    assert [column.name for column in schema.columns] == [
        column.name
        for column in (*runtime.create_table_query.columns, *runtime.added_columns)
    ]


@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
def test_a_kind_describes_the_same_table_as_a_component(
    kind: str,
    field: Optional[str],
    driver: str,
) -> None:
    component = build_component(kind, table_name="jobs", field=field, driver=driver)

    assert table_schema(kind, table_name="jobs", field_for_task_id=field) == (
        table_schema(component)
    )


@pytest.mark.parametrize("kind", KINDS)
def test_a_kind_alone_describes_the_default_component(kind: str, driver: str) -> None:
    assert table_schema(kind) == table_schema(build_component(kind, driver=driver))


def test_columns_and_indexes_are_described_without_sql_text() -> None:
    broker = table_schema("broker", field_for_task_id="VarChar")

    assert broker.kind == "broker"
    assert broker.name == "taskiq_messages"
    assert broker.columns == (
        ColumnSchema(
            name="id",
            sql_type="SERIAL",
            type_name="serial",
            length=None,
            nullable=False,
            default=None,
            primary_key=True,
        ),
        ColumnSchema("task_id", "VarChar", "varchar", None, False, None, False),
        ColumnSchema("task_name", "VARCHAR", "varchar", None, False, None, False),
        ColumnSchema("message", "BYTEA", "bytea", None, False, None, False),
        ColumnSchema("labels", "JSONB", "jsonb", None, False, None, False),
        ColumnSchema(
            "created_at",
            "TIMESTAMP WITH TIME ZONE",
            "timestamptz",
            None,
            False,
            "NOW()",
            False,
        ),
        # Added by step 2, so they come last in the table.
        ColumnSchema("channel", "VARCHAR", "varchar", None, True, None, False),
        ColumnSchema(
            "not_before",
            "TIMESTAMP WITH TIME ZONE",
            "timestamptz",
            None,
            False,
            "NOW()",
            False,
        ),
    )
    assert broker.primary_key == broker.columns[0]
    assert broker.indexes == (
        IndexSchema("taskiq_messages_id_idx", ("id",), "hash"),
        IndexSchema(
            "taskiq_messages_not_before_idx",
            ("channel", "not_before", "id"),
            "btree",
        ),
    )
    assert [step.number for step in broker.steps] == [1, 2]

    scheduler = table_schema("scheduler_source")
    task_name = next(c for c in scheduler.columns if c.name == "task_name")
    assert (task_name.sql_type, task_name.type_name, task_name.length) == (
        "VARCHAR(100)",
        "varchar",
        100,
    )
    assert [(index.columns, index.method) for index in scheduler.indexes] == [
        (("id",), "hash"),
        (("task_name",), "hash"),
    ]
    assert table_schema("result_backend").primary_key.name == "task_id"


def test_the_types_a_later_step_needs_are_described() -> None:
    # A later step can add a nullable column and a timestamptz written short.
    assert migrations._column_schema(
        Column("not_before", "TIMESTAMPTZ", default="NOW()"),
    ) == ColumnSchema(
        "not_before",
        "TIMESTAMPTZ",
        "timestamptz",
        None,
        False,
        "NOW()",
        False,
    )
    assert migrations._column_schema(
        Column("channel", "VARCHAR", nullable=True),
    ).nullable


def test_a_table_without_a_primary_key_says_so() -> None:
    keyless = dataclasses.replace(table_schema("broker"), columns=())

    with pytest.raises(LookupError, match="'taskiq_messages' has no primary key"):
        _ = keyless.primary_key


def test_a_column_type_without_a_description_is_refused() -> None:
    with pytest.raises(ValueError, match="'MONEY'"):
        migrations._column_schema(Column("price", "MONEY"))


def test_a_step_holds_any_statement_and_an_index_any_method() -> None:
    # The shapes a later step needs: ALTER TABLE, and a btree on several columns.
    index = IndexSchema("jobs_not_before_idx", ("channel", "not_before"), "btree")
    step = MigrationStep(
        number=2,
        statements=("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS channel VARCHAR",),
        objects=StepObjects(
            creates_table=False,
            columns=("channel",),
            indexes=(index,),
        ),
    )

    assert step.statements[0].startswith("ALTER TABLE")
    assert index.columns == ("channel", "not_before")
    assert step.objects.indexes == (index,)


def test_kind_arguments_are_checked() -> None:
    with pytest.raises(ValueError, match="unknown component kind 'worker'"):
        table_schema("worker")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="no task_id column"):
        table_schema("scheduler_source", field_for_task_id="Text")
    with pytest.raises(ValueError, match="field_for_task_id"):
        table_schema("broker", field_for_task_id="BIGINT")  # type: ignore[arg-type]


def test_a_component_keeps_its_own_table_name(driver: str) -> None:
    component = build_component("broker", driver=driver)

    with pytest.raises(TypeError, match="already has its own"):
        table_schema(component, table_name="jobs")


def test_an_object_that_is_not_a_component_is_refused() -> None:
    with pytest.raises(TypeError, match="got object"):
        table_schema(object())  # type: ignore[arg-type]


# --- collecting components ---------------------------------------------------


def test_a_scheduler_brings_its_sources_and_its_broker(driver: str) -> None:
    backend = PostgresqlResultBackend(dsn=UNUSED_DSN, driver=driver, table_name="r")
    broker = PostgresqlBroker(
        dsn=UNUSED_DSN,
        driver=driver,
        table_name="m",
    ).with_result_backend(backend)
    source = PostgresqlSchedulerSource(dsn=UNUSED_DSN, driver=driver, table_name="s")
    scheduler = TaskiqScheduler(
        broker=broker,
        sources=[LabelScheduleSource(broker), source],
    )

    # The broker named twice yields its tables once.
    assert [schema.name for schema in collect_schemas(broker, scheduler)] == [
        "m",
        "r",
        "s",
    ]
    assert collect_components(scheduler) == [broker, backend, source]


def test_a_broker_of_another_package_brings_our_result_backend(driver: str) -> None:
    backend = PostgresqlResultBackend(dsn=UNUSED_DSN, driver=driver)

    assert collect_components(InMemoryBroker().with_result_backend(backend)) == [
        backend,
    ]


def test_a_target_without_a_component_is_refused() -> None:
    with pytest.raises(LookupError, match="holds no taskiq-postgresql component"):
        collect_schemas(InMemoryBroker())


def test_two_components_with_one_table_and_different_columns_are_refused(
    driver: str,
) -> None:
    text_ids = PostgresqlResultBackend(
        dsn=UNUSED_DSN,
        driver=driver,
        table_name="t",
        field_for_task_id="Text",
    )
    uuid_ids = PostgresqlResultBackend(dsn=UNUSED_DSN, driver=driver, table_name="t")

    with pytest.raises(ValueError, match="'t'"):
        collect_schemas(text_ids, uuid_ids)


def test_descriptions_are_kept_and_each_table_comes_once(driver: str) -> None:
    jobs = table_schema("broker", table_name="jobs")
    schedules = table_schema("scheduler_source")
    broker = PostgresqlBroker(dsn=UNUSED_DSN, driver=driver, table_name="jobs")

    # A description of a component's table, with the same columns, adds
    # nothing; with other columns, it is refused like a second component.
    assert collect_schemas(jobs, schedules, broker, jobs) == [jobs, schedules]
    with pytest.raises(ValueError, match="'jobs'"):
        collect_schemas(
            broker,
            table_schema("broker", table_name="jobs", field_for_task_id="Text"),
        )


def test_an_import_path_is_loaded_the_way_taskiq_worker_loads_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    driver: str,
) -> None:
    module = "tkq_" + "".join(random.choices(string.ascii_lowercase, k=10))
    (tmp_path / f"{module}.py").write_text(
        "from taskiq_postgresql import PostgresqlBroker\n"
        "\n"
        "\n"
        "def make_broker():\n"
        f"    return PostgresqlBroker(dsn={UNUSED_DSN!r}, driver={driver!r})\n",
        encoding="utf-8",
    )
    # collect_schemas adds app_dir to sys.path, as taskiq worker does.
    monkeypatch.setattr(sys, "path", list(sys.path))
    try:
        schemas = collect_schemas(f"{module}:make_broker", app_dir=str(tmp_path))
    finally:
        sys.modules.pop(module, None)

    assert schemas == [table_schema("broker")]
    with pytest.raises(ValueError, match="'module:object'"):
        collect_schemas(module)


# --- the script --------------------------------------------------------------


def test_the_script_has_a_header_and_ends_every_statement() -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    script = render_sql(schemas)
    lines = script.splitlines()

    assert lines[:2] == [
        f"-- taskiq-postgresql {version('taskiq-postgresql')}",
        "-- tables: taskiq_messages, taskiq_results",
    ]
    assert lines[2] == "BEGIN;"
    assert lines[-1] == "COMMIT;"
    # A statement may span lines (step 2's ALTER TABLE does), so the script is
    # checked whole: each statement on lines of its own, ending with ";".
    assert script == "\n".join(
        [
            *lines[:2],
            "BEGIN;",
            "SET LOCAL lock_timeout = '10s';",
            *lock_lines(script),
            *(
                f"{statement};"
                for schema in schemas
                for step in schema.steps
                for statement in step.statements
            ),
            "COMMIT;",
            "",
        ],
    )
    assert len(lock_lines(script)) == len(schemas)
    assert render_sql(schemas) == render_sql(
        [table_schema("broker"), table_schema("result_backend")],
    )


def test_without_a_transaction_there_is_no_begin_or_commit() -> None:
    script = render_sql([table_schema("broker")], transaction=False)

    assert not re.search(r"\b(BEGIN|COMMIT)\b", script)
    # The lock stays: the caller's transaction holds it until it commits.
    assert len(lock_lines(script)) == 1


def test_the_lock_line_is_the_runtime_lock_key() -> None:
    script = render_sql([table_schema("scheduler_source", table_name="jobs")])

    # The namespace is a contract between releases: never change it.
    assert MIGRATION_LOCK_NAMESPACE == "taskiq_postgresql"
    # After BEGIN and the lock timeout.
    assert script.splitlines()[4] == (
        "SELECT pg_advisory_xact_lock(hashtext('taskiq_postgresql'), hashtext('jobs'));"
    )


def test_the_lock_timeout_is_the_first_statement() -> None:
    schemas = [table_schema("broker")]

    assert DEFAULT_LOCK_TIMEOUT == 10  # noqa: PLR2004
    assert render_sql(schemas).splitlines()[2:4] == [
        "BEGIN;",
        "SET LOCAL lock_timeout = '10s';",
    ]
    assert render_sql(schemas, lock_timeout=3).splitlines()[3] == (
        "SET LOCAL lock_timeout = '3s';"
    )
    # Without BEGIN, the tool's own transaction is the one SET LOCAL lasts for.
    assert render_sql(schemas, transaction=False).splitlines()[2] == (
        "SET LOCAL lock_timeout = '10s';"
    )
    # Without the advisory lock, the table locks still wait at most this long.
    assert render_sql(schemas, lock=False).splitlines()[3] == (
        "SET LOCAL lock_timeout = '10s';"
    )
    for transaction in (True, False):
        assert "lock_timeout" not in render_sql(
            schemas,
            transaction=transaction,
            lock_timeout=0,
        )


@pytest.mark.parametrize(
    "lock_timeout",
    [-1, 2_147_484, True, 1.5, "10"],
    ids=["negative", "above-the-server-maximum", "bool", "fraction", "text"],
)
def test_a_lock_timeout_the_script_cannot_set_is_refused(lock_timeout: Any) -> None:
    message = "lock_timeout must be a whole number of seconds from 0 to 2147483"

    with pytest.raises(ValueError, match=message):
        render_sql([table_schema("broker")], lock_timeout=lock_timeout)
    with pytest.raises(ValueError, match=message):
        FormatOptions(lock_timeout=lock_timeout)


def test_without_the_lock_there_is_no_lock_line() -> None:
    script = render_sql([table_schema("broker")], lock=False)

    assert "pg_advisory" not in script
    assert "CREATE TABLE IF NOT EXISTS taskiq_messages" in script


def test_locks_are_taken_in_table_name_order() -> None:
    script = render_sql(
        [
            table_schema("scheduler_source", table_name="b"),
            table_schema("broker", table_name="a"),
        ],
    )

    assert re.findall(r"hashtext\('(\w+)'\)\);", script) == ["a", "b"]
    assert re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", script) == ["b", "a"]


@pytest.mark.parametrize(
    "name",
    ["o'brien", "back\\slash", "both'\\"],
    ids=["quote", "backslash", "both"],
)
def test_a_quote_or_backslash_in_the_table_name_is_refused(name: str) -> None:
    # The Identifier rule (ADR 0008) refuses these before any SQL is written;
    # the escaping itself is pinned by tests/unit/test_catalog.py.
    with pytest.raises(ValueError, match="is not a plain SQL identifier"):
        table_schema("result_backend", table_name=name)


@pytest.mark.parametrize(
    "name",
    ["", "jobs\nDROP TABLE users", "jobs\r", "jobs\0"],
    ids=["empty", "newline", "carriage-return", "nul"],
)
def test_a_table_name_the_script_cannot_hold_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match="table name"):
        table_schema("broker", table_name=name)
    with pytest.raises(ValueError, match="table name"):
        render_sql([dataclasses.replace(table_schema("broker"), name=name)])


def test_rendering_nothing_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one"):
        render_sql([])
    with pytest.raises(ValueError, match="at least one"):
        migration_statements([])


@pytest.mark.parametrize("lock", [True, False], ids=["lock", "no-lock"])
def test_the_statements_are_the_script_one_by_one(lock: bool) -> None:
    schemas = [
        table_schema("scheduler_source", table_name="b"),
        table_schema("broker", table_name="a"),
    ]

    statements = migration_statements(schemas, lock=lock)

    script = render_sql(schemas, transaction=False, lock=lock, lock_timeout=0)
    # A statement may span lines (step 2's ALTER TABLE), so compare the text
    # after the two header lines, not line by line.
    assert script.split("\n", 2)[2] == "".join(
        f"{statement};\n" for statement in statements
    )
    assert all(";" not in statement for statement in statements)


def test_the_header_says_unknown_without_package_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(name: str) -> str:
        raise PackageNotFoundError(name)

    monkeypatch.setattr(migrations, "version", missing)

    assert render_sql([table_schema("broker")]).startswith(
        "-- taskiq-postgresql unknown\n",
    )


# --- against PostgreSQL, once per driver -------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
async def test_the_script_builds_the_catalog_run_migrations_builds(
    kind: str,
    field: Optional[str],
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    scripted, started = f"{postgres_table}_script", f"{postgres_table}_startup"
    schema = table_schema(kind, table_name=scripted, field_for_task_id=field)

    await run_script(empty_dsn, driver, render_sql([schema]))
    component = build_component(
        kind,
        dsn=empty_dsn,
        table_name=started,
        field=field,
        driver=driver,
    )
    try:
        await component.startup()
    finally:
        await close_component(component)

    columns, indexes = await catalog(inspector, scripted)
    assert (columns, indexes) == await catalog(inspector, started)
    # And the description says what the catalog holds.
    assert [row[0] for row in columns] == [column.name for column in schema.columns]
    definitions = dict(indexes)
    for index in schema.indexes:
        assert definitions[index.name.replace(scripted, "<table>")].endswith(
            f" USING {index.method} ({', '.join(index.columns)})",
        )
    # Every index but the primary key's own, which PostgreSQL names.
    assert set(definitions) == {
        "<table>_pkey",
        *(index.name.replace(scripted, "<table>") for index in schema.indexes),
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "transaction",
    [True, False],
    ids=["transaction", "no-transaction"],
)
async def test_the_script_runs_twice(
    transaction: bool,
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
) -> None:
    schemas = [table_schema(kind) for kind in KINDS]
    script = render_sql(schemas, transaction=transaction)

    await run_script(empty_dsn, driver, script)
    await run_script(empty_dsn, driver, script)

    assert await inspector.fetchval(
        "SELECT count(*) FROM pg_class WHERE relkind = 'r' "
        "AND relname = ANY($1::text[])",
        [schema.name for schema in schemas],
    ) == len(KINDS)


@pytest.mark.anyio
async def test_the_script_brings_a_0_4_broker_table_up_to_date(
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    upgraded, started = f"{postgres_table}_script", f"{postgres_table}_startup"
    await inspector.execute(BROKER_TABLE_0_4_SQL.format(table=upgraded))
    await inspector.execute(
        f"INSERT INTO {upgraded} (task_id, task_name, message, labels) "  # noqa: S608
        "VALUES (gen_random_uuid(), 'old_task', 'old', '{}')",
    )
    script = render_sql([table_schema("broker", table_name=upgraded)])
    rows_query = (
        "SELECT message, channel IS NULL AS no_channel,"  # noqa: S608
        " not_before >= created_at AND not_before <= NOW() AS due,"
        f" not_before FROM {upgraded}"
    )

    await run_script(empty_dsn, driver, script)
    first = [tuple(row) for row in await inspector.fetch(rows_query)]
    await run_script(empty_dsn, driver, script)
    second = [tuple(row) for row in await inspector.fetch(rows_query)]
    component = build_component(
        "broker",
        dsn=empty_dsn,
        table_name=started,
        driver=driver,
    )
    try:
        await component.startup()
    finally:
        await close_component(component)

    # The old row is kept, without a channel, due since the upgrade.
    assert [row[:3] for row in first] == [(b"old", True, True)]
    # The second run changed nothing, not even the default it filled in.
    assert second == first
    assert await catalog(inspector, upgraded) == await catalog(inspector, started)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_after_the_script_a_dml_only_role_starts_with_run_migrations(
    kind: str,
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    await run_script(
        empty_dsn,
        driver,
        render_sql([table_schema(kind, table_name=postgres_table)]),
    )
    role = f"{postgres_table}_dml"
    password = secrets.token_hex(16)
    await inspector.execute(
        f"CREATE ROLE {role} LOGIN PASSWORD '{scram_verifier(password)}'",
    )
    try:
        await inspector.execute(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON {postgres_table} TO {role}",
        )
        if kind == "broker":
            await inspector.execute(
                f"GRANT USAGE ON SEQUENCE {postgres_table}_id_seq TO {role}",
            )
        # No CREATE on the schema and not the owner: any DDL would fail.
        assert not await inspector.fetchval(
            "SELECT has_schema_privilege($1, current_schema(), 'CREATE')",
            role,
        )
        assert await inspector.fetchval(
            "SELECT pg_get_userbyid(relowner) <> $1 FROM pg_class "
            "WHERE oid = to_regclass($2)",
            role,
            postgres_table,
        )
        parts = urlsplit(empty_dsn)
        limited = build_component(
            kind,
            dsn=parts._replace(
                netloc=f"{role}:{password}@{address_of(parts)}",
            ).geturl(),
            table_name=postgres_table,
            driver=driver,
        )
        try:
            await limited.startup()
        finally:
            await close_component(limited)
    finally:
        await inspector.execute(f"DROP OWNED BY {role}")
        await inspector.execute(f"DROP ROLE {role}")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_a_startup_waits_for_the_lock_the_script_holds(
    kind: str,
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    (lock_line,) = lock_lines(
        render_sql([table_schema(kind, table_name=postgres_table)]),
    )
    component = build_component(
        kind,
        dsn=empty_dsn,
        table_name=postgres_table,
        driver=driver,
    )
    holder = await asyncpg.connect(empty_dsn)
    try:
        transaction = holder.transaction()
        await transaction.start()
        await holder.execute(lock_line)
        startup = asyncio.create_task(component.startup())
        try:
            locktype = await wait_until_blocked_by(
                inspector,
                holder.get_server_pid(),
                startup,
            )
        finally:
            await transaction.commit()
        await asyncio.wait_for(startup, WAIT_SECONDS)

        assert locktype == "advisory"
    finally:
        await holder.close()
        await close_component(component)


def script_in_a_transaction(schema: TableSchema, *, transaction: bool) -> str:
    """
    The script with ``LOCK_TIMEOUT``, in its own transaction or in a tool's.

    Without its own ``BEGIN``, it runs inside the ``BEGIN`` and ``COMMIT`` that
    Alembic, Django and Flyway wrap around a migration.
    """
    script = render_sql(
        [schema],
        transaction=transaction,
        lock_timeout=LOCK_TIMEOUT,
    )
    return script if transaction else f"BEGIN;\n{script}COMMIT;\n"


async def lock_waited_on(
    admin: asyncpg.Connection,
    blocker_pid: int,
    script: asyncio.Task[None],
) -> tuple[str, str]:
    """
    Wait until a session waits on a lock ``blocker_pid`` holds.

    Returns:
        The type and mode of the lock it waits for.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_SECONDS
    while not script.done() and loop.time() < deadline:
        waiting = await admin.fetchrow(
            "SELECT locktype, mode FROM pg_locks "
            "WHERE NOT granted AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
            blocker_pid,
        )
        if waiting is not None:
            return waiting["locktype"], waiting["mode"]
        await asyncio.sleep(0.05)
    pytest.fail("the script never waited on the lock")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "transaction",
    [True, False],
    ids=["transaction", "tool-transaction"],
)
async def test_the_lock_timeout_bounds_the_wait_for_the_advisory_lock(
    transaction: bool,
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    schema = table_schema("broker", table_name=postgres_table)
    (lock_line,) = lock_lines(render_sql([schema]))
    loop = asyncio.get_running_loop()
    # Another script, or a worker's run_migrations=True, holds the lock.
    holder = await asyncpg.connect(empty_dsn)
    try:
        holding = holder.transaction()
        await holding.start()
        await holder.execute(lock_line)
        started = loop.time()
        script = asyncio.create_task(
            run_script(
                empty_dsn,
                driver,
                script_in_a_transaction(schema, transaction=transaction),
            ),
        )
        try:
            waited_on = await lock_waited_on(
                inspector,
                holder.get_server_pid(),
                script,
            )
            with pytest.raises(Exception, match="lock timeout"):
                await asyncio.wait_for(script, WAIT_SECONDS)
            waited = loop.time() - started
        finally:
            await holding.rollback()
            await asyncio.gather(script, return_exceptions=True)
    finally:
        await holder.close()

    assert waited_on == ("advisory", "ExclusiveLock")
    assert LOCK_TIMEOUT <= waited < WAIT_SECONDS
    # It failed as a whole: the table it would have created is not there.
    assert await inspector.fetchval("SELECT to_regclass($1)", postgres_table) is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "transaction",
    [True, False],
    ids=["transaction", "tool-transaction"],
)
async def test_the_lock_timeout_bounds_the_wait_of_alter_table_on_a_live_table(
    transaction: bool,
    driver: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    schema = table_schema("broker", table_name=postgres_table)
    await run_script(empty_dsn, driver, render_sql([schema]))
    insert = (
        f"INSERT INTO {postgres_table} (task_id, task_name, message, labels) "  # noqa: S608
        "VALUES (gen_random_uuid(), 'task', 'message', '{}')"
    )
    loop = asyncio.get_running_loop()
    # A worker's transaction that read the table and has not ended yet.
    reader = await asyncpg.connect(empty_dsn)
    writer = await asyncpg.connect(empty_dsn)
    produced: Optional[asyncio.Task[str]] = None
    try:
        reading = reader.transaction()
        await reading.start()
        await reader.fetchval(f"SELECT count(*) FROM {postgres_table}")  # noqa: S608
        started = loop.time()
        script = asyncio.create_task(
            run_script(
                empty_dsn,
                driver,
                script_in_a_transaction(schema, transaction=transaction),
            ),
        )
        try:
            waited_on = await lock_waited_on(
                inspector,
                reader.get_server_pid(),
                script,
            )
            # A producer's INSERT, queued behind the waiting ALTER TABLE.
            produced = asyncio.create_task(writer.execute(insert))
            with pytest.raises(Exception, match="lock timeout"):
                await asyncio.wait_for(script, WAIT_SECONDS)
            waited = loop.time() - started
            await asyncio.wait_for(produced, WAIT_SECONDS)
        finally:
            await reading.rollback()
            await asyncio.gather(
                script,
                *([produced] if produced else []),
                return_exceptions=True,
            )
    finally:
        await reader.close()
        await writer.close()

    assert waited_on == ("relation", "AccessExclusiveLock")
    assert LOCK_TIMEOUT <= waited < WAIT_SECONDS
    # The INSERT went through while the reader's transaction was still open.
    assert await inspector.fetchval(f"SELECT count(*) FROM {postgres_table}") == 1  # noqa: S608


@pytest.mark.anyio
async def test_the_largest_lock_timeout_is_one_the_server_accepts(
    driver: str,
    empty_dsn: str,
) -> None:
    script = render_sql([table_schema("result_backend")], lock_timeout=2_147_483)

    await run_script(empty_dsn, driver, script)
    with pytest.raises(Exception, match="lock_timeout"):
        await run_script(
            empty_dsn,
            driver,
            script.replace("'2147483s'", "'2147484s'"),
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "name",
    ["o'brien", "back\\slash", "both'\\"],
    ids=["quote", "backslash", "both"],
)
async def test_the_sql_literal_of_a_quoted_name_hashes_like_the_raw_value(
    name: str,
    driver: str,
    empty_dsn: str,
) -> None:
    # No table can carry this name any more (ADR 0008), so the lock is built
    # from the name directly: the escaping still has to match the server.
    with pytest.raises(ValueError, match="is not a plain SQL identifier"):
        table_schema("broker", table_name=name)
    literal = _sql_literal(name)

    def check(literal: str) -> str:
        # Dollar quoting takes the name byte for byte: the value the runtime
        # binds as a parameter.
        return (
            f"DO $check$ BEGIN IF hashtext({literal}) IS DISTINCT FROM "
            f"hashtext($name${name}$name$) THEN "
            "RAISE EXCEPTION 'the lock key changed'; END IF; END $check$;"
        )

    await run_script(empty_dsn, driver, check(literal))
    # The check does fail on a literal that names another table.
    with pytest.raises(Exception, match="the lock key changed"):
        await run_script(empty_dsn, driver, check("'another table'"))


@pytest.mark.anyio
@pytest.mark.parametrize("name", ["Jobs_Mixed", "jobs$1"])
async def test_a_rendered_lock_line_hashes_like_the_runtime_lock_key(
    name: str,
    driver: str,
    empty_dsn: str,
) -> None:
    # End to end: the literal render_sql writes for an accepted name is the
    # key run_migrations takes by bind, mixed case included (not folded).
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        script = render_sql([table_schema("broker", table_name=name)])
    (lock_line,) = lock_lines(script)
    rendered = re.fullmatch(
        r"SELECT pg_advisory_xact_lock\(hashtext\('taskiq_postgresql'\), "
        r"hashtext\((?P<literal>.+)\)\);",
        lock_line,
    )
    assert rendered is not None, lock_line

    def check(literal: str) -> str:
        return (
            f"DO $check$ BEGIN IF hashtext({literal}) IS DISTINCT FROM "
            f"hashtext($name${name}$name$) THEN "
            "RAISE EXCEPTION 'the lock key changed'; END IF; END $check$;"
        )

    await run_script(empty_dsn, driver, check(rendered["literal"]))
    # A folded literal is another key: the check tells them apart.
    with pytest.raises(Exception, match="the lock key changed"):
        await run_script(empty_dsn, driver, check(f"'{name.lower()}x'"))
