"""
Comparing a database with the table description.

``catalog_query``, ``CatalogState`` and ``plan_migration`` in
``taskiq_postgresql.migrations``.

The first tests build catalog states by hand. The others read real catalogs
through each driver's own ``fetch_row``, the way a component would, and check
two promises: every step declares exactly the objects its SQL creates, and a
released step never changes (``tests/golden/migration_steps.sql``).
"""

from __future__ import annotations

import dataclasses
import random
import string
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

import asyncpg
import pytest

from taskiq_postgresql import migrations
from taskiq_postgresql.migrations import (
    DEFAULT_LOCK_TIMEOUT,
    CatalogState,
    ColumnSchema,
    IndexSchema,
    IndexState,
    MigrationPlan,
    SchemaProblem,
    StepObjects,
    TableSchema,
    TableState,
    catalog_query,
    migration_statements,
    plan_migration,
    render_sql,
    table_schema,
)
from tests.helpers import (
    column_is_not_null,
    create_database,
    drop_database,
    in_database,
    run_script,
    script_of,
)
from tests.relax_schema import relaxing_schema
from tests.test_migration_script import SHAPE_IDS, SHAPES, build_component
from tests.test_migrations import BROKER_TABLE_0_4_SQL, close_component

GOLDEN = Path(__file__).parent / "golden" / "migration_steps.sql"
KINDS = ("broker", "result_backend", "scheduler_source")
# Mixed case, and long enough for PostgreSQL to shorten the names it derives.
LONG_NAME = "Taskiq_" + "Long" * 13
LOCK = (
    "SELECT pg_advisory_xact_lock(hashtext('taskiq_postgresql'), "
    "hashtext('taskiq_messages'))"
)
ALTER_NOTE = (
    "step 2 of taskiq_messages: ALTER TABLE takes an ACCESS EXCLUSIVE lock "
    "on taskiq_messages for a moment; every query on the table waits for it"
)
INDEX_NOTE = (
    "step 2 of taskiq_messages: CREATE INDEX takes a SHARE lock on "
    "taskiq_messages; writes wait until the index is built"
)
# What format_type() reports for each type the description names; the
# tests against PostgreSQL check these against real catalogs.
CATALOG_TYPES = {
    "serial": "integer",
    "uuid": "uuid",
    "varchar": "character varying",
    "text": "text",
    "bytea": "bytea",
    "jsonb": "jsonb",
    "timestamptz": "timestamp with time zone",
}


def catalog_type(column: ColumnSchema) -> str:
    spelling = CATALOG_TYPES[column.type_name]
    return spelling if column.length is None else f"{spelling}({column.length})"


def present(index: IndexSchema) -> IndexState:
    """The catalog entry of ``index`` as the step creates it."""
    return IndexState(
        name=index.name,
        relkind="i",
        on_table=True,
        valid=True,
        method=index.method,
        columns=index.columns,
    )


def state_after(schema: TableSchema, steps: int) -> CatalogState:
    """The catalog of ``schema``'s table after its first ``steps`` steps ran."""
    if steps == 0:
        return CatalogState({schema.name: TableState(schema.name)})
    done = schema.steps[:steps]
    created = {name for step in done for name in step.objects.columns}
    columns = [column for column in schema.columns if column.name in created]
    indexes = {
        index.name: present(index) for step in done for index in step.objects.indexes
    }
    return CatalogState(
        {
            schema.name: TableState(
                name=schema.name,
                relkind="r",
                columns={column.name: catalog_type(column) for column in columns},
                indexes=indexes,
            ),
        },
    )


def with_table(state: CatalogState, name: str, **changes: Any) -> CatalogState:
    return CatalogState(
        {**state.tables, name: dataclasses.replace(state.tables[name], **changes)},
    )


def with_index(state: CatalogState, table: str, index: IndexState) -> CatalogState:
    indexes = {**state.tables[table].indexes, index.name: index}
    return with_table(state, table, indexes=indexes)


def pending_of(plan: MigrationPlan) -> list[tuple[str, int]]:
    return [(item.table, item.step.number) for item in plan.pending]


# --- the plan, from catalog states built by hand -----------------------------


def test_a_missing_table_needs_every_step() -> None:
    broker = table_schema("broker")

    plan = plan_migration([broker], state_after(broker, 0))

    assert pending_of(plan) == [("taskiq_messages", 1), ("taskiq_messages", 2)]
    first, second = plan.pending
    assert first.table_missing
    assert first.missing_columns == broker.steps[0].objects.columns
    assert first.missing_indexes == ("taskiq_messages_id_idx",)
    assert second.missing_columns == ("channel", "not_before")
    assert (first.severity, second.severity) == ("required", "required")
    assert plan.problems == ()
    # A new table locks nothing anyone waits for.
    assert plan.notes == ()
    assert not plan.is_current
    assert not plan.is_ready
    assert plan.statements() == migration_statements([broker])


def test_a_0_4_table_needs_only_step_2() -> None:
    broker = table_schema("broker")

    plan = plan_migration([broker], state_after(broker, 1))

    (pending,) = plan.pending
    assert (pending.step.number, pending.table_missing) == (2, False)
    assert pending.missing_columns == ("channel", "not_before")
    assert pending.missing_indexes == ("taskiq_messages_not_before_idx",)
    assert pending.severity == "required"
    assert plan.statements() == [
        LOCK,
        *broker.steps[1].statements,
    ]
    assert plan.statements(lock=False) == list(broker.steps[1].statements)
    assert plan.notes == (ALTER_NOTE, INDEX_NOTE)


@pytest.mark.parametrize("kind", KINDS)
def test_a_table_with_every_object_is_current(kind: str) -> None:
    schema = table_schema(kind)

    plan = plan_migration([schema], state_after(schema, len(schema.steps)))

    assert plan == MigrationPlan(pending=(), problems=(), notes=())
    assert plan.is_current
    assert plan.is_ready
    assert plan.statements() == []
    assert plan.render_sql() == ""


def test_a_missing_index_reruns_its_step_and_is_advisory() -> None:
    broker = table_schema("broker")
    complete = state_after(broker, 2)
    indexes = dict(complete.tables["taskiq_messages"].indexes)
    del indexes["taskiq_messages_id_idx"]

    plan = plan_migration(
        [broker],
        with_table(complete, "taskiq_messages", indexes=indexes),
    )

    (pending,) = plan.pending
    assert (pending.step.number, pending.missing_columns) == (1, ())
    assert pending.missing_indexes == ("taskiq_messages_id_idx",)
    assert pending.severity == "advisory"
    assert plan.is_ready
    assert not plan.is_current


def test_step_2_columns_without_their_index_still_need_step_2() -> None:
    broker = table_schema("broker")
    complete = state_after(broker, 2)
    indexes = dict(complete.tables["taskiq_messages"].indexes)
    del indexes["taskiq_messages_not_before_idx"]

    plan = plan_migration(
        [broker],
        with_table(complete, "taskiq_messages", indexes=indexes),
    )

    assert pending_of(plan) == [("taskiq_messages", 2)]
    assert plan.pending[0].severity == "advisory"
    assert plan.notes == (ALTER_NOTE, INDEX_NOTE)


@pytest.mark.parametrize(
    ("index", "code"),
    [
        (
            IndexState("taskiq_messages_id_idx", "i", True, False, "hash", ("id",)),
            "invalid_index",
        ),
        (
            IndexState("taskiq_messages_id_idx", "i", False, True, "hash", ("id",)),
            "index_on_other_table",
        ),
        (
            IndexState("taskiq_messages_id_idx", "i", True, True, "btree", ("id",)),
            "index_shape",
        ),
        (
            IndexState(
                "taskiq_messages_id_idx", "i", True, True, "hash", ("id",), True
            ),
            "index_shape",
        ),
        (
            IndexState(
                "taskiq_messages_id_idx",
                "i",
                True,
                True,
                "hash",
                ("id",),
                partial=True,
            ),
            "index_shape",
        ),
        (IndexState("taskiq_messages_id_idx", "r"), "name_taken"),
    ],
    ids=["invalid", "other-table", "method", "unique", "partial", "not-an-index"],
)
def test_an_index_no_step_can_fix_is_a_problem_not_a_pending_step(
    index: IndexState,
    code: str,
) -> None:
    broker = table_schema("broker")

    plan = plan_migration(
        [broker],
        with_index(state_after(broker, 2), "taskiq_messages", index),
    )

    # IF NOT EXISTS would skip the name: running step 1 again changes nothing.
    assert plan.pending == ()
    (problem,) = plan.problems
    assert (problem.table, problem.code, problem.severity) == (
        "taskiq_messages",
        code,
        "advisory",
    )
    assert "taskiq_messages_id_idx" in problem.detail
    assert plan.is_current
    assert plan.is_ready


def test_a_name_taken_by_another_relation_is_a_required_problem() -> None:
    broker = table_schema("broker")
    view = with_table(state_after(broker, 0), "taskiq_messages", relkind="v")

    plan = plan_migration([broker], view)

    assert plan.pending == ()
    assert [(problem.code, problem.severity) for problem in plan.problems] == [
        ("name_taken", "required"),
    ]
    assert not plan.is_ready


def test_a_column_the_creating_step_cannot_restore_is_a_required_problem() -> None:
    broker = table_schema("broker")
    complete = state_after(broker, 2)
    columns = dict(complete.tables["taskiq_messages"].columns)
    del columns["labels"]

    plan = plan_migration(
        [broker],
        with_table(complete, "taskiq_messages", columns=columns),
    )

    assert plan.pending == ()
    assert plan.problems == (
        SchemaProblem(
            table="taskiq_messages",
            code="missing_column",
            severity="required",
            detail=(
                "taskiq_messages has no column labels, which step 1 creates "
                "with the table: add it by hand"
            ),
        ),
    )
    assert not plan.is_ready


def test_a_column_of_another_type_is_an_advisory_problem() -> None:
    broker = table_schema("broker")
    complete = state_after(broker, 2)
    columns = {**complete.tables["taskiq_messages"].columns, "task_id": "text"}

    plan = plan_migration(
        [broker],
        with_table(complete, "taskiq_messages", columns=columns),
    )

    assert plan.pending == ()
    (problem,) = plan.problems
    assert (problem.code, problem.severity) == ("column_type", "advisory")
    assert problem.detail.startswith("taskiq_messages.task_id is 'text', not uuid")


def test_a_catalog_type_with_a_line_break_is_written_escaped() -> None:
    broker = table_schema("broker")
    complete = state_after(broker, 2)
    hostile = "text\nERROR forged log line"
    columns = {**complete.tables["taskiq_messages"].columns, "task_id": hostile}

    plan = plan_migration(
        [broker],
        with_table(complete, "taskiq_messages", columns=columns),
    )

    (problem,) = plan.problems
    assert "\n" not in problem.detail
    assert repr(hostile) in problem.detail


def test_pending_steps_come_by_table_name_then_by_step() -> None:
    schemas = [
        table_schema("scheduler_source", table_name="c"),
        table_schema("broker", table_name="b"),
        table_schema("result_backend", table_name="a"),
    ]
    empty = CatalogState({schema.name: TableState(schema.name) for schema in schemas})

    plan = plan_migration(schemas, empty)

    assert pending_of(plan) == [("a", 1), ("b", 1), ("b", 2), ("c", 1)]
    assert plan.statements() == migration_statements(
        sorted(schemas, key=lambda schema: schema.name),
    )


def test_the_plan_renders_the_script_of_its_pending_steps() -> None:
    broker = table_schema("broker")
    plan = plan_migration([broker], state_after(broker, 1))

    script = plan.render_sql()

    lines = script.splitlines()
    assert lines[0].startswith("-- taskiq-postgresql ")
    assert lines[1] == "-- pending: taskiq_messages step 2"
    # As in render_sql, the lock timeout comes first, so that it also bounds
    # the wait for the advisory lock.
    assert script.split("\n", 2)[2] == "".join(
        f"{statement};\n"
        for statement in [
            "BEGIN",
            f"SET LOCAL lock_timeout = '{DEFAULT_LOCK_TIMEOUT}s'",
            *plan.statements(),
            "COMMIT",
        ]
    )
    assert "SET LOCAL lock_timeout = '3s';" in plan.render_sql(lock_timeout=3)
    assert "lock_timeout" not in plan.render_sql(lock_timeout=0)
    with pytest.raises(ValueError, match="lock_timeout must be a whole number"):
        plan.render_sql(lock_timeout=-1)
    assert "BEGIN" not in plan.render_sql(transaction=False)
    assert "pg_advisory_xact_lock" not in plan.render_sql(lock=False)


def test_a_state_built_for_other_tables_is_refused() -> None:
    with pytest.raises(LookupError, match="'taskiq_messages'"):
        plan_migration([table_schema("broker")], CatalogState({}))


@pytest.mark.parametrize(
    "text",
    ["not json", "{}", '{"tables": [{"name": "t"}]}', "[]"],
    ids=["not-json", "no-tables", "no-indexes", "array"],
)
def test_text_that_is_not_the_query_result_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match="not the result of catalog_query"):
        CatalogState.from_json(text)


def test_an_empty_catalog_query_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one"):
        catalog_query([])


def test_a_step_declares_the_objects_it_creates() -> None:
    broker = table_schema("broker", table_name="jobs")

    first, second = broker.steps

    assert first.objects == StepObjects(
        creates_table=True,
        columns=("id", "task_id", "task_name", "message", "labels", "created_at"),
        indexes=(IndexSchema("jobs_id_idx", ("id",), "hash"),),
    )
    assert second.objects == StepObjects(
        creates_table=False,
        columns=("channel", "not_before"),
        indexes=(
            IndexSchema(
                "jobs_not_before_idx", ("channel", "not_before", "id"), "btree"
            ),
        ),
    )
    # Together the steps declare the whole table.
    assert [name for step in broker.steps for name in step.objects.columns] == [
        column.name for column in broker.columns
    ]
    assert tuple(index for step in broker.steps for index in step.objects.indexes) == (
        broker.indexes
    )


# --- the through argument -----------------------------------------------------


def test_through_stops_after_that_step() -> None:
    broker = table_schema("broker")

    assert migration_statements([broker], through=1) == migration_statements(
        [dataclasses.replace(broker, steps=broker.steps[:1])],
    )
    assert migration_statements([broker], through=2) == migration_statements([broker])


@pytest.mark.parametrize("through", [0, 2, 3])
def test_through_must_name_a_step_every_table_has(through: int) -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    with pytest.raises(ValueError, match=f"not step {through}"):
        migration_statements(schemas, through=through)


# --- released steps never change ----------------------------------------------


def golden_text() -> str:
    """Every step of every table shape, with the objects it declares."""
    lines = [
        "-- The released migration steps, per table shape with the default name.",
        "-- A released step never changes: a later release adds a step instead,",
        "-- because revisions and scripts written from a step must stay valid.",
    ]
    for kind, field in SHAPES:
        schema = table_schema(kind, field_for_task_id=field)
        for step in schema.steps:
            objects = step.objects
            indexes = "; ".join(
                f"{index.name} {index.method} ({', '.join(index.columns)})"
                for index in objects.indexes
            )
            lines.extend(
                [
                    "",
                    f"-- {kind}{f' {field}' if field else ''}: step {step.number}",
                    f"-- creates table: {'yes' if objects.creates_table else 'no'}",
                    f"-- columns: {', '.join(objects.columns)}",
                    f"-- indexes: {indexes}",
                    *(f"{statement};" for statement in step.statements),
                ],
            )
    return "\n".join(lines) + "\n"


def test_released_steps_match_the_golden_file() -> None:
    assert golden_text() == GOLDEN.read_text(encoding="utf-8"), (
        "a migration step changed; released steps are immutable, add a new step "
        "(and update tests/golden/migration_steps.sql only for a new one)"
    )


def test_editing_a_released_step_breaks_the_golden_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    released = migrations._added_statements

    def reordered(*arguments: Any) -> tuple[str, ...]:
        *statements, index = released(*arguments)
        return (
            *statements,
            index.replace("(channel, not_before", "(not_before, channel"),
        )

    monkeypatch.setattr(migrations, "_added_statements", reordered)

    assert golden_text() != GOLDEN.read_text(encoding="utf-8")


# --- against PostgreSQL, through each driver -----------------------------------


@pytest.fixture
async def empty_dsn(postgresql_dsn: str) -> AsyncGenerator[str, None]:
    """DSN of a database created empty for one test and dropped after it."""
    name = "plan_" + "".join(random.choices(string.ascii_lowercase, k=10))
    await create_database(postgresql_dsn, name)
    yield in_database(postgresql_dsn, name)
    await drop_database(postgresql_dsn, name)


async def read_catalog(
    dsn: str,
    driver: str,
    schemas: list[TableSchema],
) -> CatalogState:
    """Run ``catalog_query`` through ``driver``'s ``fetch_row``, as a component."""
    component = build_component("broker", dsn=dsn, driver=driver)
    try:
        row = await component.driver.fetch_row(catalog_query(schemas))
    finally:
        await close_component(component)
    assert row is not None
    assert list(row) == ["catalog"]
    assert isinstance(row["catalog"], str)
    return CatalogState.from_json(row["catalog"])


async def object_catalog(
    connection: asyncpg.Connection,
    table: str,
) -> tuple[set[str], set[tuple[str, str, tuple[str, ...]]], bool]:
    """
    Return the columns, the indexes and whether a primary key exists.

    Read through ``information_schema`` and ``pg_indexes``, apart from the
    engine's own query, so the check does not trust what it checks.
    """
    columns = {
        row[0]
        for row in await connection.fetch(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = current_schema() AND table_name = $1",
            table,
        )
    }
    definitions = await connection.fetch(
        "SELECT indexname, indexdef FROM pg_indexes"
        " WHERE schemaname = current_schema() AND tablename = $1",
        table,
    )
    indexes = set()
    has_primary_key = False
    for name, definition in definitions:
        if name == f"{table}_pkey":
            has_primary_key = True
            continue
        method, _, columns_text = definition.partition(" USING ")[2].partition(" ")
        indexed = tuple(columns_text.strip("()").split(", "))
        indexes.add((name, method, indexed))
    return columns, indexes, has_primary_key


@pytest.mark.anyio
@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
async def test_each_step_creates_exactly_the_objects_it_declares(
    kind: str,
    field: Optional[str],
    driver: str,
    empty_dsn: str,
) -> None:
    schema = table_schema(kind, table_name="jobs", field_for_task_id=field)
    inspector = await asyncpg.connect(empty_dsn)
    try:
        before = await object_catalog(inspector, "jobs")
        assert before == (set(), set(), False)
        for step in schema.steps:
            await run_script(
                empty_dsn,
                driver,
                "".join(f"{statement};\n" for statement in step.statements),
            )
            after = await object_catalog(inspector, "jobs")

            assert after[0] - before[0] == set(step.objects.columns)
            assert after[1] - before[1] == {
                (index.name, index.method, index.columns)
                for index in step.objects.indexes
            }
            # A step removes nothing, and only the creating step adds the key.
            assert before[0] <= after[0]
            assert before[1] <= after[1]
            assert after[2] is (before[2] or step.objects.creates_table)
            before = after
    finally:
        await inspector.close()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_the_catalog_follows_the_steps_through_every_driver(
    kind: str,
    driver: str,
    empty_dsn: str,
) -> None:
    schema = table_schema(kind, table_name="jobs")

    seen = [plan_migration([schema], await read_catalog(empty_dsn, driver, [schema]))]
    for step in schema.steps:
        await run_script(
            empty_dsn,
            driver,
            "".join(f"{statement};\n" for statement in step.statements),
        )
        seen.append(
            plan_migration([schema], await read_catalog(empty_dsn, driver, [schema])),
        )

    numbers = [step.number for step in schema.steps]
    assert [[item.step.number for item in plan.pending] for plan in seen] == [
        numbers[index:] for index in range(len(numbers) + 1)
    ]
    assert all(plan.problems == () for plan in seen)
    assert seen[-1].is_current


@pytest.mark.anyio
async def test_a_step_that_relaxes_not_null_is_pending_until_it_has_run(
    driver: str,
    empty_dsn: str,
) -> None:
    schema = relaxing_schema()
    first, second = schema.steps
    await run_script(empty_dsn, driver, script_of(first.statements))

    before = await read_catalog(empty_dsn, driver, [schema])
    plan = plan_migration([schema], before)

    # The catalog reports the constraint the first step wrote, and the key's.
    assert before.tables[schema.name].not_null == {"id", "note"}
    (pending,) = plan.pending
    assert pending.step == second
    assert pending.not_null_columns == ("note",)
    assert pending.severity == "required"
    assert (pending.missing_columns, pending.missing_indexes) == ((), ())
    assert plan.problems == ()

    await run_script(empty_dsn, driver, script_of(plan.statements()))
    after = await read_catalog(empty_dsn, driver, [schema])

    # Read apart from the engine's own query, so the check does not trust what
    # it checks.
    assert not await column_is_not_null(empty_dsn, schema.name, "note")
    assert await column_is_not_null(empty_dsn, schema.name, "id")
    assert after.tables[schema.name].not_null == {"id"}
    assert plan_migration([schema], after) == MigrationPlan((), (), ())
    # The engine only runs it while pending; running it again is harmless too.
    await run_script(empty_dsn, driver, script_of(second.statements))


@pytest.mark.anyio
async def test_a_0_4_broker_table_needs_step_2_and_run_migrations_completes_it(
    driver: str,
    empty_dsn: str,
) -> None:
    broker = table_schema("broker", table_name="jobs")
    inspector = await asyncpg.connect(empty_dsn)
    try:
        await inspector.execute(BROKER_TABLE_0_4_SQL.format(table="jobs"))
    finally:
        await inspector.close()

    before = plan_migration([broker], await read_catalog(empty_dsn, driver, [broker]))
    component = build_component(
        "broker", dsn=empty_dsn, table_name="jobs", driver=driver
    )
    try:
        await component.startup()
    finally:
        await close_component(component)
    after = plan_migration([broker], await read_catalog(empty_dsn, driver, [broker]))

    (pending,) = before.pending
    assert (pending.step.number, pending.severity) == (2, "required")
    assert pending.missing_columns == ("channel", "not_before")
    assert before.problems == ()
    assert after.is_current
    assert after.problems == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kind", "table_name"),
    [("result_backend", LONG_NAME), ("broker", "Jobs")],
    ids=["long-mixed-case", "mixed-case"],
)
async def test_names_resolve_as_postgresql_folds_and_shortens_them(
    kind: str,
    table_name: str,
    driver: str,
    empty_dsn: str,
) -> None:
    schema = table_schema(kind, table_name=table_name)
    component = build_component(
        kind, dsn=empty_dsn, table_name=table_name, driver=driver
    )
    try:
        await component.startup()
    finally:
        await close_component(component)

    plan = plan_migration([schema], await read_catalog(empty_dsn, driver, [schema]))

    assert plan == MigrationPlan(pending=(), problems=(), notes=())


@pytest.mark.anyio
async def test_an_invalid_index_and_an_index_on_another_table_are_problems(
    driver: str,
    empty_dsn: str,
) -> None:
    broker = table_schema("broker", table_name="jobs")
    inspector = await asyncpg.connect(empty_dsn)
    try:
        # The name of jobs' first index, taken first by another table.
        await inspector.execute("CREATE TABLE other (id INTEGER)")
        await inspector.execute("CREATE INDEX jobs_id_idx ON other USING HASH (id)")
        await run_script(empty_dsn, driver, render_sql([broker]))
        # What a failed CREATE INDEX CONCURRENTLY leaves (superuser only).
        await inspector.execute(
            "UPDATE pg_index SET indisvalid = false"
            " WHERE indexrelid = to_regclass('jobs_not_before_idx')",
        )
    finally:
        await inspector.close()

    plan = plan_migration([broker], await read_catalog(empty_dsn, driver, [broker]))

    assert plan.pending == ()
    assert [(problem.code, problem.severity) for problem in plan.problems] == [
        ("index_on_other_table", "advisory"),
        ("invalid_index", "advisory"),
    ]
