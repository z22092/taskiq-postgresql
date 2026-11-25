"""
Migration steps that relax ``NOT NULL``, without a database.

A step may make a column nullable (``StepObjects.nullable_columns``). The plan
finds such a step pending while the column is still ``NOT NULL``, and every
consumer of the plan (``check``, the startup check, Alembic) follows from
that. The tables here are described by hand, because no released step relaxes
a column yet: see ``tests/relax_schema.py``. The Alembic side is in
``test_alembic_relax.py``.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping

import pytest

from taskiq_postgresql.exceptions import HOW_TO_MIGRATE, SchemaNotReadyError
from taskiq_postgresql.migrations import (
    CatalogState,
    MigrationPlan,
    MigrationStep,
    RenderedStep,
    StepObjects,
    TableSchema,
    TableState,
    catalog_query,
    plan_migration,
    table_schema,
)
from tests.relax_schema import (
    CATALOG_COLUMNS,
    RELAXED,
    TABLE,
    late_relaxing_schema,
    relaxing_schema,
)

STEP_2_SQL = f"ALTER TABLE {TABLE} ALTER COLUMN note DROP NOT NULL"
ALTER_NOTE = (
    f"step 2 of {TABLE}: ALTER TABLE takes an ACCESS EXCLUSIVE lock on {TABLE} "
    "for a moment; every query on the table waits for it"
)


def state(
    not_null: set[str],
    columns: Mapping[str, str] = CATALOG_COLUMNS,
) -> CatalogState:
    """The catalog of the test table, with these columns ``NOT NULL``."""
    return CatalogState(
        {
            TABLE: TableState(
                name=TABLE,
                relkind="r",
                columns=columns,
                not_null=frozenset(not_null),
            ),
        },
    )


def plan_with(
    not_null: set[str],
    columns: Mapping[str, str] = CATALOG_COLUMNS,
    schema: TableSchema | None = None,
) -> MigrationPlan:
    """Plan ``schema`` (the relaxing table by default) over such a catalog."""
    return plan_migration([schema or relaxing_schema()], state(not_null, columns))


def with_steps(schema: TableSchema, *steps: MigrationStep) -> TableSchema:
    """``schema`` with these steps, checked again as a new description is."""
    return dataclasses.replace(schema, steps=steps)


# --- the statement -------------------------------------------------------------


def test_the_step_runs_the_builders_statement() -> None:
    step_1, step_2 = relaxing_schema().steps

    assert step_2.statements == (STEP_2_SQL,)
    assert step_2.objects.nullable_columns == (RELAXED,)
    # A step that creates the column is not the one that relaxes it.
    assert step_1.objects.nullable_columns == ()


# --- the plan ------------------------------------------------------------------


def test_a_column_that_is_still_not_null_makes_the_step_pending() -> None:
    plan = plan_with({"id", "note"})

    (pending,) = plan.pending
    assert (pending.step.number, pending.table_missing) == (2, False)
    assert pending.not_null_columns == ("note",)
    assert pending.missing_columns == ()
    assert pending.missing_indexes == ()
    assert pending.severity == "required"
    assert pending.describe() == "columns still NOT NULL: note"
    assert plan.notes == (ALTER_NOTE,)
    assert not plan.is_current
    assert not plan.is_ready
    assert plan.statements(lock=False) == [STEP_2_SQL]
    assert plan.problems == ()


def test_a_nullable_column_leaves_the_step_done() -> None:
    plan = plan_with({"id"})

    assert plan == MigrationPlan(pending=(), problems=(), notes=())
    assert plan.is_current
    assert plan.is_ready


def test_a_column_created_with_the_table_and_lost_is_a_problem_not_a_step() -> None:
    # The catalog says nothing of a column that is not there, whatever the
    # not-null set holds. Step 1 creates the table, so no step adds the column
    # back: that one is a problem for a person, reported once.
    plan = plan_with({"id", "note"}, columns={"id": "integer"})

    assert plan.pending == ()
    (problem,) = plan.problems
    assert (problem.code, problem.severity) == ("missing_column", "required")
    assert "note" in problem.detail


def test_a_column_an_earlier_pending_step_adds_is_relaxed_in_the_same_run() -> None:
    # Step 2 adds ``note`` NOT NULL and step 3 relaxes it: once step 2 has run,
    # step 3 would be pending, so the plan carries both now.
    schema = late_relaxing_schema()

    plan = plan_with({"id"}, columns={"id": "integer"}, schema=schema)

    adding, relaxing = plan.pending
    assert (adding.step.number, adding.missing_columns) == (2, ("note",))
    assert adding.not_null_columns == ()
    assert (relaxing.step.number, relaxing.missing_columns) == (3, ())
    assert relaxing.not_null_columns == ("note",)
    assert relaxing.severity == "required"
    assert plan.statements(lock=False) == [
        *schema.steps[1].statements,
        *schema.steps[2].statements,
    ]
    assert plan.problems == ()


def test_a_column_that_exists_leaves_only_the_relaxing_step() -> None:
    # Once step 2 has run, the column exists and is NOT NULL: only step 3 is left.
    plan = plan_with(
        {"id", "note"},
        columns=CATALOG_COLUMNS,
        schema=late_relaxing_schema(),
    )

    (relaxing,) = plan.pending
    assert (relaxing.step.number, relaxing.not_null_columns) == (3, ("note",))


def test_a_missing_table_needs_the_step_but_names_no_column_in_it() -> None:
    plan = plan_migration(
        [relaxing_schema()],
        CatalogState({TABLE: TableState(TABLE)}),
    )

    step_1, step_2 = plan.pending
    assert (step_1.table_missing, step_2.table_missing) == (True, True)
    assert step_1.not_null_columns == step_2.not_null_columns == ()
    # The table is the missing object: no double report of its columns.
    assert step_2.describe() == "the table is missing"


# --- the comments of a step ----------------------------------------------------


def test_a_relaxing_step_says_it_relaxes_in_its_comments() -> None:
    step_1, step_2 = relaxing_schema().steps

    relaxing = RenderedStep.of(TABLE, "result_backend", step_2, transaction_note="x")
    creating = RenderedStep.of(TABLE, "result_backend", step_1, transaction_note="x")

    assert relaxing.nullable_columns == (RELAXED,)
    assert (
        "and each statement only adds what is missing, or makes note nullable "
        "where it is still NOT NULL."
    ) in relaxing.comments
    # The text of every released step stays as it was.
    assert creating.nullable_columns == ()
    assert "and each statement only adds what is missing." in creating.comments


# --- the catalog ---------------------------------------------------------------


def test_the_catalog_query_reads_attnotnull_beside_the_columns() -> None:
    query = catalog_query([relaxing_schema()])

    assert "attnotnull" in query
    # One query, and the pairs of the columns keep their shape.
    assert query.count("SELECT json_build_object('tables'") == 1
    assert "json_build_array(a.attname, format_type(a.atttypid, a.atttypmod))" in query


def test_the_catalog_state_reads_the_not_null_columns() -> None:
    document = {
        "tables": [
            {
                "name": TABLE,
                "relkind": "r",
                "columns": [["id", "integer"], ["note", "text"]],
                "not_null": ["id", "note"],
                "indexes": [],
            },
        ],
    }

    read = CatalogState.from_json(json.dumps(document))

    assert read.tables[TABLE].not_null == frozenset({"id", "note"})
    assert dict(read.tables[TABLE].columns) == CATALOG_COLUMNS


def test_a_table_without_not_null_columns_reads_an_empty_set() -> None:
    # json_agg over no row is NULL.
    table = {
        "name": TABLE,
        "relkind": None,
        "columns": None,
        "not_null": None,
        "indexes": [],
    }

    read = CatalogState.from_json(json.dumps({"tables": [table]}))

    assert read.tables[TABLE].not_null == frozenset()


def test_a_catalog_without_the_not_null_key_is_refused() -> None:
    # Read as "nothing is NOT NULL", a relaxing step would look done.
    table = {
        "name": TABLE,
        "relkind": "r",
        "columns": [["id", "integer"]],
        "indexes": [],
    }

    with pytest.raises(ValueError, match="not_null"):
        CatalogState.from_json(json.dumps({"tables": [table]}))


# --- the description is checked against its steps ------------------------------


def test_a_step_cannot_relax_a_column_the_table_does_not_have() -> None:
    schema = relaxing_schema()
    step = dataclasses.replace(
        schema.steps[1],
        objects=StepObjects(
            creates_table=False,
            columns=(),
            indexes=(),
            nullable_columns=("ghost",),
        ),
    )

    with pytest.raises(ValueError, match="no such column"):
        with_steps(schema, schema.steps[0], step)


def test_a_step_cannot_relax_a_column_the_final_description_has_not_null() -> None:
    schema = relaxing_schema()
    columns = tuple(
        dataclasses.replace(column, nullable=False)
        if column.name == RELAXED
        else column
        for column in schema.columns
    )

    with pytest.raises(ValueError, match="NOT NULL"):
        dataclasses.replace(schema, columns=columns)


def test_a_step_cannot_relax_a_column_it_adds_itself() -> None:
    schema = late_relaxing_schema()
    adds_and_relaxes = dataclasses.replace(
        schema.steps[1],
        objects=StepObjects(
            creates_table=False,
            columns=("note",),
            indexes=(),
            nullable_columns=("note",),
        ),
    )

    with pytest.raises(ValueError, match="no earlier step adds it"):
        with_steps(schema, schema.steps[0], adds_and_relaxes)


def test_a_step_cannot_relax_a_column_a_later_step_adds() -> None:
    schema = late_relaxing_schema()
    adds, relaxes = schema.steps[1:]
    adds_later = dataclasses.replace(adds, number=4)

    with pytest.raises(ValueError, match="no earlier step adds it"):
        with_steps(schema, schema.steps[0], relaxes, adds_later)


@pytest.mark.parametrize("kind", ["broker", "result_backend", "scheduler_source"])
def test_the_released_tables_relax_nothing(kind: str) -> None:
    schema = table_schema(kind)

    assert all(step.objects.nullable_columns == () for step in schema.steps)


# --- the startup check -----------------------------------------------------------


def test_the_startup_check_refuses_naming_the_step_and_the_column() -> None:
    plan = plan_with({"id", "note"})

    # start_table raises this when it does not migrate and the plan is not ready.
    message = str(SchemaNotReadyError(TABLE, plan.pending, plan.problems))

    assert not plan.is_ready
    assert f"{TABLE} is not ready and run_migrations is off" in message
    assert "step 2 has not run (columns still NOT NULL: note)" in message
    assert "step 1" not in message
    assert HOW_TO_MIGRATE in message
