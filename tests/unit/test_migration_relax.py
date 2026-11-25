"""
Migration steps that relax ``NOT NULL``, without a database.

A step may make a column nullable (``StepObjects.nullable_columns``). The plan
finds such a step pending while the column is still ``NOT NULL``, and every
consumer of the plan (``check``, the startup check, Alembic) follows from
that. The tables here are described by hand, because no released step relaxes
a column yet: see ``tests/relax_schema.py``.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

import pytest
from alembic.operations import ops
from sqlalchemy import Integer, Text

from taskiq_postgresql.exceptions import HOW_TO_MIGRATE, SchemaNotReadyError
from taskiq_postgresql.migrations import (
    CatalogState,
    FormatOptions,
    MigrationPlan,
    StepObjects,
    TableState,
    catalog_query,
    plan_migration,
    table_schema,
)
from tests.relax_schema import CATALOG_COLUMNS, RELAXED, TABLE, relaxing_schema

STEP_2_SQL = f"ALTER TABLE {TABLE} ALTER COLUMN note DROP NOT NULL"
ALTER_NOTE = (
    f"step 2 of {TABLE}: ALTER TABLE takes an ACCESS EXCLUSIVE lock on {TABLE} "
    "for a moment; every query on the table waits for it"
)


def state(
    not_null: set[str],
    columns: dict[str, str] = CATALOG_COLUMNS,
    **changes: Any,
) -> CatalogState:
    """The catalog of the test table, with these columns ``NOT NULL``."""
    return CatalogState(
        {
            TABLE: TableState(
                name=TABLE,
                relkind="r",
                columns=columns,
                not_null=frozenset(not_null),
                **changes,
            ),
        },
    )


def plan_of(not_null: set[str], **options: Any) -> MigrationPlan:
    return plan_migration([relaxing_schema()], state(not_null, **options))


# --- the statement -------------------------------------------------------------


def test_the_builder_writes_one_statement_per_column() -> None:
    from taskiq_postgresql.abc.query import DropNotNullQuery  # noqa: PLC0415

    query = DropNotNullQuery("results").make_query("result")

    assert query == "ALTER TABLE results ALTER COLUMN result DROP NOT NULL;"


def test_the_step_runs_the_builders_statement() -> None:
    step_1, step_2 = relaxing_schema().steps

    assert step_2.statements == (STEP_2_SQL,)
    assert step_2.objects.nullable_columns == (RELAXED,)
    # A step that creates the column is not the one that relaxes it.
    assert step_1.objects.nullable_columns == ()


# --- the plan ------------------------------------------------------------------


def test_a_column_that_is_still_not_null_makes_the_step_pending() -> None:
    plan = plan_of({"id", "note"})

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
    plan = plan_of({"id"})

    assert plan == MigrationPlan(pending=(), problems=(), notes=())
    assert plan.is_current
    assert plan.is_ready


def test_a_column_the_table_lacks_is_the_earlier_steps_business() -> None:
    # The catalog says nothing of a column that is not there, whatever the
    # not-null set holds.
    plan = plan_of({"id", "note"}, columns={"id": "integer"})

    assert plan.pending == ()
    # Step 1 creates the table, so no step adds the column back: that one
    # is a problem for a person, reported once.
    (problem,) = plan.problems
    assert (problem.code, problem.severity) == ("missing_column", "required")
    assert "note" in problem.detail


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


def test_a_state_without_not_null_means_nothing_is_not_null() -> None:
    # The states the other tests build by hand have no not_null at all.
    plan = plan_migration(
        [relaxing_schema()],
        CatalogState(
            {TABLE: TableState(TABLE, relkind="r", columns=CATALOG_COLUMNS)},
        ),
    )

    assert plan.pending == ()


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
    # json_agg over no row is NULL, and a state written before the key existed
    # has no key.
    for missing in ({"not_null": None}, {}):
        table = {"name": TABLE, "relkind": None, "columns": None, "indexes": []}
        read = CatalogState.from_json(json.dumps({"tables": [{**table, **missing}]}))

        assert read.tables[TABLE].not_null == frozenset()


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
        dataclasses.replace(schema, steps=(schema.steps[0], step))


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


def test_the_released_tables_relax_nothing() -> None:
    for kind in ("broker", "result_backend", "scheduler_source"):
        schema = table_schema(kind)

        assert all(step.objects.nullable_columns == () for step in schema.steps)


# --- the startup check -----------------------------------------------------------


def test_the_startup_check_refuses_naming_the_step_and_the_column() -> None:
    plan = plan_of({"id", "note"})

    # start_table raises this when it does not migrate and the plan is not ready.
    message = str(SchemaNotReadyError(TABLE, plan.pending, plan.problems))

    assert not plan.is_ready
    assert f"{TABLE} is not ready and run_migrations is off" in message
    assert "step 2 has not run (columns still NOT NULL: note)" in message
    assert "step 1" not in message
    assert HOW_TO_MIGRATE in message


# --- Alembic -------------------------------------------------------------------


def alter(column: str = RELAXED, **changes: Any) -> ops.AlterColumnOp:
    """What Alembic proposes for a column that is NOT NULL and should not be."""
    options: dict[str, Any] = {
        "existing_type": Text(),
        "existing_nullable": False,
        "modify_nullable": True,
        **changes,
    }
    return ops.AlterColumnOp(TABLE, column, **options)


def upgrade_after(*alters: ops.AlterColumnOp) -> list[Any]:
    """Run ``_drop_covered`` on a revision that alters ``alters`` of the table."""
    from taskiq_postgresql.contrib.alembic import _drop_covered  # noqa: PLC0415

    upgrade_ops = ops.UpgradeOps([ops.ModifyTableOps(TABLE, list(alters))])
    _drop_covered(upgrade_ops, plan_of({"id", "note"}).pending)
    return upgrade_ops.ops


def test_alembic_leaves_only_the_step_for_a_relaxed_column() -> None:
    assert upgrade_after(alter()) == []


def test_alembic_keeps_the_other_changes_to_that_column() -> None:
    kept = alter(modify_type=Integer())

    (modify,) = upgrade_after(kept)

    assert modify.ops == [kept]


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(alter(modify_nullable=False), id="not-null-again"),
        pytest.param(alter(column="id"), id="a-column-no-step-relaxes"),
        pytest.param(alter(modify_server_default="x"), id="default"),
        pytest.param(alter(modify_comment="x"), id="comment"),
        pytest.param(alter(modify_name="other"), id="rename"),
    ],
)
def test_alembic_keeps_what_is_not_the_steps_relaxation(
    operation: ops.AlterColumnOp,
) -> None:
    (modify,) = upgrade_after(operation)

    assert modify.ops == [operation]


def test_the_downgrade_of_a_relaxing_step_is_a_noop() -> None:
    from taskiq_postgresql.contrib.alembic import (  # noqa: PLC0415
        TaskiqStepNoopOp,
        TaskiqStepOp,
    )

    (pending,) = plan_of({"id", "note"}).pending
    upgrade = TaskiqStepOp.from_pending(pending)

    downgrade = upgrade.reverse()

    assert isinstance(downgrade, TaskiqStepNoopOp)
    assert any(STEP_2_SQL in statement for statement in upgrade.statements)
    assert not hasattr(downgrade, "statements")


@pytest.mark.parametrize("downgrade", ["none", "drop"])
def test_a_revision_never_puts_not_null_back(downgrade: str) -> None:
    from taskiq_postgresql.contrib.alembic import alembic_format  # noqa: PLC0415

    source = alembic_format(
        [relaxing_schema()],
        FormatOptions(extra={"downgrade": downgrade}),
    )

    upgrade, _, back = source.partition("def downgrade()")
    assert STEP_2_SQL in upgrade
    assert "SET NOT NULL" not in source
    assert "ALTER COLUMN" not in back
