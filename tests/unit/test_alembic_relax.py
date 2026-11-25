"""
The Alembic integration with a step that relaxes ``NOT NULL``, without a database.

Autogenerate proposes an ``alter_column`` for a column the metadata has
nullable and the database has ``NOT NULL``; the step already runs that
statement, so the comparator leaves it out. The tables are described by hand
(``tests/relax_schema.py``), and the engine's side is in
``test_migration_relax.py``.
"""

from __future__ import annotations

from typing import Any

import pytest
from alembic.operations import ops
from sqlalchemy import Integer, Text

from taskiq_postgresql.contrib.alembic import (
    TaskiqStepNoopOp,
    TaskiqStepOp,
    _drop_covered,
    alembic_format,
)
from taskiq_postgresql.migrations import FormatOptions
from tests.relax_schema import RELAXED, TABLE, relaxing_schema
from tests.unit.test_migration_relax import STEP_2_SQL, plan_with

pytestmark = pytest.mark.alembic

RELAXING_COMMENT = f"# taskiq-postgresql step 2 of {TABLE} makes {RELAXED} nullable: "


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
    upgrade_ops = ops.UpgradeOps([ops.ModifyTableOps(TABLE, list(alters))])
    _drop_covered(upgrade_ops, plan_with({"id", "note"}).pending)
    return upgrade_ops.ops


def test_alembic_leaves_only_the_step_for_a_relaxed_column() -> None:
    assert upgrade_after(alter()) == []


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(alter(modify_type=Integer()), id="type"),
        pytest.param(alter(modify_nullable=False), id="not-null-again"),
        pytest.param(alter(column="id"), id="a-column-no-step-relaxes"),
        pytest.param(alter(modify_server_default="x"), id="default"),
        pytest.param(alter(modify_comment="x"), id="comment"),
        pytest.param(alter(modify_name="other"), id="rename"),
        # A comparator of the user's own may add any modify_* key to kw.
        pytest.param(alter(modify_identity=True), id="identity"),
    ],
)
def test_alembic_keeps_what_is_not_the_steps_relaxation(
    operation: ops.AlterColumnOp,
) -> None:
    (modify,) = upgrade_after(operation)

    assert modify.ops == [operation]


def test_the_downgrade_of_a_relaxing_step_is_a_noop() -> None:
    (pending,) = plan_with({"id", "note"}).pending
    upgrade = TaskiqStepOp.from_pending(pending)

    downgrade = upgrade.reverse()

    assert isinstance(downgrade, TaskiqStepNoopOp)
    assert any(STEP_2_SQL in statement for statement in upgrade.statements)


@pytest.mark.parametrize("downgrade", ["none", "drop"])
def test_a_revision_never_puts_not_null_back(downgrade: str) -> None:
    source = alembic_format(
        [relaxing_schema()],
        FormatOptions(extra={"downgrade": downgrade}),
    )

    upgrade, _, back = source.partition("def downgrade()")
    assert STEP_2_SQL in upgrade
    assert "SET NOT NULL" not in source
    assert "ALTER COLUMN" not in back


def test_the_downgrade_comment_of_a_relaxing_step_says_what_it_leaves() -> None:
    source = alembic_format([relaxing_schema()], FormatOptions())

    header, _, rest = source.partition("def upgrade()")
    _, _, back = rest.partition("def downgrade()")
    assert f"{RELAXING_COMMENT}it stays, and NOT NULL is not put back." in back
    # Step 1 only adds: its text is the one every release wrote.
    assert f"# taskiq-postgresql step 1 of {TABLE} is additive: it stays." in back
    assert "makes note nullable where it is still NOT NULL" in header
