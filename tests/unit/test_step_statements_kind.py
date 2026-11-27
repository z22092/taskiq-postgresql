"""
``step_statements(kind=...)``: one step of the tables of one component.

A step number means one thing for the broker's table and another for the
result backend's (both have a step 2), so a tool that runs a step needs to say
which kind it runs it for. No test here needs a database.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskiq_postgresql.migrations import (
    TableSchema,
    migration_statements,
    step_statements,
    table_schema,
)
from tests.helpers import lock_of

BROKER = table_schema("broker")
RESULTS = table_schema("result_backend")
SCHEDULER = table_schema("scheduler_source")


def run(
    schemas: list[TableSchema],
    **options: Any,
) -> list[str]:
    """``step_statements`` without a lock bound, so the statements read plainly."""
    return step_statements(schemas, lock_timeout=0, **options)


def test_without_a_kind_a_step_runs_for_every_table_that_has_it() -> None:
    statements = run([BROKER, RESULTS], step=2)

    assert statements == [
        lock_of(BROKER),
        lock_of(RESULTS),
        *BROKER.steps[1].statements,
        *RESULTS.steps[1].statements,
    ]


def test_the_broker_kind_runs_step_2_of_the_broker_tables_only() -> None:
    statements = run([BROKER, RESULTS], step=2, kind="broker")

    assert statements == [lock_of(BROKER), *BROKER.steps[1].statements]


def test_the_result_backend_kind_runs_step_2_of_the_results_tables_only() -> None:
    statements = run([BROKER, RESULTS], step=2, kind="result_backend")

    assert statements == [lock_of(RESULTS), *RESULTS.steps[1].statements]
    assert any("DROP NOT NULL" in statement for statement in statements)
    assert not any("channel" in statement for statement in statements)


def test_a_kind_with_no_table_among_the_schemas_runs_nothing() -> None:
    assert run([BROKER, SCHEDULER], step=2, kind="result_backend") == []


def test_a_kind_whose_tables_lack_the_step_runs_nothing() -> None:
    assert run([BROKER, RESULTS, SCHEDULER], step=2, kind="scheduler_source") == []


def test_a_bounded_run_of_a_kind_with_no_table_runs_nothing_at_all() -> None:
    # Not the bound around nothing: SET LOCAL and its restore would still run.
    assert step_statements([BROKER], step=2, kind="result_backend") == []


def test_a_kind_keeps_the_tables_of_that_kind_for_a_whole_run_too() -> None:
    statements = run([BROKER, RESULTS], kind="result_backend")

    assert statements == migration_statements([RESULTS])


def test_through_applies_to_the_tables_the_kind_keeps() -> None:
    # The scheduler table has no step 2, which would refuse the run if it
    # counted; the kind leaves it out.
    statements = run([SCHEDULER, RESULTS], through=2, kind="result_backend")

    assert statements == migration_statements([RESULTS], through=2)


def test_through_still_refuses_a_kept_table_that_lacks_the_step() -> None:
    with pytest.raises(ValueError, match="not step 2"):
        run([BROKER, SCHEDULER], through=2, kind="scheduler_source")


def test_a_kind_the_package_does_not_have_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown component kind"):
        run([BROKER], step=2, kind="queue")


def test_step_and_through_together_are_still_refused_with_a_kind() -> None:
    with pytest.raises(TypeError, match="step or through"):
        run([BROKER], step=2, through=2, kind="broker")


def test_no_schema_is_still_refused_with_a_kind() -> None:
    with pytest.raises(ValueError, match="at least one table"):
        run([], step=2, kind="broker")
