"""
What ``SchemaNotReadyError`` tells a person.

The error is built from a plan's pending steps and problems, so these tests
need no database.
"""

from taskiq_postgresql.exceptions import SchemaNotReadyError
from taskiq_postgresql.migrations import PendingStep, SchemaProblem, table_schema

# Commands that only run migration steps.
STEP_COMMANDS = ("taskiq postgresql migrate", "taskiq postgresql plan")


def test_a_problem_no_step_fixes_is_not_sent_to_the_steps() -> None:
    broker = table_schema("broker")
    step_2 = broker.steps[1]
    # A table that lost a column the CREATE TABLE of step 1 makes, and that
    # also lacks the index of step 2.
    lost_column = SchemaProblem(
        table=broker.name,
        code="missing_column",
        severity="required",
        detail=(
            f"{broker.name} has no column labels, which step 1 creates with "
            "the table: add it by hand"
        ),
    )
    index_only = PendingStep(
        table=broker.name,
        kind="broker",
        step=step_2,
        table_missing=False,
        missing_columns=(),
        missing_indexes=tuple(index.name for index in step_2.objects.indexes),
    )

    message = str(SchemaNotReadyError(broker.name, [index_only], [lost_column]))

    assert lost_column.detail in message
    # Running the steps would add the index and leave the column missing.
    for command in STEP_COMMANDS:
        assert command not in message
    assert "run_migrations=True" not in message
