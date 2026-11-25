from typing import TYPE_CHECKING, Final, Sequence

if TYPE_CHECKING:
    from taskiq_postgresql.migrations import PendingStep, SchemaProblem

HOW_TO_MIGRATE: Final = (
    "Create what is missing with `taskiq postgresql migrate <module>:<object>`, "
    "with the SQL `taskiq postgresql plan <module>:<object>` prints, or with an "
    "Alembic revision (`alembic revision --autogenerate`); or start once with "
    "run_migrations=True."
)
"""
What creates a missing table, column or index: the ways to run the steps.

``<module>:<object>`` stays for the reader to fill in: a component cannot
know the import path of the application that built it.
"""


class BaseTaskiqAsyncpgError(Exception):
    """Base error for all possible exception in the lib."""


class DatabaseConnectionError(BaseTaskiqAsyncpgError):
    """Error if cannot connect to PostgreSQL."""


class ResultIsMissingError(BaseTaskiqAsyncpgError):
    """Error if cannot retrieve result from PostgreSQL."""


class SchemaNotReadyError(BaseTaskiqAsyncpgError):
    """
    A component's table lacks what the component needs, and it does not migrate.

    With ``run_migrations=False``, ``startup()`` raises it before anything else
    starts when the table is missing, when a migration step that adds columns
    has not run (a broker table created by 0.4.x lacks ``channel`` and
    ``not_before``), when a step that makes a column nullable has not run, or
    when something no step fixes keeps the component from working. Nothing is
    created: the message says how to create what a step creates
    (``HOW_TO_MIGRATE``), and what to do about the rest.

    Attributes:
        table: The table name.
        pending: The migration steps the table still needs.
        problems: What is wrong in a way no step fixes, each with what to do.
    """

    def __init__(
        self,
        table: str,
        pending: Sequence["PendingStep"] = (),
        problems: Sequence["SchemaProblem"] = (),
    ) -> None:
        self.table = table
        self.pending = tuple(pending)
        self.problems = tuple(problems)
        super().__init__(_not_ready_message(table, self.pending, self.problems))


def _not_ready_message(
    table: str,
    pending: Sequence["PendingStep"],
    problems: Sequence["SchemaProblem"],
) -> str:
    required = [item for item in pending if item.severity == "required"]
    found = []
    if any(item.table_missing for item in required):
        steps = ", ".join(f"step {item.step.number}" for item in pending)
        found.append(f"the table does not exist ({steps} not run)")
    else:
        found.extend(
            f"step {item.step.number} has not run ({item.describe()})"
            for item in required
        )
    found.extend(problem.detail for problem in problems)
    message = f"{table} is not ready and run_migrations is off: {'; '.join(found)}."
    # Running the steps only helps when a step is what is missing; a problem
    # keeps its own advice, and an index alone does not stop a start.
    return f"{message} {HOW_TO_MIGRATE}" if required else message
