"""
The operation the Django app's migrations hold: one migration step.

Migration ``0001`` runs step 1 of every configured table, ``0002`` step 2,
and so on: a release that adds a step adds the migration that runs it. The
operation reads the settings when ``migrate`` applies it, so the migrations
are the same for every project.
"""

from __future__ import annotations

from typing import Any

from django.db import NotSupportedError, router
from django.db.migrations.operations.base import Operation, OperationCategory

from taskiq_postgresql.contrib.django.conf import (
    TARGETS_SETTING,
    configured_lock_timeout,
    configured_schemas,
)
from taskiq_postgresql.migrations import step_statements

__all__ = ["RunTaskiqStep"]


class RunTaskiqStep(Operation):
    """
    Run one migration step of every table ``TASKIQ_POSTGRESQL_TARGETS`` names.

    When ``migrate`` applies the migration, the operation describes the
    tables the settings name and runs the step's statements for each table
    that has the step, one statement at a time, in the migration's
    transaction. They are the statements ``run_migrations=True`` runs: the
    advisory lock it takes comes first, so workers that start meanwhile wait
    for the migration, and every statement only adds what is missing, or
    relaxes a ``NOT NULL`` that is still there. The
    lock waits are bounded as ``taskiq postgresql generate`` bounds them:
    ``SET LOCAL lock_timeout`` to ``TASKIQ_POSTGRESQL_LOCK_TIMEOUT`` seconds
    (``DEFAULT_LOCK_TIMEOUT`` unless set, no statement with ``0``) after
    keeping the transaction's own value, which the last statement restores.
    ``sqlmigrate`` shows the statements.

    A database router's ``allow_migrate`` decides, as for any operation, and
    a database that is not PostgreSQL is refused.

    The reverse runs nothing, so ``migrate taskiq_postgresql zero`` leaves
    the tables in place: a step adds tables, columns and indexes, which an
    earlier release ignores, and dropping them would lose the messages,
    results and schedules they hold; a step that makes a column nullable
    leaves it so, because putting ``NOT NULL`` back would fail on a row that
    holds a ``NULL``.

    Args:
        number: The step it runs.
    """

    reduces_to_sql = True
    reversible = True
    category = OperationCategory.SQL

    def __init__(self, number: int) -> None:
        self.number = number

    def state_forwards(self, app_label: str, state: Any) -> None:
        """Change nothing: the tables are not Django models."""

    def database_forwards(
        self,
        app_label: str,
        schema_editor: Any,
        from_state: Any,
        to_state: Any,
    ) -> None:
        """
        Run the step for every configured table that has it.

        Args:
            app_label: The app's label.
            schema_editor: The schema editor of the migration's database.
            from_state: The project state before the migration.
            to_state: The project state after it.

        Raises:
            ImproperlyConfigured: if ``TASKIQ_POSTGRESQL_TARGETS`` is missing,
                empty or malformed, a path in it cannot be resolved, or
                ``TASKIQ_POSTGRESQL_LOCK_TIMEOUT`` is not a whole number of
                seconds from 0 to 2147483. Nothing runs then.
            NotSupportedError: if the database is not PostgreSQL.
        """
        connection = schema_editor.connection
        if not router.allow_migrate(connection.alias, app_label):
            return
        if connection.vendor != "postgresql":
            raise NotSupportedError(
                "the taskiq-postgresql tables need PostgreSQL, and the database "
                f"{connection.alias!r} is {connection.vendor}: keep the app "
                f"{app_label} off it with a database router",
            )
        statements = step_statements(
            configured_schemas(),
            step=self.number,
            lock_timeout=configured_lock_timeout(),
        )
        for statement in statements:
            # Without parameters the statement goes as written: a "%" in it
            # is never read as a placeholder.
            schema_editor.execute(statement, params=None)

    def database_backwards(
        self,
        app_label: str,
        schema_editor: Any,
        from_state: Any,
        to_state: Any,
    ) -> None:
        """Run nothing: the step's tables, columns and indexes stay."""

    def describe(self) -> str:
        """
        Say what the operation does, as ``migrate`` and ``sqlmigrate`` show it.

        Returns:
            The description.
        """
        return (
            f"Run step {self.number} of the taskiq-postgresql tables "
            f"{TARGETS_SETTING} names"
        )
