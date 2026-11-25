"""
What the bundled query drivers run in ``on_startup``: one flow for all three.

``start_table`` checks or migrates the table; a driver lends it only its own
primitives, a ``Session`` on one connection in one transaction. With the
component's kind, one read-only catalog query (``read_catalog``), compared with
the table's description (``plan_migration``), tells what the table lacks. A
driver that migrates skips the lock and the DDL when no step is pending; one
that does not migrate refuses to start without the table or one of its
columns. Both log what only slows the component down or needs a person.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import TYPE_CHECKING, Any, NamedTuple, Optional

from taskiq_postgresql.exceptions import HOW_TO_MIGRATE, SchemaNotReadyError
from taskiq_postgresql.migrations import (
    PendingStep,
    SchemaProblem,
    TableSchema,
    plan_migration,
    read_catalog,
    table_schema,
)

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver

logger = logging.getLogger(__name__)


class Session(NamedTuple):
    """
    One pooled connection in one transaction, as a driver lends it to migrate.

    Attributes:
        fetch_value: Runs a query with its values bound to ``$1``, ``$2``,
            ...; returns the first column of its first row, or ``None``.
        execute: Runs a statement with its values bound; without values, a
            script of several statements.
    """

    fetch_value: Callable[..., Awaitable[Any]]
    execute: Callable[..., Awaitable[Any]]


async def start_table(
    driver: QueryDriver,
    session: Callable[[], AbstractAsyncContextManager[Session]],
) -> None:
    """
    Check the table of ``driver``, or migrate it: see ``QueryDriver.on_startup``.

    Without ``run_migrations`` it reads the catalog through ``fetch_row``, on
    the driver's own pool, and runs no DDL.

    Args:
        driver: The query driver.
        session: Opens the driver's pool, borrows a connection and starts a
            transaction on it, for a driver that migrates.

    Raises:
        SchemaNotReadyError: without ``run_migrations``, when the table or a
            column is missing, or a problem no step fixes keeps the component
            from working.
    """
    schema = described_table(driver)
    if not driver.run_migrations:
        if schema is None:
            return
        fetch_value = partial(_first_value, driver)
        plan = plan_migration([schema], await read_catalog([schema], fetch_value))
        if not plan.is_ready:
            raise SchemaNotReadyError(schema.name, plan.pending, plan.problems)
        # Ready: a step still pending lacks indexes only.
        _warn(schema.name, plan.pending, plan.problems)
        return
    problems: Sequence[SchemaProblem] = ()
    async with session() as connection:
        if schema is None:
            # Nothing to compare with: the probe of releases before the
            # comparison, which skips the lock once every relation exists.
            probe = driver.relations_exist_query
            current = bool(
                await connection.fetch_value(
                    probe.make_query(
                        driver.index_columns,
                        added_columns=driver.added_columns,
                        added_indexes=driver.added_indexes,
                    ),
                    *probe.params(
                        driver.index_columns,
                        added_columns=driver.added_columns,
                        added_indexes=driver.added_indexes,
                    ),
                ),
            )
        else:
            plan = plan_migration(
                [schema],
                await read_catalog([schema], connection.fetch_value),
            )
            current, problems = plan.is_current, plan.problems
        if not current:
            await _migrate(driver, connection)
    # The DDL ran every pending step; IF NOT EXISTS skipped what no step fixes.
    _warn(driver.table_name, (), problems)


def described_table(driver: QueryDriver) -> Optional[TableSchema]:
    """
    Describe the table of ``driver`` from its own query builders.

    Args:
        driver: The query driver of a component.

    Returns:
        The description, or ``None`` when no component set the driver's
        ``component_kind``.
    """
    if driver.component_kind is None:
        return None
    return table_schema(driver)


async def _first_value(driver: QueryDriver, query: str) -> Any:
    row = await driver.fetch_row(query)
    return None if row is None else next(iter(row.values()), None)


async def _migrate(driver: QueryDriver, connection: Session) -> None:
    # A concurrent startup waits here for the commit; its IF NOT EXISTS then
    # finds the objects instead of failing on a duplicate catalog entry.
    await connection.execute(
        driver.migration_lock_query.make_query(),
        *driver.migration_lock_query.params(),
    )
    await connection.execute(driver.create_table_query.make_query())
    if driver.index_columns is not None:
        await connection.execute(
            driver.create_index_query.make_query(driver.index_columns),
        )
    # Last, so a table an older release created gets them too.
    if driver.added_columns or driver.added_indexes:
        await connection.execute(
            driver.add_columns_query.make_query(
                driver.added_columns,
                driver.added_indexes,
            ),
        )


def _warn(
    table: str,
    pending: Sequence[PendingStep],
    problems: Sequence[SchemaProblem],
) -> None:
    for item in pending:
        logger.warning(
            "taskiq-postgresql: %s: step %s has not fully run (%s); queries on "
            "the table can be slow until it has. %s",
            table,
            item.step.number,
            item.describe(),
            HOW_TO_MIGRATE,
        )
    for problem in problems:
        logger.warning("taskiq-postgresql: %s: %s", problem.code, problem.detail)
