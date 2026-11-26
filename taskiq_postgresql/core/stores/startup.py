"""
The startup flow the bundled query drivers run.

``start_table`` checks or migrates the table; a driver lends it only its own
primitives, a ``Session`` on one connection in one transaction. With the
component's kind, one read-only catalog query (``read_catalog``), compared with
the table's description (``plan_migration``), tells what the table lacks. A
driver that migrates skips the lock and the DDL when no step is pending; one
that does not migrate refuses to start without the table or one of its
columns. Both log what only slows the component down or needs a person.

``taskiq_postgresql.migrations`` is imported inside the functions that use it:
the components import the stores, and importing a component must not load the
migration code or a driver; the drivers import this module, so it names none.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import TYPE_CHECKING, Any, NamedTuple, Optional

from taskiq_postgresql.core.sql.catalog import (
    CreateIndexQuery,
    CreateTableQuery,
    MigrationLockQuery,
    RelationsExistQuery,
    declared_step_script,
)
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.exceptions import HOW_TO_MIGRATE, SchemaNotReadyError

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.migrations import (
        PendingStep,
        SchemaProblem,
        TableSchema,
    )

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
    declaration: TableDeclaration,
    schema: Optional[TableSchema],
    *,
    run_migrations: bool,
    fetch_value: Callable[[str], Awaitable[Any]],
    session: Callable[[], AbstractAsyncContextManager[Session]],
) -> None:
    """
    Check the table of ``declaration``, or migrate it.

    Without ``run_migrations`` it reads the catalog through ``fetch_value``,
    on the driver's own pool, and runs no DDL.

    Args:
        declaration: The table, as the component declares it.
        schema: Its description, or ``None`` when no component owns the
            driver, which then only probes its relations.
        run_migrations: Whether to create what the table lacks.
        fetch_value: Runs a catalog query on the driver's own pool.
        session: Opens the driver's pool, borrows a connection and starts a
            transaction on it, for a driver that migrates.

    Raises:
        SchemaNotReadyError: without ``run_migrations``, when the table or a
            column is missing, or a problem no step fixes keeps the component
            from working.
    """
    from taskiq_postgresql.migrations import (  # noqa: PLC0415
        plan_migration,
        read_catalog,
    )

    if not run_migrations:
        if schema is None:
            return
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
            probe = RelationsExistQuery(declaration.table)
            current = bool(
                await connection.fetch_value(
                    probe.make_query(
                        declaration.index_columns,
                        added_columns=declaration.added_columns,
                        added_indexes=declaration.added_indexes,
                    ),
                    *probe.params(
                        declaration.index_columns,
                        added_columns=declaration.added_columns,
                        added_indexes=declaration.added_indexes,
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
            await _migrate(declaration, connection)
    # The DDL ran every pending step; IF NOT EXISTS skipped what no step fixes.
    _warn(declaration.table.configured, (), problems)


async def start_driver(
    driver: QueryDriver,
    session: Callable[[], AbstractAsyncContextManager[Session]],
) -> None:
    """
    Run the startup flow for ``driver``: see ``QueryDriver.on_startup``.

    This is what the bundled drivers run in ``on_startup``.

    Args:
        driver: The query driver.
        session: The driver's own ``Session`` opener.
    """
    await start_table(
        TableDeclaration.of_driver(driver),
        described_table(driver),
        run_migrations=driver.run_migrations,
        fetch_value=partial(_first_value, driver),
        session=session,
    )


def described_table(driver: QueryDriver) -> Optional[TableSchema]:
    """
    Describe the table of ``driver`` from its declaration.

    Args:
        driver: The query driver of a component.

    Returns:
        The description, or ``None`` when no component set the driver's
        ``component_kind``.
    """
    from taskiq_postgresql.migrations import table_schema  # noqa: PLC0415

    if driver.component_kind is None:
        return None
    return table_schema(driver)


async def _first_value(driver: QueryDriver, query: str) -> Any:
    row = await driver.fetch_row(query)
    return None if row is None else next(iter(row.values()), None)


async def _migrate(declaration: TableDeclaration, connection: Session) -> None:
    table = declaration.table
    lock = MigrationLockQuery(table)
    # A concurrent startup waits here for the commit; its IF NOT EXISTS then
    # finds the objects instead of failing on a duplicate catalog entry.
    await connection.execute(lock.make_query(), *lock.params())
    await connection.execute(
        CreateTableQuery(table, declaration.table_columns).make_query(),
    )
    if declaration.index_columns is not None:
        await connection.execute(
            CreateIndexQuery(table).make_query(declaration.index_columns),
        )
    # Last, one script per step in the order they are declared, so a table an
    # older release created gets them too. The statements are the ones the
    # migration description holds for the step (declared_step_statements).
    for step in declaration.steps:
        script = declared_step_script(table, step)
        # A step that declares nothing has no statement to run.
        if script:
            await connection.execute(script)


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
