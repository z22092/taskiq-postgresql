"""
The schema store, and the startup flow the bundled query drivers run.

``start_table`` checks or migrates the table; a driver lends it only its own
primitives, a ``Session`` on one connection in one transaction. With the
component's kind, one read-only catalog query (``read_catalog``), compared with
the table's description (``plan_migration``), tells what the table lacks. A
driver that migrates skips the lock and the DDL when no step is pending; one
that does not migrate refuses to start without the table or one of its
columns. Both log what only slows the component down or needs a person.

``SchemaStore`` is what a component, the CLI and the migration tools hold
instead of the query driver.

``taskiq_postgresql.migrations`` is imported inside the functions that use it:
the components import this module, and importing a component must not load the
migration code or a driver.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Optional

from taskiq_postgresql.core.sql.catalog import (
    AddColumnsQuery,
    CreateIndexQuery,
    CreateTableQuery,
    MigrationLockQuery,
    PrimaryKeyColumn,
    RelationsExistQuery,
    database_read,
)
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.exceptions import HOW_TO_MIGRATE, SchemaNotReadyError
from taskiq_postgresql.utils import get_db_driver

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.migrations import (
        ComponentKind,
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
    # Last, so a table an older release created gets them too.
    if declaration.added_columns or declaration.added_indexes:
        await connection.execute(
            AddColumnsQuery(table).make_query(
                declaration.added_columns,
                declaration.added_indexes,
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


class SchemaStore:
    """
    The table of one component, over the query driver it uses now.

    Attributes:
        kind: The component that owns the table.
        driver: The query driver in use; a component that is given another one
            hands it here, so the store follows it.
    """

    def __init__(self, kind: ComponentKind, driver: QueryDriver) -> None:
        """Hold ``driver`` for the table of a component of ``kind``."""
        self.kind = kind
        self.driver = driver

    @property
    def declaration(self) -> TableDeclaration:
        """The table as the current driver declares it, read at each use."""
        return TableDeclaration.of_driver(self.driver)

    @classmethod
    def build(
        cls,
        kind: ComponentKind,
        declaration: TableDeclaration,
        driver_name: Literal["asyncpg", "psqlpy", "psycopg"],
        dsn: str,
        /,
        *,
        run_migrations: bool,
        **connect_kwargs: Any,
    ) -> SchemaStore:
        """
        Build the driver of a component from its declaration.

        The first four arguments are positional only, so that a connection
        option of the user's cannot collide with them.

        Args:
            kind: The component that owns the table.
            declaration: What the component hands its driver.
            driver_name: The bundled driver to build.
            dsn: The connection string.
            run_migrations: Whether the driver creates what the table lacks.
            **connect_kwargs: Connection options of the driver.

        Returns:
            The store of the new driver.
        """
        driver = get_db_driver(driver_name)(
            dsn,
            declaration.table,
            columns=declaration.columns,
            primary_key=declaration.primary_key,
            created_at=declaration.created_at,
            index_columns=declaration.index_columns,
            run_migrations=run_migrations,
            added_columns=declaration.added_columns,
            added_indexes=declaration.added_indexes,
            **connect_kwargs,
        )
        driver.component_kind = kind
        return cls(kind, driver)

    @property
    def schema(self) -> TableSchema:
        """The description of the table the current driver declares."""
        from taskiq_postgresql.migrations import declared_schema  # noqa: PLC0415

        return declared_schema(self.kind, self.declaration)

    def bind(self, dsn: str) -> None:
        """Point the driver at ``dsn`` and tell it which component owns the table."""
        self.driver.connection_string = dsn
        self.driver.component_kind = self.kind

    async def start(self) -> None:
        """Check or migrate the table, by the driver's ``on_startup``."""
        await self.driver.on_startup()

    async def close(self) -> None:
        """Close the driver's pool."""
        await self.driver.on_shutdown()

    async def migrate(self, dsn: str) -> None:
        """
        Create what the table lacks on ``dsn``, whatever the flag says.

        ``run_migrations`` is set only for the call and the pool left open is
        closed, so the driver is as it was found.

        Args:
            dsn: The connection string to migrate.
        """
        driver = self.driver
        driver.connection_string = dsn
        run_migrations = driver.run_migrations
        # The startup path runs its DDL only with the flag set; migrate runs it
        # whatever the component says, then leaves the flag as it found it.
        driver.run_migrations = True
        try:
            await driver.on_startup()
        finally:
            driver.run_migrations = run_migrations
            if getattr(driver, "pool", None) is not None:
                await driver.on_shutdown()


def schema_store(component: Any) -> SchemaStore:
    """
    Return the schema store of a component.

    Args:
        component: A broker, result backend or scheduler source.

    Returns:
        Its store.
    """
    # The store is internal to the components; only the core reaches it, for
    # the CLI and the migration tools.
    return component._store.schema  # noqa: SLF001


async def read_database(
    dsn: str,
    schemas: Sequence[TableSchema],
    store: Optional[SchemaStore] = None,
) -> Optional[dict[str, Any]]:
    """
    Read the catalog of ``dsn`` and what the plan's header names, in one query.

    Args:
        dsn: The connection string to read.
        schemas: The tables whose catalog is read.
        store: The store whose driver reads; without it, a driver that only
            reads the catalog.

    Returns:
        The row, or ``None``. The pool the read opened is closed.
    """
    from taskiq_postgresql.migrations import catalog_tables  # noqa: PLC0415

    driver = store.driver if store is not None else _catalog_driver(dsn)
    driver.connection_string = dsn
    read = database_read(catalog_tables(schemas))
    try:
        return await driver.fetch_row(read.text)
    finally:
        if getattr(driver, "pool", None) is not None:
            await driver.on_shutdown()


def _catalog_driver(dsn: str) -> QueryDriver:
    """Return a query driver that only reads the catalog of ``dsn``."""
    from taskiq_postgresql.utils.libs_available import (  # noqa: PLC0415
        is_asyncpg_available,
        is_psqlpy_available,
        is_psycopg_available,
    )

    # Without the application no component says which driver: the first one
    # installed, else asyncpg, whose error then says what to install.
    installed: tuple[tuple[Literal["asyncpg", "psqlpy", "psycopg"], bool], ...] = (
        ("asyncpg", is_asyncpg_available()),
        ("psycopg", is_psycopg_available()),
        ("psqlpy", is_psqlpy_available()),
    )
    name = next((name for name, available in installed if available), "asyncpg")
    # It reads no table of its own, so its table and columns never matter.
    return get_db_driver(name)(dsn, "taskiq_catalog", [], PrimaryKeyColumn())
