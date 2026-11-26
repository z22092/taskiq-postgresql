"""
The schema store, which a component, the CLI and the migration tools hold.

It stands in for the query driver there: it builds it from the table's
declaration, binds, starts and closes it, and migrates or reads the catalog
through it. The startup flow itself, which the drivers run, is in
``core.stores.startup``.

``taskiq_postgresql.migrations`` is imported inside the functions that use it:
the components import this module, and importing a component must not load the
migration code or a driver.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from taskiq_postgresql.core.sql.catalog import PrimaryKeyColumn, database_read
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.utils import get_db_driver

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.drivers import AsyncpgDriver, PsqlpyDriver, PsycopgDriver
    from taskiq_postgresql.migrations import ComponentKind, TableSchema


def build_driver(
    kind: ComponentKind,
    declaration: TableDeclaration,
    driver_name: Literal["asyncpg", "psqlpy", "psycopg"],
    dsn: str,
    /,
    *,
    run_migrations: bool,
    **connect_kwargs: Any,
) -> Union[AsyncpgDriver, PsqlpyDriver, PsycopgDriver]:
    """
    Build the query driver of a component from its declaration.

    The first four arguments are positional only, so that a connection option
    of the user's cannot collide with them.

    Args:
        kind: The component that owns the table.
        declaration: What the component hands its driver.
        driver_name: The bundled driver to build.
        dsn: The connection string.
        run_migrations: Whether the driver creates what the table lacks.
        **connect_kwargs: Connection options of the driver.

    Returns:
        The driver, which knows the component that owns its table.
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
    return driver


class SchemaStore:
    """
    The table of one component, over the query driver it uses now.

    Attributes:
        kind: The component that owns the table.
    """

    def __init__(
        self,
        kind: ComponentKind,
        declaration: TableDeclaration,
        driver_of: Callable[[], QueryDriver],
    ) -> None:
        """
        Keep the declaration of the table of a component of ``kind``.

        Args:
            kind: The component that owns the table.
            declaration: What the component handed its driver.
            driver_of: Returns the driver in use, which is read at each use so
                that a driver a component is given instead is the one used.
        """
        self.kind = kind
        self._declared = declaration
        self._driver_of = driver_of
        self._declared_for = driver_of()

    @property
    def driver(self) -> QueryDriver:
        """The query driver in use."""
        return self._driver_of()

    @property
    def declaration(self) -> TableDeclaration:
        """The table as the component declared it, for the driver it built."""
        driver = self.driver
        if driver is self._declared_for:
            return self._declared
        # A driver assigned in place of it declares the table it creates.
        return TableDeclaration.of_driver(driver)

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
            await _close_opened(driver)


class TableStore:
    """
    What the stores of a component's table share: the schema store behind them.

    Attributes:
        schema: The store of the table, which starts, closes and describes it.
    """

    def __init__(self, schema: SchemaStore) -> None:
        """Hold the store of the table."""
        self.schema = schema

    @property
    def driver(self) -> QueryDriver:
        """The query driver in use."""
        return self.schema.driver

    def bind(self, dsn: str) -> None:
        """Point the driver at ``dsn`` and tell it which component owns the table."""
        self.schema.bind(dsn)

    async def start(self) -> None:
        """Check or migrate the table."""
        await self.schema.start()

    async def close(self) -> None:
        """Close the driver's pool."""
        await self.schema.close()


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
        await _close_opened(driver)


async def _close_opened(driver: QueryDriver) -> None:
    """Close the pool of ``driver`` if one is open."""
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
