from abc import ABC, abstractmethod
from datetime import date, datetime
from types import TracebackType
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional, Sequence, Union

from typing_extensions import Self

from taskiq_postgresql.core.sql.catalog import (
    AddColumnsQuery,
    AddedIndex,
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    DeclaredStep,
    DeleteByDateQuery,
    DeleteQuery,
    ExistsQuery,
    InsertOrUpdateQuery,
    InsertQuery,
    MigrationLockQuery,
    RelationsExistQuery,
    SelectQuery,
    check_table_objects,
    listen_script,
)
from taskiq_postgresql.core.sql.identifier import Identifier, as_identifier
from taskiq_postgresql.exceptions import DatabaseConnectionError

if TYPE_CHECKING:
    from taskiq_postgresql.migrations import ComponentKind


class _ConnectionStringHolder:
    """Holds the connection string and never hands out an empty one."""

    _connection_string: str = ""

    @property
    def connection_string(self) -> str:
        """
        Return the connection string.

        Raises:
            DatabaseConnectionError: if there is none yet. An empty string
                would make the driver connect to the local defaults instead.
        """
        if not self._connection_string:
            raise DatabaseConnectionError(
                "No connection string yet: a callable DSN is resolved by "
                "startup(), so call startup() before using the component.",
            )
        return self._connection_string

    @connection_string.setter
    def connection_string(self, value: str) -> None:
        self._connection_string = value


def _legacy_steps(
    added_columns: Sequence[Column],
    added_indexes: Sequence[AddedIndex],
) -> tuple[DeclaredStep, ...]:
    """
    Return the steps of a driver built the old way, with no list of steps.

    It exists for a driver that passes ``added_columns`` and ``added_indexes``
    to the constructor, as every driver did before a table's steps were a list:
    what it adds is one step. The bundled components replace the result with
    the steps they declare.

    Args:
        added_columns: The columns added after ``CREATE TABLE``.
        added_indexes: The indexes added with them.

    Returns:
        One step, or none when nothing is added.
    """
    if not added_columns and not added_indexes:
        return ()
    return (DeclaredStep(added_columns, added_indexes),)


class QueryDriver(_ConnectionStringHolder, ABC):
    """Base class for all PostgreSQL backends."""

    #: Seconds a driver should let ``fetch_row()`` and ``on_shutdown()`` wait
    #: on a connection that does not answer before giving the connection up;
    #: ``None`` waits for as long as it takes. The broker sets it on its own
    #: driver. The asyncpg and psycopg drivers bound both by it; psqlpy only
    #: its close, since it can neither cancel a query on the server nor drop
    #: one pooled connection, and a claim given up would leave its DELETE to
    #: run later with nobody to take the row.
    query_timeout: Optional[float] = None
    #: The component that owns the table, which the bundled components set at
    #: construction and again in ``startup()``: with it, ``on_startup()``
    #: describes the table from this driver's declaration (its table, columns
    #: and indexes, as the component handed them over) and compares the
    #: database with that. ``None``, a driver no component owns, compares
    #: nothing: it probes its relations when it migrates, and checks nothing
    #: when it does not.
    component_kind: Optional["ComponentKind"] = None
    #: The migration steps after ``CREATE TABLE``, in the order they run: the
    #: one stored form of what the table gets after its ``CREATE``s. The
    #: bundled components set it on their driver after building it; a driver
    #: built the old way, with ``added_columns`` and ``added_indexes``, has them
    #: as its one step. ``added_columns`` and ``added_indexes`` are derived from
    #: it.
    steps: Sequence[DeclaredStep] = ()

    def __init__(
        self,
        connection_string: str,
        table_name: Union[str, Identifier],
        columns: Sequence[Column],
        primary_key: Column,
        created_at: Optional[Column] = None,
        index_columns: Optional[Sequence[Column]] = None,
        run_migrations: bool = False,
        added_columns: Sequence[Column] = (),
        added_indexes: Sequence[AddedIndex] = (),
        **connection_kwargs: Any,
    ) -> None:
        """
        Initialize the backend.

        ``added_columns`` and ``added_indexes`` are not part of
        ``CREATE TABLE``: ``on_startup`` adds them last, so a table created by
        an older release gets them too. They become one step in ``steps``, from
        which ``added_columns`` and ``added_indexes`` are read back.
        """
        self.table: Identifier = as_identifier(table_name, what="table name")
        self.connection_string = connection_string
        # The name as configured, which the SQL writes.
        self.table_name = self.table.configured
        self.columns = columns
        self.primary_key = primary_key
        self.created_at = created_at or CreatedAtColumn()
        self.index_columns = index_columns
        self.steps = _legacy_steps(added_columns, added_indexes)
        self.connection_kwargs = connection_kwargs
        check_table_objects(
            self.table,
            columns=[self.primary_key, *columns, self.created_at],
            index_columns=index_columns,
            added_indexes=added_indexes,
        )

        self.create_table_query = CreateTableQuery(
            self.table,
            [self.primary_key, *self.columns, self.created_at],
        )
        self.create_index_query = CreateIndexQuery(self.table)
        # The bundled drivers build the probe and the lock from the table
        # declaration; these attributes stay for third-party drivers that use
        # them, such as the probe of a driver without component_kind.
        self.relations_exist_query = RelationsExistQuery(self.table)
        self.migration_lock_query = MigrationLockQuery(self.table)
        self.add_columns_query = AddColumnsQuery(self.table)
        self.insert_query = InsertQuery(self.table)
        self.delete_query = DeleteQuery(self.table)
        self.delete_by_date_query = DeleteByDateQuery(self.table)
        self.select_query = SelectQuery(self.table)
        self.insert_or_update_query = InsertOrUpdateQuery(self.table)
        self.exists_query = ExistsQuery(self.table)
        self.run_migrations = run_migrations

    @property
    def added_columns(self) -> tuple[Column, ...]:
        """The columns every step adds, in the order the steps run."""
        return tuple(column for step in self.steps for column in step.added_columns)

    @property
    def added_indexes(self) -> tuple[AddedIndex, ...]:
        """The indexes every step adds, in the order the steps run."""
        return tuple(index for step in self.steps for index in step.added_indexes)

    @abstractmethod
    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        return self

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit the context manager."""

    @abstractmethod
    async def insert(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        returning: Optional[Sequence[Column]],
    ) -> Any:
        """Insert a row into a table."""

    @abstractmethod
    async def insert_or_update(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        on_conflict_columns: Sequence[Column],
        on_conflict_update_columns: Sequence[Column],
        returning: Optional[Sequence[Column]],
    ) -> Any:
        """Insert or update a row into a table."""

    @abstractmethod
    async def delete(self, column: Column, value: Any) -> Any:
        """Delete a row from a table."""

    @abstractmethod
    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        """
        Run ``query`` with ``values`` bound to ``$1``, ``$2``, ...

        Returns:
            The first row it returns, by column name, or None if it returns
            none.

        Raises:
            asyncio.TimeoutError: if ``query_timeout`` is set and the query
                got no answer within it, which gives the query up. psqlpy's
                driver never raises it, see ``query_timeout``.
        """

    @abstractmethod
    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> Any:
        """
        Delete results by date.

        Both bounds are inclusive. Prefer time-zone-aware datetimes: a ``date``
        counts as midnight at the start of that day, in a time zone that depends
        on the driver, so ``to_date`` as a ``date`` keeps that day's results.

        Args:
            from_date: delete the results created at or after it.
            to_date: delete only the results created at or before it; left out,
                the results are deleted without an upper bound.
        """

    @abstractmethod
    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
        not_null: Sequence[Column] = (),
    ) -> list[dict[str, Any]]:
        """
        Select a row from a table.

        Args:
            columns: The columns to return.
            where_columns: The columns compared with ``where_values``.
            where_values: The values of ``where_columns``, in the same order.
            not_null: Columns that must not be ``NULL``: a row where one is
                is not returned.
        """

    @abstractmethod
    async def exists(self, id: Any, not_null: Sequence[Column] = ()) -> bool:
        """
        Check if a row exists in a table.

        Args:
            id: The value of the primary key.
            not_null: Columns that must not be ``NULL`` for the row to count.
        """

    @abstractmethod
    async def on_startup(self) -> None:
        """
        Check the table, or create and complete it when ``run_migrations`` is set.

        A component calls it through its store, at startup. Given
        ``component_kind``, it describes the table from the driver's
        declaration and starts with one read-only catalog query
        (``catalog_query``), compared with that description
        (``plan_migration``). Without it, it cannot describe the table: it
        then probes the table and its indexes when ``run_migrations`` is set,
        as releases before that comparison did, and checks nothing otherwise.
        The bundled drivers build the probe and the lock from the table
        declaration; ``relations_exist_query`` and ``migration_lock_query``
        stay for third-party drivers.

        With ``run_migrations`` set it runs on one connection, in one
        transaction. When no migration step is pending, or the probe finds
        every relation, it does nothing else, so a role without CREATE on the
        schema or ownership of the table can start. Otherwise it takes
        the migration lock before the DDL: a concurrent startup waits for
        the commit, and its ``IF NOT EXISTS`` then finds the objects instead
        of failing on a duplicate catalog entry. ``added_columns`` and
        ``added_indexes`` come last, so a table created by an older release
        gets them too. Then it logs a warning for each problem no step fixes,
        which ``IF NOT EXISTS`` skipped.

        Without ``run_migrations`` it runs no DDL. It logs a warning for a
        missing index and for a problem no step fixes that the component works
        with.

        Raises:
            SchemaNotReadyError: without ``run_migrations``, when the table or
                a column of it is missing, or when a problem no step fixes
                keeps the component from working.
        """

    @abstractmethod
    async def on_shutdown(self) -> None:
        """
        Close the pool.

        A driver should stop waiting on connections that do not answer once
        ``query_timeout`` runs out and abort them, as the bundled ones do. The
        broker cancels the close after twice ``query_timeout``, which leaves a
        driver that bounds its own close the time to abort.
        """

    @abstractmethod
    async def execute(self, query: str, *values: Any) -> Any:
        """Execute a query."""


class ListenDriver(_ConnectionStringHolder, ABC):
    """
    Base class for all PostgreSQL broker drivers.

    The broker reopens the connection when the iterator ends or raises, by
    calling ``on_shutdown()`` and then ``on_startup()`` again, so both must
    work any number of times and ``on_shutdown()`` must not fail on a
    connection that is already gone. To reopen, the broker gives
    ``on_startup()`` ``query_timeout`` seconds; it gives every
    ``on_shutdown()`` twice that, see ``QueryDriver.on_shutdown``.
    """

    #: Seconds of silence on the LISTEN connection after which a driver should
    #: check it with a round trip, and end its iterator with an error if the
    #: check fails, so that the broker reopens the connection. ``None`` never
    #: checks. The bundled drivers send the same LISTEN again.
    health_check_interval: Optional[float] = None
    #: Seconds a driver should wait for that check, and for a close, before it
    #: gives the connection up, as the bundled drivers do. ``None`` waits for
    #: as long as it takes.
    query_timeout: Optional[float] = None

    def __init__(
        self,
        connection_string: str,
        channel_name: Union[str, Identifier],
        **connection_kwargs: Any,
    ) -> None:
        """
        Initialize the broker driver.

        Args:
            connection_string: The DSN.
            channel_name: The channel, as configured or already checked. A
                string is checked here, without a warning (the broker warns
                where the user wrote it).
            **connection_kwargs: Passed on to the connection.

        Raises:
            IdentifierError: ``channel_name`` is a string the rule refuses.
        """
        self.channel: Identifier = as_identifier(channel_name, what="channel name")
        # The folded name, which is the one an unquoted LISTEN subscribes to.
        self.channel_name = self.channel.name
        self.listen_text = listen_script(self.channel).text
        self.connection_string = connection_string
        self.connection_kwargs = connection_kwargs

    @abstractmethod
    async def on_startup(self) -> None:
        """
        Open the connection and LISTEN on the channel.

        Return only once LISTEN is in effect: the iterator must yield every
        NOTIFY sent after this returns, even one sent before anything iterates.
        """

    @abstractmethod
    async def on_shutdown(self) -> None:
        """On shutdown."""

    # A plain def: the drivers implement it as an async generator, which
    # returns the iterator directly instead of a coroutine.
    @abstractmethod
    def __aiter__(self) -> AsyncIterator[str]:
        """
        Yield the payload of each NOTIFY on the channel, unparsed.

        The broker reads the message id and skips, with a warning, a payload
        that is not one. A driver that parsed it would raise in its NOTIFY
        callback instead, which loses the NOTIFY and can stop LISTEN.
        """
