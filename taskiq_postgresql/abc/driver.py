from abc import ABC, abstractmethod
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncIterator, Optional, Sequence, Union

from typing_extensions import Self

from taskiq_postgresql.abc.query import (
    AddColumnsQuery,
    AddedIndex,
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    DeleteByDateQuery,
    DeleteQuery,
    InsertOrUpdateQuery,
    InsertQuery,
    MigrationLockQuery,
    RelationsExistQuery,
    SelectQuery,
)
from taskiq_postgresql.exceptions import DatabaseConnectionError


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


class QueryDriver(_ConnectionStringHolder, ABC):
    """Base class for all PostgreSQL backends."""

    def __init__(
        self,
        connection_string: str,
        table_name: str,
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
        an older release gets them too.
        """
        self.connection_string = connection_string
        self.table_name = table_name
        self.columns = columns
        self.primary_key = primary_key
        self.created_at = created_at or CreatedAtColumn()
        self.index_columns = index_columns
        self.added_columns = added_columns
        self.added_indexes = added_indexes
        self.connection_kwargs = connection_kwargs

        self.create_table_query = CreateTableQuery(
            self.table_name,
            [self.primary_key, *self.columns, self.created_at],
        )
        self.create_index_query = CreateIndexQuery(self.table_name)
        self.relations_exist_query = RelationsExistQuery(self.table_name)
        self.migration_lock_query = MigrationLockQuery(self.table_name)
        self.add_columns_query = AddColumnsQuery(self.table_name)
        self.insert_query = InsertQuery(
            self.table_name,
        )
        self.delete_query = DeleteQuery(self.table_name)
        self.delete_by_date_query = DeleteByDateQuery(self.table_name)
        self.select_query = SelectQuery(
            self.table_name,
        )
        self.insert_or_update_query = InsertOrUpdateQuery(self.table_name)
        self.run_migrations = run_migrations

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
        """

    @abstractmethod
    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> Any:
        """Delete results by date."""

    @abstractmethod
    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Select a row from a table."""

    @abstractmethod
    async def exists(self, id: Any) -> bool:
        """Check if a row exists in a table."""

    @abstractmethod
    async def on_startup(self) -> None:
        """
        Create the table and its indexes when ``run_migrations`` is set.

        Runs on one connection, in one transaction. When the table, every
        index and the ``added_columns`` and ``added_indexes`` already exist it
        does nothing else, so a role without CREATE on the schema or ownership
        of the table can start. Otherwise it takes ``migration_lock_query``
        before the DDL: a concurrent startup waits for the commit, and its
        ``IF NOT EXISTS`` then finds the objects instead of failing on a
        duplicate catalog entry. ``added_columns`` and ``added_indexes`` come
        last, so a table created by an older release gets them too.
        """

    @abstractmethod
    async def on_shutdown(self) -> None:
        """On shutdown."""

    @abstractmethod
    async def execute(self, query: str, *values: Any) -> Any:
        """Execute a query."""


class ListenDriver(_ConnectionStringHolder, ABC):
    """Base class for all PostgreSQL broker drivers."""

    def __init__(
        self,
        connection_string: str,
        channel_name: str,
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the broker driver."""
        self.channel_name = channel_name
        self.connection_string = connection_string
        self.connection_kwargs = connection_kwargs

    @abstractmethod
    async def on_startup(self) -> None:
        """On startup."""

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
