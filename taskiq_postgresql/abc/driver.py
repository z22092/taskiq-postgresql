from abc import ABC, abstractmethod
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncIterator, Optional, Sequence, Union

from typing_extensions import Self

from taskiq_postgresql.abc.query import (
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    DeleteByDateQuery,
    DeleteQuery,
    DeleteReturningQuery,
    InsertOrUpdateQuery,
    InsertQuery,
    SelectQuery,
)


class QueryDriver(ABC):
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
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the backend."""
        self.connection_string = connection_string
        self.table_name = table_name
        self.columns = columns
        self.primary_key = primary_key
        self.created_at = created_at or CreatedAtColumn()
        self.index_columns = index_columns
        self.connection_kwargs = connection_kwargs

        self.create_table_query = CreateTableQuery(
            self.table_name,
            [self.primary_key, *self.columns, self.created_at],
        )
        self.create_index_query = CreateIndexQuery(self.table_name)
        self.insert_query = InsertQuery(
            self.table_name,
        )
        self.delete_query = DeleteQuery(self.table_name)
        self.delete_returning_query = DeleteReturningQuery(self.table_name)
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
    async def create_table(self) -> Any:
        """Create a table."""

    @abstractmethod
    async def create_index(self) -> Any:
        """Create an index."""

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
    async def delete_returning(
        self,
        where_column: Column,
        value: Any,
        returning: Sequence[Column],
    ) -> Optional[dict[str, Any]]:
        """Atomically delete a row and return requested columns.

        Returns a dictionary mapping column names to values for the deleted row,
        or None if no row matched (e.g., already claimed by another worker).
        """

    @abstractmethod
    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> None:
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
        """On startup."""
        async with self:
            await self.create_table()
            await self.create_index()

    @abstractmethod
    async def on_shutdown(self) -> None:
        """On shutdown."""

    @abstractmethod
    async def execute(self, query: str, *values: Any) -> Any:
        """Execute a query."""


class ListenDriver(ABC):
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

    @abstractmethod
    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Iterate over the queue."""
