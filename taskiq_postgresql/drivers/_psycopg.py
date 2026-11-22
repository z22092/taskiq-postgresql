import logging
from contextlib import asynccontextmanager
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncGenerator, AsyncIterator, Optional, Sequence, Union, cast

from psycopg import AsyncConnection, AsyncCursor, AsyncRawCursor
from psycopg.rows import DictRow, TupleRow, dict_row
from psycopg_pool import AsyncConnectionPool
from typing_extensions import LiteralString, Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.drivers._params import encode_params
from taskiq_postgresql.exceptions import DatabaseConnectionError


def sql(query: str) -> LiteralString:
    """
    Mark a query built by ``taskiq_postgresql.abc.query`` as trusted SQL.

    psycopg's annotations accept only literal strings, to keep data out of
    the query text. These queries are built from the table, column and channel
    names given to the component at construction; values always travel as
    bind parameters.
    """
    return cast("LiteralString", query)


logger = logging.getLogger(__name__)


class PsycopgDriver(QueryDriver):
    """Asyncpg backend."""

    pool: Optional[AsyncConnectionPool] = None

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
        super().__init__(
            connection_string,
            table_name,
            columns,
            primary_key,
            created_at,
            index_columns,
            run_migrations,
            **connection_kwargs,
        )

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[AsyncConnection[TupleRow], None]:
        if self.pool is None:
            raise DatabaseConnectionError(
                "The pool is not open: enter `async with driver` first.",
            )
        try:
            await self.pool.open()
            async with self.pool.connection() as connection:
                yield connection

        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        if self.pool is None:
            self.pool = AsyncConnectionPool(
                self.connection_string,
                kwargs={
                    "cursor_factory": AsyncRawCursor,
                },
                open=False,
                **self.connection_kwargs,
            )
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit the context manager."""

    async def create_table(self) -> Optional[str]:
        """Create a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(self.create_table_query.make_query()),
            )

            return cursor.statusmessage

    async def create_index(self) -> None:
        """Create an index."""
        if self.index_columns is not None:
            async with self, self.connection() as connection:
                await connection.execute(
                    sql(self.create_index_query.make_query(self.index_columns)),
                )

    async def insert(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        returning: Optional[Sequence[Column]] = None,
    ) -> Any:
        """Insert a row into a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(self.insert_query.make_query(columns, returning)),
                params=encode_params(columns, values),
            )

            if cursor.rownumber is not None:
                value = await cursor.fetchone()
                if value is not None:
                    return next(iter(value))
            return None

    async def insert_or_update(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        on_conflict_columns: Sequence[Column],
        on_conflict_update_columns: Sequence[Column],
        returning: Optional[Sequence[Column]] = None,
    ) -> Any:
        """Insert or update a row into a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(
                    self.insert_or_update_query.make_query(
                        columns,
                        returning,
                        on_conflict_columns,
                        on_conflict_update_columns=on_conflict_update_columns,
                    ),
                ),
                params=encode_params(columns, values),
            )

            if cursor.rownumber is not None:
                value = await cursor.fetchone()
                if value is not None:
                    return next(iter(value))
            return None

    async def delete(self, column: Column, value: Any) -> Any:
        """Delete a row from a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(self.delete_query.make_query(column)),
                params=encode_params([column], [value]),
            )

            if cursor.rownumber is not None:
                data = await cursor.fetchone()
                if data is not None:
                    return next(iter(value))
            return None

    async def delete_returning(
        self,
        where_column: Column,
        value: Any,
        returning: Sequence[Column],
    ) -> Optional[dict[str, Any]]:
        """Atomically delete a row and return requested columns."""
        async with self, self.connection() as connection:
            cursor = connection.cursor(row_factory=dict_row)
            cursor = await cursor.execute(
                sql(self.delete_returning_query.make_query(where_column, returning)),
                params=encode_params([where_column], [value]),
            )
            if cursor.rownumber is not None:
                row = await cursor.fetchone()
                if row is not None:
                    # row is already a dict thanks to dict_row
                    return {column.name: row[column.name] for column in returning}
            return None

    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Select a row from a table."""
        async with self, self.connection() as connection:
            cursor = connection.cursor(row_factory=dict_row)
            cursor: AsyncCursor[DictRow] = await cursor.execute(
                sql(self.select_query.make_query(columns, where_columns)),
                params=encode_params(where_columns, where_values),
            )

            if cursor.rownumber is not None:
                return await cursor.fetchall()
            return []

    async def exists(self, id: Any) -> bool:
        """Check if a row exists in a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(
                    self.select_query.make_query(
                        [Column("1", "")],
                        [self.primary_key],
                    ),
                ),
                params=encode_params([self.primary_key], [id]),
            )

            if cursor.rownumber is not None:
                value = await cursor.fetchone()
                if value is not None:
                    return bool(next(iter(value)))
            return False

    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> Any:
        """Delete a row from a table by date."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(self.delete_by_date_query.make_query(self.created_at)),
                params=encode_params(
                    [self.created_at, self.created_at],
                    [from_date, to_date],
                ),
            )

            if cursor.rownumber is not None:
                value = await cursor.fetchone()
                if value is not None:
                    return next(iter(value))
            return None

    async def on_startup(self) -> None:
        """On startup."""
        if self.run_migrations:
            async with self, self.connection() as connection:  # noqa: SIM117
                async with connection.transaction():
                    await self.create_table()
                    await self.create_index()

    async def on_shutdown(self) -> None:
        """On shutdown."""
        # The pool opens on the first query, so a component that never ran
        # one has nothing to close.
        if self.pool is None:
            return
        await self.pool.close()
        self.pool = None

    async def execute(self, query: str, *values: Any) -> list[Any]:
        """Execute a query."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                sql(query),
                params=values,
            )

            if cursor.rownumber is not None:
                return await cursor.fetchall()
            return []


class PsycopgListenDriver(ListenDriver):
    """Asyncpg listen driver."""

    async def on_startup(self) -> None:
        """On startup."""
        try:
            self.connection: AsyncConnection[TupleRow] = await AsyncConnection.connect(
                self.connection_string,
                **self.connection_kwargs,
                autocommit=True,
                cursor_factory=AsyncRawCursor,
            )
            async with self.connection.cursor() as cursor:
                await cursor.execute(sql(f"LISTEN {self.channel_name}"))
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def on_shutdown(self) -> None:
        """On shutdown."""
        await self.connection.close()

    async def __aiter__(self) -> AsyncIterator[int]:
        """
        Iterate over the ids of the notified messages.

        A payload that is not an integer is logged and skipped.
        """
        # notifies() alone: psycopg runs a registered notify handler on the
        # same notifications, and a handler that raised would stop this loop.
        async for notify in self.connection.notifies():
            try:
                message_id = int(notify.payload)
            except ValueError:
                logger.warning(
                    "Invalid NOTIFY payload %r on channel %s",
                    notify.payload,
                    self.channel_name,
                )
                continue
            yield message_id
