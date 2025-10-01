import logging
from asyncio import Queue as AsyncQueue
from contextlib import asynccontextmanager
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncIterator, Optional, Sequence, Union
from uuid import UUID

from psycopg import AsyncConnection, AsyncCursor, AsyncRawCursor, Notify
from psycopg.rows import DictRow, TupleRow, dict_row
from psycopg_pool import AsyncConnectionPool
from taskiq.compat import IS_PYDANTIC2
from typing_extensions import Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.exceptions import DatabaseConnectionError

if IS_PYDANTIC2:
    from pydantic_core import to_json

    def dumps(value: dict) -> str:
        return to_json(value).decode()
else:
    from json import dumps as to_json

    def dumps(value: dict) -> str:
        return to_json(value)


logger = logging.getLogger(__name__)


class PsycopgDriver(QueryDriver):
    """Asyncpg backend."""

    pool: AsyncConnectionPool = None

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

    def __parser_params(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
    ) -> list[Any]:
        """Parser query.

        Args:
            columns (Sequence[Column]): Columns to parse.
            values (Sequence[Any]): Values to parse.

        Returns:
            list[Any]: Parsed values.
        """
        if not values:
            return []

        if not columns:
            return values

        new_values = [*values]

        for index, (column, value) in enumerate(zip(columns, values)):
            if column.type.upper() == "JSONB" and isinstance(value, dict):
                new_values[index] = dumps(value)

            if column.type.upper() == "UUID" and isinstance(value, UUID):
                new_values[index] = value.hex

        return new_values

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection[TupleRow]]:
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

    async def create_table(self) -> str:
        """Create a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                self.create_table_query.make_query(),
            )

            return cursor.statusmessage

    async def create_index(self) -> None:
        """Create an index."""
        if self.index_columns is not None:
            async with self, self.connection() as connection:
                await connection.execute(
                    self.create_index_query.make_query(self.index_columns),
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
                self.insert_query.make_query(columns, returning),
                params=self.__parser_params(columns, values),
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
                self.insert_or_update_query.make_query(
                    columns,
                    returning,
                    on_conflict_columns,
                    on_conflict_update_columns=on_conflict_update_columns,
                ),
                params=self.__parser_params(columns, values),
            )

            if cursor.rownumber is not None:
                value = await cursor.fetchone()
                if value is not None:
                    return next(iter(value))
            return None

    async def delete(self, column: Column, value: Any) -> str:
        """Delete a row from a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                self.delete_query.make_query(column),
                params=self.__parser_params([column], [value]),
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
                self.delete_returning_query.make_query(where_column, returning),
                params=self.__parser_params([where_column], [value]),
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
                self.select_query.make_query(columns, where_columns),
                params=self.__parser_params(where_columns, where_values),
            )

            if cursor.rownumber is not None:
                return await cursor.fetchall()
            return []

    async def exists(self, id: Any) -> bool:
        """Check if a row exists in a table."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                self.select_query.make_query([Column("1", "")], [self.primary_key]),
                params=self.__parser_params([self.primary_key], [id]),
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
    ) -> str:
        """Delete a row from a table by date."""
        async with self, self.connection() as connection:
            cursor: AsyncCursor[TupleRow] = await connection.execute(
                self.delete_by_date_query.make_query(self.created_at),
                params=self.__parser_params(
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
        await self.pool.close()
        self.pool = None

    async def execute(self, query: str, *values: Any) -> list[Any]:
        """Execute a query."""
        async with self, self.connection() as connection:
            cursor: AsyncRawCursor[TupleRow] = await connection.execute(
                query,
                params=values,
            )

            if cursor.rownumber is not None:
                return await cursor.fetchall()
            return []


class PsycopgListenDriver(ListenDriver):
    """Asyncpg listen driver."""

    def __init__(
        self,
        connection_string: str,
        channel_name: str,
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the listen driver."""
        super().__init__(connection_string, channel_name, **connection_kwargs)
        self._queue: AsyncQueue[int] = AsyncQueue()

    async def on_startup(self) -> None:
        """On startup."""
        self.connection: AsyncConnection[DictRow] = await AsyncConnection.connect(
            self.connection_string,
            **self.connection_kwargs,
            autocommit=True,
            cursor_factory=AsyncRawCursor,
        )
        async with self.connection.cursor() as cursor:
            await cursor.execute(f"LISTEN {self.channel_name}")

        self.connection.add_notify_handler(self._notification_handler)

    async def on_shutdown(self) -> None:
        """On shutdown."""
        await self.connection.close()

    def _notification_handler(
        self,
        notify: Notify,
    ) -> None:
        """
        Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **con_ref**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
        """
        if self._queue is not None:
            self._queue.put_nowait(int(notify.payload))

    async def __aiter__(self) -> AsyncIterator[Any]:
        """Iterate over the queue."""
        async for notify in self.connection.notifies():
            yield notify.payload

        while not self.connection.closed:
            message_id = await self._queue.get()
            yield message_id
