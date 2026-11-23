from asyncio import Queue as AsyncQueue
from contextlib import asynccontextmanager
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncGenerator, Optional, Sequence, Union

from asyncpg import Connection, Pool, connect, create_pool
from typing_extensions import Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import AddedIndex, Column
from taskiq_postgresql.drivers._params import encode_params
from taskiq_postgresql.exceptions import DatabaseConnectionError


class AsyncpgDriver(QueryDriver):
    """Asyncpg backend."""

    pool: Optional[Pool] = None

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
        """Initialize the backend."""
        super().__init__(
            connection_string,
            table_name,
            columns,
            primary_key,
            created_at,
            index_columns,
            run_migrations,
            added_columns,
            added_indexes,
            **connection_kwargs,
        )

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[Connection, None]:
        """
        Borrow a connection from the pool for the duration of the block.

        The pool only exists after ``async with driver``, so the query methods
        enter that first; refusing here keeps a stray call from opening a pool
        nobody closes.

        Yields:
            A pooled asyncpg connection, released when the block ends.

        Raises:
            DatabaseConnectionError: if the pool is not open.
        """
        if self.pool is None:
            raise DatabaseConnectionError(
                "The pool is not open: enter `async with driver` first.",
            )
        async with self.pool.acquire() as connection:
            yield connection

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        try:
            if self.pool is None:
                self.pool = await create_pool(
                    self.connection_string,
                    **self.connection_kwargs,
                )
            return self
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit the context manager."""

    async def insert(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        returning: Optional[Sequence[Column]] = None,
    ) -> Any:
        """Insert a row into a table."""
        async with self, self.connection() as connection:
            return await connection.fetchval(
                self.insert_query.make_query(columns, returning),
                *encode_params(columns, values),
            )

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
            return (
                await connection.fetchval(
                    self.insert_or_update_query.make_query(
                        columns,
                        returning,
                        on_conflict_columns,
                        on_conflict_update_columns=on_conflict_update_columns,
                    ),
                    *encode_params(columns, values),
                ),
            )

    async def delete(self, column: Column, value: Any) -> str:
        """Delete a row from a table."""
        async with self, self.connection() as connection:
            return await connection.execute(
                self.delete_query.make_query(column),
                value,
            )

    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        """Return the first row of ``query``, see ``QueryDriver.fetch_row``."""
        async with self, self.connection() as connection:
            row = await connection.fetchrow(query, *values)
            return None if row is None else dict(row)

    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Select a row from a table."""
        async with self, self.connection() as connection:
            rows = await connection.fetch(
                self.select_query.make_query(columns, where_columns),
                *encode_params(where_columns, where_values or ()),
            )

            return [
                {column.name: row[column.name] for column in columns} for row in rows
            ]

    async def exists(self, id: Any) -> bool:
        """Check if a row exists in a table."""
        async with self, self.connection() as connection:
            return await connection.fetchval(
                self.select_query.make_query([Column("1", "")], [self.primary_key]),
                id,
            )

    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> str:
        """Delete a row from a table by date."""
        async with self, self.connection() as connection:
            return await connection.execute(
                self.delete_by_date_query.make_query(self.created_at),
                from_date,
                to_date,
            )

    async def on_startup(self) -> None:
        """Create the table and its indexes, see ``QueryDriver.on_startup``."""
        if self.run_migrations:
            async with (
                self,
                self.connection() as connection,
                connection.transaction(),
            ):
                if await connection.fetchval(
                    self.relations_exist_query.make_query(
                        self.index_columns,
                        added_columns=self.added_columns,
                        added_indexes=self.added_indexes,
                    ),
                    *self.relations_exist_query.params(
                        self.index_columns,
                        added_columns=self.added_columns,
                        added_indexes=self.added_indexes,
                    ),
                ):
                    return
                await connection.execute(
                    self.migration_lock_query.make_query(),
                    *self.migration_lock_query.params(),
                )
                await connection.execute(self.create_table_query.make_query())
                if self.index_columns is not None:
                    await connection.execute(
                        self.create_index_query.make_query(self.index_columns),
                    )
                if self.added_columns or self.added_indexes:
                    await connection.execute(
                        self.add_columns_query.make_query(
                            self.added_columns,
                            self.added_indexes,
                        ),
                    )

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
            return await connection.fetch(query, *values)


class AsyncpgListenDriver(ListenDriver):
    """Asyncpg listen driver."""

    def __init__(
        self,
        connection_string: str,
        channel_name: str,
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the listen driver."""
        super().__init__(connection_string, channel_name, **connection_kwargs)
        # None: a connection closed, see _termination_handler.
        self._queue: AsyncQueue[Optional[str]] = AsyncQueue()

    async def on_startup(self) -> None:
        """On startup."""
        try:
            self.connection = await connect(
                self.connection_string,
                **self.connection_kwargs,
            )
            await self.connection.add_listener(
                self.channel_name,
                self._notification_handler,
            )
            self.connection.add_termination_listener(self._termination_handler)
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def on_shutdown(self) -> None:
        """On shutdown."""
        await self.connection.remove_listener(
            self.channel_name,
            self._notification_handler,
        )
        await self.connection.close()

    def _notification_handler(
        self,
        con_ref: object,
        pid: int,
        channel: str,
        payload: str,
        /,
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
            # Unparsed, see ListenDriver.__aiter__.
            self._queue.put_nowait(payload)

    def _termination_handler(self, connection: object, /) -> None:
        """Wake ``__aiter__`` up when the connection closes, so that it ends."""
        # Otherwise a connection the server ended leaves __aiter__ waiting on
        # the queue for good, and nobody learns that LISTEN stopped.
        self._queue.put_nowait(None)

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        """Iterate over the queue until the connection closes."""
        while not self.connection.is_closed():
            payload = await self._queue.get()
            # A None left by an earlier connection is stale: keep reading.
            if payload is not None:
                yield payload
