import json
from asyncio import Queue as AsyncQueue
from contextlib import asynccontextmanager
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncGenerator, AsyncIterator, Optional, Sequence, Union
from uuid import UUID

from psqlpy import Connection, ConnectionPool
from psqlpy.extra_types import JSONB
from typing_extensions import Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.drivers._notify import parse_message_id
from taskiq_postgresql.exceptions import DatabaseConnectionError


def encode_psqlpy_params(
    columns: Sequence[Column],
    values: Optional[Sequence[Any]],
) -> list[Any]:
    """
    Shape bind parameters for psqlpy.

    A value bound to a ``JSONB`` column is wrapped in psqlpy's ``JSONB`` (JSON
    text is parsed first) and a value bound to a ``UUID`` column becomes a
    ``uuid.UUID``; every other value is passed through unchanged. psqlpy does
    not convert these on its own, so the shaping has to happen before the call.

    Args:
        columns: the columns the values are bound to, in order.
        values: the values to bind, or ``None``.

    Returns:
        The values to send with the query; an empty list when ``values`` is
        ``None``.

    Raises:
        ValueError: if ``columns`` and ``values`` differ in length.
    """
    if values is None:
        return []

    new_values = [*values]

    for index, (column, value) in enumerate(zip(columns, values, strict=True)):
        if column.type.upper() == "JSONB":
            if isinstance(value, str):
                value = json.loads(value)  # noqa: PLW2901
            new_values[index] = JSONB(value)

        if column.type.upper() == "UUID":
            new_values[index] = UUID(value)

    return new_values


class PsqlpyDriver(QueryDriver):
    """Asyncpg backend."""

    pool: Optional[ConnectionPool] = None

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
    async def connection(self) -> AsyncGenerator[Connection, None]:
        """
        Borrow a connection from the pool for the duration of the block.

        Every error raised inside the block is re-raised as
        ``DatabaseConnectionError``, so callers handle one exception type
        whatever the driver. The pool only exists after ``async with driver``,
        so refusing here keeps a stray call from opening a pool nobody closes.

        Yields:
            A pooled psqlpy connection, released when the block ends.

        Raises:
            DatabaseConnectionError: if the pool is not open, or for any error
                raised while the connection is held.
        """
        if self.pool is None:
            raise DatabaseConnectionError(
                "The pool is not open: enter `async with driver` first.",
            )
        try:
            async with self.pool.acquire() as connection:
                yield connection
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        if self.pool is None:
            self.pool = ConnectionPool(
                self.connection_string,
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

    async def insert(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        returning: Optional[Sequence[Column]] = None,
    ) -> Any:
        """Insert a row into a table."""
        async with self, self.connection() as connection:
            if returning:
                return await connection.fetch_val(
                    self.insert_query.make_query(columns, returning),
                    encode_psqlpy_params(columns, values),
                )

            await connection.execute(
                self.insert_query.make_query(columns, returning),
                encode_psqlpy_params(columns, values),
            )

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
            if returning:
                return await connection.fetch_val(
                    self.insert_or_update_query.make_query(
                        columns,
                        returning,
                        on_conflict_columns,
                        on_conflict_update_columns=on_conflict_update_columns,
                    ),
                    encode_psqlpy_params(columns, values),
                )

            await connection.execute(
                self.insert_or_update_query.make_query(
                    columns,
                    returning,
                    on_conflict_columns,
                    on_conflict_update_columns=on_conflict_update_columns,
                ),
                encode_psqlpy_params(columns, values),
            )

            return None

    async def delete(self, column: Column, value: Any) -> list[tuple[Any, ...]]:
        """Delete a row from a table."""
        async with self, self.connection() as connection:
            results = await connection.execute(
                self.delete_query.make_query(column),
                encode_psqlpy_params([column], [value]),
            )
            return results.result(as_tuple=True)

    async def delete_returning(
        self,
        where_column: Column,
        value: Any,
        returning: Sequence[Column],
    ) -> Optional[dict[str, Any]]:
        """Atomically delete a row and return requested columns."""
        async with self, self.connection() as connection:
            results = await connection.fetch(
                self.delete_returning_query.make_query(where_column, returning),
                encode_psqlpy_params([where_column], [value]),
            )
            rows = results.result()
            if not rows:
                return None
            row = rows[0]
            return {column.name: row[column.name] for column in returning}

    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Select a row from a table."""
        parameters = encode_psqlpy_params(where_columns or (), where_values)
        async with self, self.connection() as connection:
            rows = await connection.execute(
                querystring=self.select_query.make_query(columns, where_columns),
                parameters=parameters,
            )

            return rows.result()

    async def exists(self, id: Any) -> bool:
        """Check if a row exists in a table."""
        async with self, self.connection() as connection:
            results = await connection.fetch(
                self.select_query.make_query([Column("1", "")], [self.primary_key]),
                encode_psqlpy_params([self.primary_key], [id]),
            )
            v = results.result()
            return len(v) == 1

    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> list[tuple[Any, ...]]:
        """Delete a row from a table by date."""
        async with self, self.connection() as connection:
            results = await connection.execute(
                self.delete_by_date_query.make_query(self.created_at),
                (from_date, to_date),
            )
            return results.result(as_tuple=True)

    async def on_startup(self) -> None:
        """Create the table and its indexes, see ``QueryDriver.on_startup``."""
        if self.run_migrations:
            async with (
                self,
                self.connection() as connection,
                connection.transaction() as transaction,
            ):
                if await transaction.fetch_val(
                    self.relations_exist_query.make_query(self.index_columns),
                    self.relations_exist_query.relation_names(self.index_columns),
                ):
                    return
                await transaction.execute(
                    self.migration_lock_query.make_query(),
                    list(self.migration_lock_query.params()),
                )
                await transaction.execute_batch(self.create_table_query.make_query())
                if self.index_columns is not None:
                    await transaction.execute_batch(
                        self.create_index_query.make_query(self.index_columns),
                    )

    async def on_shutdown(self) -> None:
        """On shutdown."""
        # The pool opens on the first query, so a component that never ran
        # one has nothing to close.
        if self.pool is None:
            return
        self.pool.close()
        self.pool = None

    async def execute(self, query: str, *values: Any) -> list[dict[str, Any]]:
        """Execute a query."""
        async with self, self.connection() as connection:
            results = await connection.fetch(query, parameters=values)
            return results.result()


class PsqlpyListenDriver(ListenDriver):
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
        try:
            self.connection = ConnectionPool(
                self.connection_string,
                **self.connection_kwargs,
            )
            self.listener = self.connection.listener()

            await self.listener.add_callback(
                self.channel_name,
                self._notification_handler,
            )

            await self.listener.startup()
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

        self.listener.listen()

    async def on_shutdown(self) -> None:
        """On shutdown."""
        await self.listener.clear_channel_callbacks(
            self.channel_name,
        )
        await self.listener.shutdown()

        self.connection.close()

    async def _notification_handler(
        self,
        connection: object,
        payload: str,
        channel: str,
        process_id: int,
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
        message_id = parse_message_id(payload, self.channel_name)
        if message_id is not None:
            self._queue.put_nowait(message_id)

    async def __aiter__(self) -> AsyncIterator[int]:
        """Iterate over the queue."""
        while True:
            message_id = await self._queue.get()
            yield message_id
