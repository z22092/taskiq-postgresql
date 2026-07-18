from asyncio import Event
from asyncio import TimeoutError as AsyncioTimeoutError
from collections import deque
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncIterator, Optional, Sequence, Union
from uuid import UUID

from asyncpg import Connection, Pool, connect, create_pool
from asyncpg.exceptions import (
    AdminShutdownError,
    CannotConnectNowError,
    CrashShutdownError,
    IdleSessionTimeoutError,
    InterfaceError,
    PostgresConnectionError,
    PostgresError,
    TooManyConnectionsError,
)
from asyncpg.transaction import Transaction
from taskiq.compat import IS_PYDANTIC2

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.exceptions import (
    DatabaseConnectionError,
    ListenerDisconnectedError,
    TransientDatabaseConnectionError,
)

_TRANSIENT_CONNECTION_ERRORS = (
    OSError,
    TimeoutError,
    AsyncioTimeoutError,
    PostgresConnectionError,
    AdminShutdownError,
    CrashShutdownError,
    CannotConnectNowError,
    IdleSessionTimeoutError,
    TooManyConnectionsError,
)

if IS_PYDANTIC2:
    from pydantic_core import to_json

    def dumps(value: dict) -> str:
        return to_json(value).decode()
else:
    from json import dumps as to_json

    def dumps(value: dict) -> str:
        return to_json(value)


class AsyncpgDriver(QueryDriver):
    """Asyncpg backend."""

    pool: Pool = None
    transaction: Transaction = None

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

    def __parser_query(
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
    async def connection(self) -> AsyncIterator[Connection]:
        connection: Optional[Connection] = None
        try:
            async with self.pool.acquire() as connection:
                yield connection
        except _TRANSIENT_CONNECTION_ERRORS:
            raise TransientDatabaseConnectionError(
                "PostgreSQL query connection failed.",
            ) from None
        except InterfaceError:
            if connection is not None and connection.is_closed():
                raise TransientDatabaseConnectionError(
                    "PostgreSQL query connection failed.",
                ) from None
            raise

    async def __aenter__(self) -> Connection:
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

    async def create_table(self) -> str:
        """Create a table."""
        async with self, self.connection() as connection:
            return await connection.execute(self.create_table_query.make_query())

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
            return await connection.fetchval(
                self.insert_query.make_query(columns, returning),
                *self.__parser_query(columns, values),
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
                    *self.__parser_query(columns, values),
                ),
            )

    async def delete(self, column: Column, value: Any) -> str:
        """Delete a row from a table."""
        async with self, self.connection() as connection:
            return await connection.execute(
                self.delete_query.make_query(column),
                value,
            )

    async def delete_returning(
        self,
        where_column: Column,
        value: Any,
        returning: Sequence[Column],
    ) -> Optional[dict[str, Any]]:
        """Atomically delete a row and return requested columns."""
        async with self, self.connection() as connection:
            row = await connection.fetchrow(
                self.delete_returning_query.make_query(where_column, returning),
                value,
            )
            if row is None:
                return None
            return {column.name: row[column.name] for column in returning}

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
                *self.__parser_query(where_columns, where_values or ()),
            )

            return [
                {column.name: row[column.name] for column in columns} for row in rows
            ]

    async def max_value(self, column: Column) -> int:
        """Return the greatest value in a column."""
        async with self, self.connection() as connection:
            value = await connection.fetchval(self.max_value_query.make_query(column))
            return int(value)

    async def select_available_ids(
        self,
        *,
        primary_key: Column,
        labels: Column,
        created_at: Column,
        channel_name: str,
        after_id: int,
        through_id: int,
        limit: int,
    ) -> list[int]:
        """Return a bounded page of message IDs that are ready to run."""
        async with self, self.connection() as connection:
            rows = await connection.fetch(
                self.select_available_ids_query.make_query(
                    primary_key,
                    labels,
                    created_at,
                ),
                after_id,
                through_id,
                channel_name,
                limit,
            )
            return [int(row[primary_key.name]) for row in rows]

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
        """On startup."""
        try:
            if self.run_migrations:
                async with self, self.connection() as connection:
                    transaction = connection.transaction()
                    await transaction.start()
                    await self.create_table()
                    await self.create_index()
                    await transaction.commit()
        except BaseException:
            with suppress(Exception):
                await self.on_shutdown()
            raise

    async def on_shutdown(self) -> None:
        """On shutdown."""
        if self.pool is not None:
            await self.pool.close()
        self.pool = None

    async def execute(self, query: str, *values: Any) -> str:
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
        self.connection: Optional[Connection] = None
        self._messages: deque[int] = deque()
        self._state_changed = Event()
        self._stopping = False
        self._disconnected = False
        self._termination_callback: Callable[[Connection], None] = (
            self._termination_handler
        )

    async def on_startup(self) -> None:
        """On startup."""
        self._messages.clear()
        self._state_changed.clear()
        self._stopping = False
        self._disconnected = False

        connection: Optional[Connection] = None
        termination_registered = False
        listener_registered = False
        try:
            connection = await connect(
                self.connection_string,
                **self.connection_kwargs,
            )
            self.connection = connection
            connection.add_termination_listener(self._termination_callback)
            termination_registered = True
            await connection.add_listener(
                self.channel_name,
                self._notification_handler,
            )
            listener_registered = True
        except BaseException as error:
            self._stopping = True
            await self._close_connection(
                connection,
                termination_registered=termination_registered,
                listener_registered=listener_registered,
                propagate_errors=False,
            )
            if self.connection is connection:
                self.connection = None
            connection_closed = connection is not None and connection.is_closed()
            if isinstance(error, _TRANSIENT_CONNECTION_ERRORS) or (
                isinstance(error, InterfaceError) and connection_closed
            ):
                raise TransientDatabaseConnectionError(
                    "PostgreSQL listener connection failed.",
                ) from None
            if isinstance(
                error,
                (PostgresError, InterfaceError, ValueError, TypeError),
            ):
                raise DatabaseConnectionError(
                    "PostgreSQL listener configuration failed.",
                ) from None
            raise

    async def on_shutdown(self) -> None:
        """On shutdown."""
        self._stopping = True
        self._state_changed.set()
        connection, self.connection = self.connection, None
        await self._close_connection(
            connection,
            termination_registered=True,
            listener_registered=True,
        )

    async def _close_connection(
        self,
        connection: Optional[Connection],
        *,
        termination_registered: bool,
        listener_registered: bool,
        propagate_errors: bool = True,
    ) -> None:
        """Close a listener connection without skipping later cleanup steps."""
        if connection is None:
            return

        first_error: Optional[Exception] = None
        if termination_registered:
            try:
                connection.remove_termination_listener(self._termination_callback)
            except Exception as error:  # pragma: no cover - defensive cleanup
                first_error = error
        if listener_registered:
            try:
                await connection.remove_listener(
                    self.channel_name,
                    self._notification_handler,
                )
            except Exception as error:  # pragma: no cover - defensive cleanup
                first_error = first_error or error
        try:
            await connection.close()
        except Exception as error:  # pragma: no cover - defensive cleanup
            first_error = first_error or error

        if first_error is not None and propagate_errors:
            raise first_error

    def _termination_handler(self, connection: Connection) -> None:
        """Wake consumers when the active connection terminates unexpectedly."""
        if connection is self.connection and not self._stopping:
            self._disconnected = True
            self._state_changed.set()

    def _notification_handler(
        self,
        con_ref: object,
        pid: int,
        channel: str,
        payload: object,
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
        if con_ref is self.connection and not self._stopping:
            self._messages.append(int(payload))
            self._state_changed.set()

    async def __aiter__(self) -> AsyncIterator[Any]:
        """Iterate over the queue."""
        while True:
            if self._stopping:
                return
            if self._disconnected:
                raise ListenerDisconnectedError(
                    "PostgreSQL listener connection was lost.",
                )
            if self._messages:
                yield self._messages.popleft()
                continue

            self._state_changed.clear()
            await self._state_changed.wait()
