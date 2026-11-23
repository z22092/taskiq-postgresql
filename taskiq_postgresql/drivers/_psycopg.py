import logging
import os
import socket
from asyncio import Future, ensure_future, gather, wait
from asyncio import TimeoutError as AsyncTimeoutError
from collections.abc import Awaitable
from contextlib import asynccontextmanager, suppress
from datetime import date, datetime
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Optional,
    Sequence,
    TypeVar,
    Union,
)
from uuid import UUID

from psycopg import AsyncConnection, AsyncCursor, AsyncRawCursor
from psycopg import Error as PsycopgError
from psycopg.rows import DictRow, TupleRow, dict_row
from psycopg_pool import AsyncConnectionPool
from typing_extensions import Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import AddedIndex, Column
from taskiq_postgresql.drivers._health import check_listen
from taskiq_postgresql.drivers._json import dumps
from taskiq_postgresql.exceptions import DatabaseConnectionError

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Seconds a wait woken up by shutting its socket down gets to end.
_WAKE_UP_WITHIN = 1.0


async def _answered_within(
    connection: AsyncConnection[Any],
    answer: Awaitable[_T],
    timeout: Optional[float],
) -> _T:
    """
    Await ``answer`` from ``connection``, closing the connection after ``timeout``.

    Cancelling the query instead makes psycopg try to cancel it on the server
    and then wait for its result, with no deadline up to psycopg 3.3.4: on a
    connection that never answers, forever. Shutting the socket down ends that
    wait.

    Raises:
        asyncio.TimeoutError: if no answer came within ``timeout`` seconds.
    """
    if timeout is None:
        return await answer
    task = ensure_future(answer)
    try:
        done, _ = await wait({task}, timeout=timeout)
    finally:
        if not task.done():
            await _give_up(connection, task)
    if not done:
        raise AsyncTimeoutError
    return task.result()


async def _give_up(connection: AsyncConnection[Any], task: Future[Any]) -> None:
    """End ``task``'s wait for an answer on ``connection``, then close it."""
    # Closing first would free the socket's fd while asyncio still reads it for
    # the pending wait: a socket opened next can get the same fd, and the
    # wait's cleanup would then stop asyncio reading that one. A shutdown,
    # through a duplicate so the fd itself stays open, wakes the wait instead.
    with (
        suppress(OSError, PsycopgError),
        socket.socket(fileno=os.dup(connection.pgconn.socket)) as duplicate,
    ):
        duplicate.shutdown(socket.SHUT_RDWR)
    await wait({task}, timeout=_WAKE_UP_WITHIN)
    await connection.close()
    # Only left running if the shutdown failed; on a closed connection the
    # cancellation ends it at once.
    task.cancel()
    await gather(task, return_exceptions=True)


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

        for index, (column, value) in enumerate(zip(columns, values, strict=True)):
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
        except AsyncTimeoutError:
            # A query that got no answer, see _answered_within().
            raise
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

    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        """Return the first row of ``query``, see ``QueryDriver.fetch_row``."""
        async with self, self.connection() as connection:
            return await _answered_within(
                connection,
                self._fetch_row(connection, query, values),
                self.query_timeout,
            )

    @staticmethod
    async def _fetch_row(
        connection: AsyncConnection[TupleRow],
        query: str,
        values: Sequence[Any],
    ) -> Optional[dict[str, Any]]:
        cursor = connection.cursor(row_factory=dict_row)
        cursor = await cursor.execute(query, params=values)
        # A statement without a result set, such as one with no RETURNING.
        row = None if cursor.rownumber is None else await cursor.fetchone()
        # Here, within the caller's bound: the pool would commit on its way
        # out too, but without a limit on the wait for the server's answer.
        await connection.commit()
        return row

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
        """Create the table and its indexes, see ``QueryDriver.on_startup``."""
        if self.run_migrations:
            async with (
                self,
                self.connection() as connection,
                connection.transaction(),
            ):
                cursor: AsyncCursor[TupleRow] = await connection.execute(
                    self.relations_exist_query.make_query(
                        self.index_columns,
                        added_columns=self.added_columns,
                        added_indexes=self.added_indexes,
                    ),
                    params=self.relations_exist_query.params(
                        self.index_columns,
                        added_columns=self.added_columns,
                        added_indexes=self.added_indexes,
                    ),
                )
                exists = await cursor.fetchone()
                if exists is not None and exists[0]:
                    return
                await connection.execute(
                    self.migration_lock_query.make_query(),
                    params=self.migration_lock_query.params(),
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
        """
        Close the pool, if a query created it.

        psycopg_pool bounds the close itself, by ``query_timeout`` when it is
        set and 5 s otherwise, and discards a connection that broke.
        """
        pool = self.pool
        if pool is None:
            return
        try:
            if self.query_timeout is None:
                await pool.close()
            else:
                await pool.close(timeout=self.query_timeout)
        finally:
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
    """Psycopg listen driver."""

    def __init__(
        self,
        connection_string: str,
        channel_name: str,
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the listen driver."""
        super().__init__(connection_string, channel_name, **connection_kwargs)
        self.connection: Optional[AsyncConnection[DictRow]] = None
        self._listen_statement = f"LISTEN {channel_name}"

    async def on_startup(self) -> None:
        """Connect and LISTEN on the channel."""
        try:
            connection = self.connection = await AsyncConnection.connect(
                self.connection_string,
                **self.connection_kwargs,
                autocommit=True,
                cursor_factory=AsyncRawCursor,
            )
            await _answered_within(
                connection,
                connection.execute(self._listen_statement),
                self.query_timeout,
            )
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def on_shutdown(self) -> None:
        """Close the connection; psycopg does not wait on the socket to do it."""
        connection = self.connection
        if connection is None:
            return
        try:
            await connection.close()
        finally:
            self.connection = None

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        """
        Yield each payload until the connection fails.

        Reads in rounds of ``health_check_interval`` seconds and checks the
        connection between two rounds. A NOTIFY that arrives during the check
        waits in psycopg's backlog and comes out first in the next round, which
        only works without a notify handler on the connection.

        Raises:
            OperationalError: when the server ends the connection.
            DatabaseConnectionError: when the connection is not open, or does
                not answer the health check.
        """
        connection = self.connection
        if connection is None:
            raise DatabaseConnectionError("the LISTEN connection is not open")
        while True:
            async for notify in connection.notifies(
                timeout=self.health_check_interval,
            ):
                yield notify.payload
            await check_listen(
                _answered_within(
                    connection,
                    connection.execute(self._listen_statement),
                    self.query_timeout,
                ),
                connection.close,
            )
