import logging
from asyncio import Queue as AsyncQueue
from asyncio import TimeoutError as AsyncTimeoutError
from asyncio import wait_for
from contextlib import asynccontextmanager
from datetime import date, datetime
from functools import partial
from types import TracebackType
from typing import Any, AsyncGenerator, Optional, Sequence, Union

from asyncpg import Connection, Pool, connect, create_pool
from typing_extensions import Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.core.sql.catalog import (
    AddedIndex,
    Column,
)
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.stores.startup import Session, start_driver
from taskiq_postgresql.drivers._health import check_listen
from taskiq_postgresql.drivers._params import encode_params
from taskiq_postgresql.exceptions import DatabaseConnectionError
from taskiq_postgresql.utils.dsn import UNREADABLE_DSN, is_ambiguous_uri

logger = logging.getLogger(__name__)

# What fetch_row() holds until the query returns, None being a valid answer.
_NOT_FETCHED: Any = object()


class AsyncpgDriver(QueryDriver):
    """Asyncpg backend."""

    pool: Optional[Pool] = None

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
            asyncio.TimeoutError: if no connection is free within
                ``query_timeout``.
        """
        if self.pool is None:
            raise DatabaseConnectionError(
                "The pool is not open: enter `async with driver` first.",
            )
        # The acquire timeout is also the budget asyncpg gives a cancelled
        # query to finish before it terminates the connection; without one, a
        # connection that never answers keeps the release, and its caller,
        # waiting for good.
        async with self.pool.acquire(timeout=self.query_timeout) as connection:
            yield connection

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        # Raised here, where only `self` is a local: a frame that held the
        # string would hand it to an error tracker that captures locals.
        if self.pool is None and not await self._open_pool():
            raise DatabaseConnectionError(UNREADABLE_DSN)
        return self

    async def _open_pool(self) -> bool:
        """
        Create the pool; ``False`` for a URI asyncpg would misread.

        asyncpg's own ``ValueError`` for a bad port quotes part of the string,
        and a host name made of the rest of a password reaches the resolver's
        error, so the shared check runs first and refuses both.
        """
        if is_ambiguous_uri(self.connection_string):
            return False
        try:
            self.pool = await create_pool(
                self.connection_string,
                **self.connection_kwargs,
            )
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error
        return True

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
        row: Any = _NOT_FETCHED
        try:
            async with self, self.connection() as connection:
                row = await connection.fetchrow(
                    query,
                    *values,
                    timeout=self.query_timeout,
                )
        except Exception as error:
            if row is _NOT_FETCHED:
                raise
            # The query is done, a claim's DELETE included: raising would lose
            # the row it returned. asyncpg has already dropped the connection.
            logger.warning(
                "Releasing a connection to the pool failed after its query (%r)",
                error,
            )
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
            found = await connection.fetchval(
                self.exists_query.make_query(self.primary_key),
                id,
            )
            # The query selects the constant 1, or no row at all.
            return found is not None

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
        """Check the table, or migrate it; see ``QueryDriver.on_startup``."""
        await start_driver(self, self._session)

    @asynccontextmanager
    async def _session(self) -> AsyncGenerator[Session, None]:
        """One pooled connection in one transaction, for ``start_driver``."""
        async with self, self.connection() as connection, connection.transaction():
            yield Session(fetch_value=connection.fetchval, execute=connection.execute)

    async def on_shutdown(self) -> None:
        """
        Close the pool; by force once ``query_timeout`` runs out.

        Nothing to close when no query created the pool yet, or after a
        previous shutdown.
        """
        pool = self.pool
        if pool is None:
            return
        try:
            await wait_for(pool.close(), self.query_timeout)
        except AsyncTimeoutError:
            # close() waits on every connection; one that never answers would
            # hold the shutdown for good.
            pool.terminate()
        finally:
            self.pool = None

    async def execute(self, query: str, *values: Any) -> list[Any]:
        """Execute a query."""
        async with self, self.connection() as connection:
            return await connection.fetch(query, *values)


async def _terminate(connection: Connection) -> None:
    """Abort ``connection`` without waiting on a server that stopped answering."""
    connection.terminate()


class AsyncpgListenDriver(ListenDriver):
    """Asyncpg listen driver."""

    def __init__(
        self,
        connection_string: str,
        channel_name: Union[str, Identifier],
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the listen driver."""
        # The base class folds the name: add_listener() quotes what it is
        # given, so only the folded one receives the NOTIFY.
        super().__init__(connection_string, channel_name, **connection_kwargs)
        self.connection: Optional[Connection] = None
        # None: a connection closed, see _termination_handler.
        self._queue: AsyncQueue[Optional[str]] = AsyncQueue()

    async def on_startup(self) -> None:
        """On startup."""
        connection = await self._connect()
        if connection is None:
            # Raised here, not in _connect: see AsyncpgDriver.__aenter__.
            raise DatabaseConnectionError(UNREADABLE_DSN)
        try:
            await connection.add_listener(
                self.channel_name,
                self._notification_handler,
            )
            connection.add_termination_listener(self._termination_handler)
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error

    async def _connect(self) -> Optional[Connection]:
        """Connect; ``None`` for a URI asyncpg would misread."""
        if is_ambiguous_uri(self.connection_string):
            return None
        try:
            connection = self.connection = await connect(
                self.connection_string,
                **self.connection_kwargs,
            )
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error
        return connection

    async def on_shutdown(self) -> None:
        """
        Close the connection; by force once ``query_timeout`` runs out.

        Nothing to do when it never opened or is already closed. Closing ends
        the LISTEN on the server, so there is no UNLISTEN round trip first.
        """
        connection = self.connection
        if connection is None:
            return
        try:
            if not connection.is_closed():
                # The outer bound covers asyncpg before 0.32, whose close()
                # waits without a limit for a cancelled query's cancel request;
                # close() aborts the socket itself when cancelled or timed out.
                await wait_for(
                    connection.close(timeout=self.query_timeout),
                    self.query_timeout,
                )
        except Exception:
            connection.terminate()
        finally:
            self.connection = None

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
        """
        Yield each payload until the connection closes or fails a health check.

        Raises:
            DatabaseConnectionError: if the connection is not open, or does not
                answer the health check, run after ``health_check_interval``
                seconds without a NOTIFY.
        """
        connection = self.connection
        if connection is None:
            raise DatabaseConnectionError("the LISTEN connection is not open")
        while not connection.is_closed():
            try:
                payload = await wait_for(
                    self._queue.get(),
                    self.health_check_interval,
                )
            except AsyncTimeoutError:
                await check_listen(
                    connection.execute(
                        self.listen_text,
                        timeout=self.query_timeout,
                    ),
                    partial(_terminate, connection),
                )
                continue
            # A None left by an earlier connection is stale: keep reading.
            if payload is not None:
                yield payload
