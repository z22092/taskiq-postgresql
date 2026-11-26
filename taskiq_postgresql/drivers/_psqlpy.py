import json
from asyncio import Queue as AsyncQueue
from asyncio import Task, create_task, gather, wait_for
from asyncio import TimeoutError as AsyncTimeoutError
from contextlib import asynccontextmanager, suppress
from datetime import date, datetime
from functools import partial
from types import TracebackType
from typing import Any, AsyncGenerator, Optional, Sequence, Union
from uuid import UUID

from psqlpy import Connection, ConnectionPool, Listener, Transaction
from psqlpy.extra_types import JSONB
from typing_extensions import Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.core.sql.catalog import (
    AddedIndex,
    Column,
)
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.drivers._health import check_listen
from taskiq_postgresql.drivers._schema import Session, start_table
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


async def _fetch_value(transaction: Transaction, query: str, *values: Any) -> Any:
    """Return the first column of the first row of ``query``, or ``None``."""
    return await transaction.fetch_val(query, list(values) or None)


async def _execute(transaction: Transaction, query: str, *values: Any) -> None:
    """Run a statement with ``values``, or, without values, a whole script."""
    if values:
        await transaction.execute(query, list(values))
    else:
        # execute() prepares its query, which takes one statement only.
        await transaction.execute_batch(query)


class PsqlpyDriver(QueryDriver):
    """Asyncpg backend."""

    pool: Optional[ConnectionPool] = None

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

    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        """Return the first row of ``query``, see ``QueryDriver.fetch_row``."""
        async with self, self.connection() as connection:
            results = await connection.fetch(query, list(values))
            rows = results.result()
            return rows[0] if rows else None

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
                self.exists_query.make_query(self.primary_key),
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
        """Check the table, or migrate it; see ``QueryDriver.on_startup``."""
        await start_table(self, self._session)

    @asynccontextmanager
    async def _session(self) -> AsyncGenerator[Session, None]:
        """One pooled connection in one transaction, for ``start_table``."""
        async with (
            self,
            self.connection() as connection,
            connection.transaction() as transaction,
        ):
            yield Session(
                fetch_value=partial(_fetch_value, transaction),
                execute=partial(_execute, transaction),
            )

    async def on_shutdown(self) -> None:
        """Close the pool, if a query created it."""
        pool, self.pool = self.pool, None
        if pool is not None:
            pool.close()

    async def execute(self, query: str, *values: Any) -> list[dict[str, Any]]:
        """Execute a query."""
        async with self, self.connection() as connection:
            results = await connection.fetch(query, parameters=values)
            return results.result()


async def _ignore_notify(*_: Any) -> None:
    """Callback that registers the channel; the payloads are read by iterating."""


async def _close(pool: ConnectionPool) -> None:
    """Close ``pool``, which psqlpy does at once, without a round trip."""
    pool.close()


class PsqlpyListenDriver(ListenDriver):
    """Psqlpy listen driver."""

    def __init__(
        self,
        connection_string: str,
        channel_name: Union[str, Identifier],
        **connection_kwargs: Any,
    ) -> None:
        """Initialize the listen driver."""
        super().__init__(connection_string, channel_name, **connection_kwargs)
        self.connection: Optional[ConnectionPool] = None
        self.listener: Optional[Listener] = None
        # A payload, or the error that ended the listener.
        self._queue: AsyncQueue[Union[str, BaseException]] = AsyncQueue()
        self._reader: Optional[Task[None]] = None

    async def on_startup(self) -> None:
        """Open a pool and its listener, and start reading the listener."""
        # A new queue, so nothing from a previous connection is read again.
        self._queue = AsyncQueue()
        try:
            pool = self.connection = ConnectionPool(
                self.connection_string,
                **self.connection_kwargs,
            )
            listener = self.listener = pool.listener()
            await listener.add_callback(self.channel_name, _ignore_notify)
            await listener.startup()
            # psqlpy (0.13.1 at least) sends its own LISTEN only once
            # iterating starts, so a NOTIFY before that would be lost: listen
            # before returning (psqlpy-python/psqlpy#179).
            await wait_for(
                listener.connection.execute(self.listen_text),
                self.query_timeout,
            )
        except Exception as error:
            raise DatabaseConnectionError(str(error)) from error
        self._reader = create_task(self._read(listener, self._queue))

    @staticmethod
    async def _read(
        listener: Listener,
        queue: "AsyncQueue[Union[str, BaseException]]",
    ) -> None:
        """
        Move each payload of ``listener`` to ``queue``, then the error that ended it.

        Iterating, unlike the background listen(), raises when the server ends
        the connection. The iteration runs in a task of its own so that the
        health check never cancels psqlpy's pending read.
        """
        try:
            async for message in listener:
                queue.put_nowait(message.payload)
        except Exception as error:
            queue.put_nowait(error)
        else:
            queue.put_nowait(DatabaseConnectionError("the LISTEN connection ended"))

    async def on_shutdown(self) -> None:
        """
        Stop reading, then shut the listener down and close its pool.

        Each step runs even if the one before failed on a dead connection, or
        was cancelled.
        """
        reader, self._reader = self._reader, None
        listener, pool = self.listener, self.connection
        if reader is not None:
            reader.cancel()
            await gather(reader, return_exceptions=True)
        if reader is not None or listener is not None:
            # Ends a running iteration of this connection, the way a closed
            # connection ends the other drivers' iterators. Not when nothing
            # was open: no iteration can be running then.
            self._queue.put_nowait(
                DatabaseConnectionError("the LISTEN connection was shut down"),
            )
        try:
            if listener is not None:
                with suppress(Exception):
                    await wait_for(listener.shutdown(), self.query_timeout)
        finally:
            if pool is not None:
                pool.close()
            self.listener = None
            self.connection = None

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        """
        Yield each payload until the listener fails.

        Raises:
            DatabaseConnectionError: when the listener is not open, when the
                server ends the connection, or when it does not answer the
                health check run after ``health_check_interval`` seconds
                without a NOTIFY.
        """
        listener, pool = self.listener, self.connection
        if listener is None or pool is None:
            raise DatabaseConnectionError("the LISTEN connection is not open")
        queue = self._queue
        while True:
            try:
                item = await wait_for(queue.get(), self.health_check_interval)
            except AsyncTimeoutError:
                # On the listener's own connection, the one that registered
                # the channel.
                await check_listen(
                    wait_for(
                        listener.connection.execute(self.listen_text),
                        self.query_timeout,
                    ),
                    partial(_close, pool),
                )
                continue
            if isinstance(item, DatabaseConnectionError):
                # An end this driver queued itself, already worded.
                raise item
            if isinstance(item, BaseException):
                msg = f"the LISTEN connection ended: {item!r}"
                raise DatabaseConnectionError(msg) from item
            yield item
