from contextlib import asynccontextmanager
from datetime import date, datetime
from types import TracebackType
from typing import Any, AsyncGenerator, Optional, Sequence, Union, cast

from psycopg import AsyncConnection, AsyncCursor, AsyncRawCursor
from psycopg.rows import DictRow, TupleRow, dict_row
from psycopg_pool import AsyncConnectionPool
from typing_extensions import LiteralString, Self

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.abc.query import AddedIndex, Column
from taskiq_postgresql.drivers._params import encode_params
from taskiq_postgresql.exceptions import DatabaseConnectionError


def sql(query: str) -> LiteralString:
    """
    Mark a query built by ``taskiq_postgresql.abc.query`` as trusted SQL.

    psycopg's annotations accept only literal strings, to keep data out of
    the query text. These queries are built from the table, column and channel
    names given to the component at construction; values always travel as
    bind parameters.

    Args:
        query: the SQL text, built by the query builders or by the broker's
            own ``NOTIFY``.

    Returns:
        The same string, typed so the type checker accepts it.
    """
    return cast("LiteralString", query)


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
    async def connection(self) -> AsyncGenerator[AsyncConnection[TupleRow], None]:
        """
        Borrow a connection from the pool for the duration of the block.

        The pool is created closed and opened here on first use, so building a
        driver never connects. Every error raised inside the block is
        re-raised as ``DatabaseConnectionError``, so callers handle one
        exception type whatever the driver.

        Yields:
            A pooled psycopg connection, returned when the block ends.

        Raises:
            DatabaseConnectionError: if the pool object does not exist yet
                (enter ``async with driver`` first), or for any error raised
                while the connection is held.
        """
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

    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        """Return the first row of ``query``, see ``QueryDriver.fetch_row``."""
        async with self, self.connection() as connection:
            cursor = connection.cursor(row_factory=dict_row)
            cursor = await cursor.execute(sql(query), params=values)
            # A statement without a result set, such as one with no RETURNING.
            if cursor.rownumber is None:
                return None
            return await cursor.fetchone()

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
        """Create the table and its indexes, see ``QueryDriver.on_startup``."""
        if self.run_migrations:
            async with (
                self,
                self.connection() as connection,
                connection.transaction(),
            ):
                cursor: AsyncCursor[TupleRow] = await connection.execute(
                    sql(
                        self.relations_exist_query.make_query(
                            self.index_columns,
                            added_columns=self.added_columns,
                            added_indexes=self.added_indexes,
                        ),
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
                    sql(self.migration_lock_query.make_query()),
                    params=self.migration_lock_query.params(),
                )
                await connection.execute(sql(self.create_table_query.make_query()))
                if self.index_columns is not None:
                    await connection.execute(
                        sql(self.create_index_query.make_query(self.index_columns)),
                    )
                if self.added_columns or self.added_indexes:
                    await connection.execute(
                        sql(
                            self.add_columns_query.make_query(
                                self.added_columns,
                                self.added_indexes,
                            ),
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

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        """Yield each payload unparsed, see ``ListenDriver.__aiter__``."""
        # notifies() alone: psycopg warns that notifies() and a notify handler
        # on the same connection are not reliable together.
        async for notify in self.connection.notifies():
            yield notify.payload
