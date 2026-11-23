import asyncio
import os
import socket
from collections.abc import Awaitable, Callable
from typing import Any, Optional, TypeVar, Union
from urllib.parse import SplitResult, parse_qsl, urlsplit

import asyncpg

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

_ReturnType = TypeVar("_ReturnType")

# How long a listening worker may take to get a message it is notified of,
# with room for a busy machine, where 0.6 s was seen under CPU load.
NOTIFY_DELIVERS_WITHIN = 3.0


def closed_local_port() -> int:
    """Return a loopback port nothing listens on (never the default 5432)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def address_of(parts: SplitResult) -> str:
    """
    Return the ``host:port`` text of ``parts`` exactly as written.

    ``parts.hostname`` drops the brackets of an IPv6 literal, so rebuilding the
    address from it would turn ``[::1]:5432`` into the invalid ``::1:5432``.
    """
    return parts.netloc.rpartition("@")[2]


def with_netloc(
    parts: SplitResult,
    *,
    password: Optional[str],
    address: str,
) -> SplitResult:
    """Rebuild ``parts`` with the given password and address, keeping the user."""
    userinfo = parts.username or ""
    if password is not None:
        userinfo = f"{userinfo}:{password}"
    return parts._replace(netloc=f"{userinfo}@{address}" if userinfo else address)


def with_sslmode_allow(dsn: str) -> str:
    """
    ``dsn`` with ``sslmode=allow``, unless it or ``PGSSLMODE`` names a mode.

    For asyncpg, 0.32 at least: a connect() cancelled during the SSLRequest of
    the default ``prefer`` leaves an error nobody reads
    (MagicStack/asyncpg#1397), which fails a test that cancels connects.
    ``allow`` connects without that exchange and still moves to TLS when the
    server requires it.
    """
    parts = urlsplit(dsn)
    names = {name for name, _ in parse_qsl(parts.query, keep_blank_values=True)}
    if "sslmode" in names or os.environ.get("PGSSLMODE"):
        return dsn
    query = f"{parts.query}&sslmode=allow" if parts.query else "sslmode=allow"
    return parts._replace(query=query).geturl()


def in_database(dsn: str, name: str) -> str:
    """Return ``dsn`` pointed at the database ``name`` on the same server."""
    return urlsplit(dsn)._replace(path=f"/{name}").geturl()


async def fetch_on_server(dsn: str, query: str, *args: Any) -> list[asyncpg.Record]:
    """Run one query on the database ``dsn`` names, on a connection of its own."""
    connection = await asyncpg.connect(dsn)
    try:
        return await connection.fetch(query, *args)
    finally:
        await connection.close()


async def _execute_on_server(dsn: str, statement: str) -> None:
    """Run one statement on the database ``dsn`` names."""
    await fetch_on_server(dsn, statement)


async def create_database(dsn: str, name: str) -> None:
    """
    Create the database ``name`` on the server ``dsn`` points at.

    ``name`` is placed in SQL as written: pass a generated identifier.
    """
    await _execute_on_server(dsn, f"CREATE DATABASE {name}")


async def drop_database(dsn: str, name: str) -> None:
    """Drop the database ``name``, closing any connection still open to it."""
    await _execute_on_server(dsn, f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


async def run_script(dsn: str, driver: str, script: str) -> None:
    """
    Run a whole SQL script in one call, through ``driver``'s own library.

    This is how a generated script reaches the server from a tool: one simple
    query with every statement in it, including the script's own ``BEGIN`` and
    ``COMMIT``.
    """
    if driver == "asyncpg":
        import asyncpg  # noqa: PLC0415

        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute(script)
        finally:
            await connection.close()
    elif driver == "psycopg":
        import psycopg  # noqa: PLC0415

        # Autocommit, so the script's BEGIN opens the only transaction.
        async with await psycopg.AsyncConnection.connect(
            dsn,
            autocommit=True,
        ) as connection:
            await connection.execute(script)
    else:
        import psqlpy  # noqa: PLC0415

        pool = psqlpy.ConnectionPool(dsn)
        try:
            async with pool.acquire() as connection:
                await connection.execute_batch(script)
        finally:
            pool.close()


async def drop_and_shutdown(
    component: Union[
        PostgresqlResultBackend[_ReturnType],
        PostgresqlBroker,
        PostgresqlSchedulerSource,
    ],
    table: str,
) -> None:
    """Drop the component's table, then shut the component down."""
    async with component.driver, component.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {table}")
    await component.shutdown()


async def listen_backends(dsn: str, channel: str) -> list[int]:
    """Return the pids of the sessions whose last statement LISTENs on ``channel``."""
    rows = await fetch_on_server(
        dsn,
        "SELECT pid FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() AND query ILIKE $1",
        f"LISTEN %{channel}%",
    )
    return [row["pid"] for row in rows]


async def terminate_backends(dsn: str, pids: list[int]) -> None:
    """End the server sessions ``pids``, as a restart or an operator would."""
    await fetch_on_server(
        dsn,
        "SELECT pg_terminate_backend(p) FROM unnest($1::int[]) p",
        pids,
    )


async def eventually(
    check: Callable[[], Union[bool, Awaitable[bool]]],
    timeout: float,
) -> None:
    """
    Wait until ``check`` holds, polling every 0.05 s.

    Raises:
        AssertionError: if it still does not hold after ``timeout`` seconds.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = check()
        if not isinstance(result, bool):
            result = await result
        if result:
            return
        if loop.time() >= deadline:
            msg = f"still false after {timeout} s"
            raise AssertionError(msg)
        await asyncio.sleep(0.05)
