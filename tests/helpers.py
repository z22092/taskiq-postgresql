import asyncio
import socket
from collections.abc import Awaitable, Callable
from typing import Any, Optional, TypeVar, Union
from urllib.parse import SplitResult, urlsplit

import asyncpg

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

_ReturnType = TypeVar("_ReturnType")


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
