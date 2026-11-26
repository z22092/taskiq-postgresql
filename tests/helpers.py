import asyncio
import os
import random
import signal
import socket
import string
import subprocess
import sys
from collections.abc import (
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Sequence,
)
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, TypeVar, Union
from urllib.parse import SplitResult, parse_qsl, urlsplit

import asyncpg
import pytest

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.migrations import (
    DEFAULT_LOCK_TIMEOUT,
    MigrationPlan,
    TableSchema,
    migration_statements,
    plan_migration,
    read_catalog,
)
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

_ReturnType = TypeVar("_ReturnType")

Component = Union[
    PostgresqlBroker,
    PostgresqlResultBackend[Any],
    PostgresqlSchedulerSource,
]

# Construction needs no server; the components never connect with it.
UNUSED_DSN = "postgresql://localhost/unused"

# How long a listening worker may take to get a message it is notified of,
# with room for a busy machine, where 0.6 s was seen under CPU load.
NOTIFY_DELIVERS_WITHIN = 3.0

# The longest a test waits on the server; statement_timeout ends any wait
# that nothing else bounds.
WAIT_SECONDS = 10
# Short, so a test that waits it out stays fast; far below WAIT_SECONDS.
LOCK_TIMEOUT = 1

# The broker's table and index exactly as 0.4.x creates them, before the
# claim columns existed.
BROKER_TABLE_0_4_SQL = (
    "CREATE TABLE {table} (id SERIAL NOT NULL PRIMARY KEY, "
    "task_id Uuid NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, "
    "labels JSONB NOT NULL, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());"
    "CREATE INDEX {table}_id_idx ON {table} USING HASH (id);"
)

# Where the taskiq steps keep the transaction's own lock_timeout while theirs
# applies, and the statements that keep and give it back.
SAVED_LOCK_TIMEOUT = "taskiq_postgresql_step.lock_timeout"
SAVE_LOCK_TIMEOUT = (
    f"SELECT set_config('{SAVED_LOCK_TIMEOUT}', current_setting('lock_timeout'), true)"
)
RESTORE_LOCK_TIMEOUT = (
    "SELECT set_config('lock_timeout', "
    f"NULLIF(current_setting('{SAVED_LOCK_TIMEOUT}', true), ''), true)"
)


def bounded(
    statements: Sequence[str],
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
) -> list[str]:
    """
    ``statements`` as the Alembic and Django integrations run them.

    They run with a ``lock_timeout`` of their own, set after keeping the
    transaction's, which comes back after them; ``0`` adds nothing.
    """
    if not lock_timeout:
        return list(statements)
    return [
        SAVE_LOCK_TIMEOUT,
        f"SET LOCAL lock_timeout = '{lock_timeout}s'",
        *statements,
        RESTORE_LOCK_TIMEOUT,
    ]


def lock_of(schema: TableSchema) -> str:
    """Return the advisory lock statement of ``schema``'s table."""
    return migration_statements([schema])[0]


def block(monkeypatch: pytest.MonkeyPatch, package: str) -> None:
    """Make ``package`` and every submodule of it unimportable, as uninstalled."""
    loaded = [name for name in sys.modules if name.startswith(f"{package}.")]
    for name in (package, *loaded):
        monkeypatch.setitem(sys.modules, name, None)


def build_component(
    kind: str,
    dsn: str = UNUSED_DSN,
    table_name: Optional[str] = None,
    driver: str = "asyncpg",
    *,
    field: Optional[str] = None,
    run_migrations: bool = True,
) -> Component:
    """
    Build a component of ``kind``; it migrates its table by default.

    A broker on ``table_name`` listens on its own ``<table_name>_channel``.
    """
    options: dict[str, Any] = {
        "dsn": dsn,
        "driver": driver,
        "run_migrations": run_migrations,
    }
    if table_name is not None:
        options["table_name"] = table_name
    if field is not None:
        options["field_for_task_id"] = field
    if kind == "broker":
        if table_name is not None:
            options["channel_name"] = f"{table_name}_channel"
        return PostgresqlBroker(**options)
    if kind == "result_backend":
        return PostgresqlResultBackend(**options)
    return PostgresqlSchedulerSource(**options)


def new_database(dsn: str) -> Generator[str, None, None]:
    """Create an empty database on the server of ``dsn``, yield its DSN, drop it."""
    name = "cli_" + "".join(random.choices(string.ascii_lowercase, k=10))
    asyncio.run(create_database(dsn, name))
    try:
        yield in_database(dsn, name)
    finally:
        asyncio.run(drop_database(dsn, name))


@contextmanager
def importable_modules(
    directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[[str], str]]:
    """
    Yield a function that writes a module into ``directory`` and names it.

    Each name is unique, so in-process tests never reuse another test's
    import. The command adds ``--app-dir`` to ``sys.path``, as taskiq worker
    does; ``sys.path`` and the imported modules are put back afterwards.
    """
    monkeypatch.setattr(sys, "path", list(sys.path))
    names: list[str] = []

    def write(source: str) -> str:
        name = "tkq_app_" + "".join(random.choices(string.ascii_lowercase, k=10))
        (directory / f"{name}.py").write_text(source, encoding="utf-8")
        names.append(name)
        return name

    try:
        yield write
    finally:
        for name in names:
            sys.modules.pop(name, None)


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


def word() -> str:
    """Letters only, made at run time: no literal looks like a credential."""
    return "".join(random.choices(string.ascii_lowercase, k=12))


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


async def column_is_not_null(dsn: str, table: str, column: str) -> bool:
    """Tell whether ``column`` of ``table`` is ``NOT NULL`` in the database ``dsn``."""
    (row,) = await fetch_on_server(
        dsn,
        "SELECT attnotnull FROM pg_attribute"
        " WHERE attrelid = to_regclass($1) AND attname = $2",
        table,
        column,
    )
    return row[0]


def script_of(statements: Iterable[str]) -> str:
    """Return ``statements`` as a script: each one ended by ``;`` and a newline."""
    return "".join(f"{statement};\n" for statement in statements)


async def _execute_on_server(dsn: str, statement: str) -> None:
    """Run one statement on the database ``dsn`` names."""
    await fetch_on_server(dsn, statement)


def tables_in(dsn: str) -> set[str]:
    """Return the tables of the current schema of the database ``dsn`` names."""
    rows = asyncio.run(
        fetch_on_server(
            dsn,
            "SELECT DISTINCT table_name FROM information_schema.columns"
            " WHERE table_schema = current_schema()",
        ),
    )
    return {row[0] for row in rows}


def plan_of(dsn: str, schemas: list[TableSchema]) -> MigrationPlan:
    """Compare the database ``dsn`` names with ``schemas``."""

    async def fetch_value(query: str) -> Any:
        (row,) = await fetch_on_server(dsn, query)
        return row[0]

    return plan_migration(schemas, asyncio.run(read_catalog(schemas, fetch_value)))


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


async def close_component(component: Component) -> None:
    """
    Close what ``startup()`` opened, also after a failed ``startup()``.

    The broker's own ``shutdown()`` cannot do it: it fails when LISTEN never
    started.
    """
    if component.driver.pool is not None:
        await component.driver.on_shutdown()
    if isinstance(component, PostgresqlBroker) and hasattr(
        component.listen_driver,
        "connection",
    ):
        await component.listen_driver.on_shutdown()


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


def assert_exited_cleanly(
    script: subprocess.CompletedProcess[str],
    driver: str,
    *,
    version: tuple[int, ...] = sys.version_info[:3],
) -> None:
    """
    Assert that a README script exited with code 0.

    psqlpy aborts the interpreter at exit, after the script's work is done,
    on its own and without this package (psqlpy-python/psqlpy#182): psqlpy
    alone (connect, ``SELECT 1``, close) aborted in 14 of 200 runs on Python
    3.10, 12 of 150 on 3.11, 9 of 150 on 3.12 and 0 of 150 on 3.13, under 16
    busy loops on 8 cores. Newer releases print ``PyGILState_Release`` while
    the runtime is finalizing; psqlpy 0.11.3, the floor, aborts with no fatal
    error line at all, only pydantic's ``UserWarning`` on stderr. So the
    tolerated abort is SIGABRT of a psqlpy script on Python before 3.13,
    with no traceback and either no fatal error line or that ``PyGILState_Release``
    one. Any other exit still fails, and the caller still checks what the script
    did.

    Args:
        script: The finished script process, with its stderr captured as text.
        driver: The driver the script ran on.
        version: The interpreter version the script ran on; the running one
            by default.

    Raises:
        AssertionError: If the script did not exit with code 0 and the abort is
            not the tolerated one.
    """
    stderr = script.stderr
    gil_abort = (
        "Fatal Python error: PyGILState_Release" in stderr and "finalizing" in stderr
    )
    if (
        script.returncode == -signal.SIGABRT
        and driver == "psqlpy"
        and version < (3, 13)
        and "Traceback (most recent call last)" not in stderr
        and ("Fatal Python error" not in stderr or gil_abort)
    ):
        return
    assert script.returncode == 0, script.stderr
