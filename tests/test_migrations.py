from __future__ import annotations

import asyncio
import secrets
from typing import TYPE_CHECKING, AsyncGenerator, Optional, Union
from urllib.parse import urlsplit

import pytest

from taskiq_postgresql.abc import query as sql_builders
from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import address_of

if TYPE_CHECKING:
    from asyncpg import Connection

Component = Union[
    PostgresqlBroker,
    PostgresqlResultBackend[object],
    PostgresqlSchedulerSource,
]

COMPONENTS = ["broker", "result_backend", "scheduler_source"]

# Pinned here on purpose: two releases running side by side (a rolling deploy)
# only serialize their migrations if both take this exact lock.
MIGRATION_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))"
MIGRATION_LOCK_NAMESPACE = "taskiq_postgresql"

# The smallest pool each driver accepts. psqlpy refuses fewer than two
# connections, so the one-connection case only exists for asyncpg and psycopg.
SMALLEST_POOL = {
    "asyncpg": {"min_size": 1, "max_size": 1},
    "psycopg": {"min_size": 1, "max_size": 1},
    "psqlpy": {"max_db_pool_size": 2},
}

# Keeps eight components per round well under the server's connection limit.
SMALL_POOL = {
    "asyncpg": {"min_size": 1, "max_size": 2},
    "psycopg": {"min_size": 1, "max_size": 2},
    "psqlpy": {"max_db_pool_size": 2},
}

WAIT_SECONDS = 10


def build_component(kind: str, dsn: str, table: str, driver: str) -> Component:
    if kind == "broker":
        return PostgresqlBroker(
            dsn=dsn,
            table_name=table,
            channel_name=f"{table}_channel",
            driver=driver,
            run_migrations=True,
        )
    if kind == "result_backend":
        return PostgresqlResultBackend(
            dsn=dsn,
            table_name=table,
            driver=driver,
            run_migrations=True,
        )
    return PostgresqlSchedulerSource(
        dsn=dsn,
        table_name=table,
        driver=driver,
        run_migrations=True,
    )


async def close_component(component: Component) -> None:
    """
    Close what ``startup()`` opened, also after a failed ``startup()``.

    The components' own ``shutdown()`` cannot do it: the result backend's
    leaves the pool open, and the broker's fails when LISTEN never started.
    """
    if component.driver.pool is not None:
        await component.driver.on_shutdown()
    if isinstance(component, PostgresqlBroker) and hasattr(
        component.listen_driver,
        "connection",
    ):
        await component.listen_driver.on_shutdown()


@pytest.fixture
async def admin(postgresql_dsn: str) -> AsyncGenerator[Connection, None]:
    """A superuser connection for setup, checks and cleanup."""
    asyncpg = pytest.importorskip("asyncpg")
    connection = await asyncpg.connect(postgresql_dsn)
    yield connection
    await connection.close()


async def count_tables(admin: Connection, table: str) -> int:
    return await admin.fetchval(
        "SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind = 'r'",
        table,
    )


async def wait_until_blocked_by(
    admin: Connection,
    blocker_pid: int,
    startup: asyncio.Task[None],
) -> Optional[str]:
    """
    Wait until a session waits on a lock held by ``blocker_pid``.

    :return: the type of the lock it waits on, or ``None`` when ``startup``
        ended first.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_SECONDS
    while not startup.done():
        locktype = await admin.fetchval(
            "SELECT locktype FROM pg_locks "
            "WHERE NOT granted AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
            blocker_pid,
        )
        if locktype is not None:
            return locktype
        if loop.time() > deadline:
            pytest.fail(f"startup() never waited on the lock for {WAIT_SECONDS}s")
        await asyncio.sleep(0.05)
    return None


def test_migration_lock_key_is_pinned() -> None:
    lock = sql_builders.MigrationLockQuery("jobs")

    assert lock.make_query() == MIGRATION_LOCK_SQL
    assert lock.params() == (MIGRATION_LOCK_NAMESPACE, "jobs")


def test_fast_path_checks_the_table_and_every_index_it_creates() -> None:
    index_columns = [Column("id", "UUID"), Column("task_name", "VARCHAR(100)")]
    fast_path = sql_builders.RelationsExistQuery("jobs")
    create_index = sql_builders.CreateIndexQuery("jobs").make_query(index_columns)

    names = fast_path.relation_names(index_columns)

    assert names == ["jobs", "jobs_id_idx", "jobs_task_name_idx"]
    assert fast_path.make_query(index_columns) == (
        "SELECT to_regclass($1) IS NOT NULL"
        " AND to_regclass($2) IS NOT NULL"
        " AND to_regclass($3) IS NOT NULL"
    )
    for index_name in names[1:]:
        assert f"CREATE INDEX IF NOT EXISTS {index_name} ON jobs " in create_index


@pytest.mark.anyio
@pytest.mark.parametrize("kind", COMPONENTS)
async def test_startup_waits_for_a_migration_in_progress(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    asyncpg = pytest.importorskip("asyncpg")
    component = build_component(kind, postgresql_dsn, postgres_table, driver)
    blocker = await asyncpg.connect(postgresql_dsn)
    try:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.execute(
            MIGRATION_LOCK_SQL,
            MIGRATION_LOCK_NAMESPACE,
            postgres_table,
        )
        await blocker.execute(component.driver.create_table_query.make_query())
        startup = asyncio.create_task(component.startup())
        try:
            locktype = await wait_until_blocked_by(
                admin,
                blocker.get_server_pid(),
                startup,
            )
        finally:
            await transaction.commit()

        await asyncio.wait_for(startup, WAIT_SECONDS)

        # The pinned migration lock, not a catalog row, is what it waited on.
        assert locktype == "advisory"
        assert await count_tables(admin, postgres_table) == 1
    finally:
        await blocker.close()
        await close_component(component)
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", COMPONENTS)
async def test_startup_migrates_with_the_smallest_pool(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    component = build_component(kind, postgresql_dsn, postgres_table, driver)
    # Set on the query driver only: the broker hands its connection_kwargs to
    # the LISTEN driver too, which takes no pool options.
    component.driver.connection_kwargs = SMALLEST_POOL[driver]
    try:
        await asyncio.wait_for(component.startup(), WAIT_SECONDS)

        assert await count_tables(admin, postgres_table) == 1
    finally:
        await close_component(component)
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", COMPONENTS)
async def test_startup_needs_no_ddl_privilege_once_the_tables_exist(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    owner = build_component(kind, postgresql_dsn, postgres_table, driver)
    await owner.startup()
    await close_component(owner)

    role = f"{postgres_table}_dml"
    password = secrets.token_hex(16)
    schema = await admin.fetchval(
        "SELECT relnamespace::regnamespace::text FROM pg_class "
        "WHERE oid = to_regclass($1)",
        postgres_table,
    )
    await admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
    try:
        await admin.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
        await admin.execute(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON {postgres_table} TO {role}",
        )
        if kind == "broker":
            await admin.execute(
                f"GRANT USAGE ON SEQUENCE {postgres_table}_id_seq TO {role}",
            )
        assert not await admin.fetchval(
            "SELECT has_schema_privilege($1, $2, 'CREATE')",
            role,
            schema,
        )
        parts = urlsplit(postgresql_dsn)
        role_dsn = parts._replace(
            netloc=f"{role}:{password}@{address_of(parts)}",
        ).geturl()
        limited = build_component(kind, role_dsn, postgres_table, driver)
        try:
            await limited.startup()
        finally:
            await close_component(limited)
    finally:
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")
        await admin.execute(f"DROP OWNED BY {role}")
        await admin.execute(f"DROP ROLE {role}")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", COMPONENTS)
async def test_concurrent_first_startups_all_succeed(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    for round_number in range(3):
        table = f"{postgres_table}_{round_number}"
        components = [
            build_component(kind, postgresql_dsn, table, driver) for _ in range(8)
        ]
        for component in components:
            component.driver.connection_kwargs = SMALL_POOL[driver]
        try:
            outcomes = await asyncio.gather(
                *(component.startup() for component in components),
                return_exceptions=True,
            )
        finally:
            for component in components:
                await close_component(component)

        try:
            assert [
                repr(outcome)
                for outcome in outcomes
                if isinstance(outcome, BaseException)
            ] == []
            assert await count_tables(admin, table) == 1
        finally:
            await admin.execute(f"DROP TABLE IF EXISTS {table}")
