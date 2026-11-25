from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import secrets
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, AsyncIterator, Optional, Union
from urllib.parse import urlsplit

import pytest
from taskiq import BrokerMessage

from taskiq_postgresql.abc import query as sql_builders
from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.migrations import table_schema
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from taskiq_postgresql.utils import get_db_driver
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

# The broker's table and index exactly as 0.4.x creates them, before the
# claim columns existed.
BROKER_TABLE_0_4_SQL = (
    "CREATE TABLE {table} (id SERIAL NOT NULL PRIMARY KEY, "
    "task_id Uuid NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, "
    "labels JSONB NOT NULL, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());"
    "CREATE INDEX {table}_id_idx ON {table} USING HASH (id);"
)

# The smallest pool each driver accepts. psqlpy refuses fewer than two
# connections, so the one-connection case only exists for asyncpg and psycopg.
SMALLEST_POOL = {
    "asyncpg": {"min_size": 1, "max_size": 1},
    "psycopg": {"min_size": 1, "max_size": 1},
    "psqlpy": {"max_db_pool_size": 2},
}

SCRAM_ITERATIONS = 4096


def scram_verifier(password: str) -> str:
    """
    Return the SCRAM-SHA-256 verifier PostgreSQL stores for ``password``.

    ``CREATE ROLE`` gets the verifier instead of the password, so the password
    never reaches the server log, which records DDL text under
    ``log_statement = 'ddl'`` and the text of a failed statement.
    """
    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, SCRAM_ITERATIONS)
    client_key = hmac.digest(salted, b"Client Key", "sha256")
    server_key = hmac.digest(salted, b"Server Key", "sha256")
    stored_key = hashlib.sha256(client_key).digest()
    encoded = [
        base64.b64encode(part).decode() for part in (salt, stored_key, server_key)
    ]
    return f"SCRAM-SHA-256${SCRAM_ITERATIONS}:{encoded[0]}${encoded[1]}:{encoded[2]}"


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


@pytest.fixture
async def admin(postgresql_dsn: str) -> AsyncGenerator[Connection, None]:
    """A superuser connection for setup, checks and cleanup."""
    asyncpg = pytest.importorskip("asyncpg")
    connection = await asyncpg.connect(postgresql_dsn)
    yield connection
    await connection.close()


async def claim_schema(admin: Connection, table: str) -> tuple[list[str], bool]:
    """
    Tell which claim columns ``table`` has and whether their index exists.

    :return: the names of the claim columns present, sorted, and whether the
        index on them exists.
    """
    columns = await admin.fetch(
        "SELECT attname::text FROM pg_attribute WHERE attrelid = to_regclass($1) "
        "AND attname IN ('channel', 'not_before') AND NOT attisdropped "
        "ORDER BY attname",
        table,
    )
    index_exists = await admin.fetchval(
        "SELECT to_regclass($1) IS NOT NULL",
        f"{table}_not_before_idx",
    )
    return [column[0] for column in columns], index_exists


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


def test_only_the_broker_adds_columns_to_an_existing_table() -> None:
    broker = PostgresqlBroker(dsn="unused").driver
    result_backend = PostgresqlResultBackend(dsn="unused").driver
    scheduler_source = PostgresqlSchedulerSource(dsn="unused").driver

    assert [column.name for column in broker.added_columns] == ["channel", "not_before"]
    assert [
        (index.name, [column.name for column in index.columns])
        for index in broker.added_indexes
    ] == [("taskiq_messages_not_before_idx", ["channel", "not_before", "id"])]
    for driver in (result_backend, scheduler_source):
        assert (tuple(driver.added_columns), tuple(driver.added_indexes)) == ((), ())


def test_fast_path_also_checks_the_claim_columns_and_their_index() -> None:
    driver = PostgresqlBroker(dsn="unused", table_name="jobs").driver
    index_columns = [Column("id", "SERIAL")]
    fast_path = sql_builders.RelationsExistQuery("jobs")
    add_claim_columns = driver.add_columns_query.make_query(
        driver.added_columns,
        driver.added_indexes,
    )
    added = {
        "added_columns": driver.added_columns,
        "added_indexes": driver.added_indexes,
    }

    names = fast_path.relation_names(
        index_columns,
        added_indexes=driver.added_indexes,
    )

    assert names == ["jobs", "jobs_id_idx", "jobs_not_before_idx"]
    assert fast_path.params(index_columns, **added) == [
        *names,
        "channel",
        "not_before",
    ]
    assert fast_path.make_query(index_columns, **added) == (
        "SELECT to_regclass($1) IS NOT NULL"
        " AND to_regclass($2) IS NOT NULL"
        " AND to_regclass($3) IS NOT NULL"
        " AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1)"
        " AND attname::text = $4 AND NOT attisdropped)"
        " AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1)"
        " AND attname::text = $5 AND NOT attisdropped)"
    )
    assert f"CREATE INDEX IF NOT EXISTS {names[2]}\n  ON jobs " in add_claim_columns


def test_fast_path_without_claim_columns_checks_only_the_relations() -> None:
    index_columns = [Column("id", "UUID"), Column("task_name", "VARCHAR(100)")]
    fast_path = sql_builders.RelationsExistQuery("jobs")

    assert fast_path.params(index_columns) == fast_path.relation_names(index_columns)
    assert fast_path.make_query(index_columns) == (
        "SELECT to_regclass($1) IS NOT NULL"
        " AND to_regclass($2) IS NOT NULL"
        " AND to_regclass($3) IS NOT NULL"
    )


def test_add_claim_columns_query_is_the_upgrade_sql() -> None:
    driver = PostgresqlBroker(dsn="unused").driver

    query = driver.add_columns_query.make_query(
        driver.added_columns,
        driver.added_indexes,
    )

    assert query == (
        "ALTER TABLE taskiq_messages\n"
        "  ADD COLUMN IF NOT EXISTS channel VARCHAR,\n"
        "  ADD COLUMN IF NOT EXISTS not_before"
        " TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();\n"
        "CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx\n"
        "  ON taskiq_messages (channel, not_before, id);"
    )


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


@asynccontextmanager
async def role_with(
    admin: Connection,
    postgresql_dsn: str,
    table: str,
    name: str,
    table_privileges: str,
    *,
    sequence: bool,
) -> AsyncIterator[str]:
    """
    Create a role with ``table_privileges`` on ``table`` and no DDL privilege.

    It also gets USAGE on the schema and, given ``sequence``, on the table's
    id sequence. On exit it drops the role.

    :yield: a DSN that logs in as the role.
    """
    role = f"{table}_{name}"
    password = secrets.token_hex(16)
    schema = await admin.fetchval(
        "SELECT relnamespace::regnamespace::text FROM pg_class "
        "WHERE oid = to_regclass($1)",
        table,
    )
    await admin.execute(
        f"CREATE ROLE {role} LOGIN PASSWORD '{scram_verifier(password)}'",
    )
    try:
        await admin.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
        await admin.execute(f"GRANT {table_privileges} ON {table} TO {role}")
        if sequence:
            await admin.execute(
                f"GRANT USAGE ON SEQUENCE {table}_id_seq TO {role}",
            )
        assert not await admin.fetchval(
            "SELECT has_schema_privilege($1, $2, 'CREATE')",
            role,
            schema,
        )
        parts = urlsplit(postgresql_dsn)
        yield parts._replace(
            netloc=f"{role}:{password}@{address_of(parts)}",
        ).geturl()
    finally:
        await admin.execute(f"DROP OWNED BY {role}")
        await admin.execute(f"DROP ROLE {role}")


@asynccontextmanager
async def dml_role(
    admin: Connection,
    postgresql_dsn: str,
    table: str,
    kind: str,
) -> AsyncIterator[str]:
    """
    Create a role that can use ``table`` but run no DDL on it.

    On exit it drops the role, then ``table``.

    :yield: a DSN that logs in as the role.
    """
    try:
        async with role_with(
            admin,
            postgresql_dsn,
            table,
            "dml",
            "SELECT, INSERT, UPDATE, DELETE",
            sequence=kind == "broker",
        ) as role_dsn:
            yield role_dsn
    finally:
        await admin.execute(f"DROP TABLE IF EXISTS {table}")


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

    async with dml_role(admin, postgresql_dsn, postgres_table, kind) as role_dsn:
        limited = build_component(kind, role_dsn, postgres_table, driver)
        try:
            await limited.startup()
            if isinstance(limited, PostgresqlBroker):
                await limited.kick(
                    BrokerMessage(
                        task_id=uuid.uuid4().hex,
                        task_name="limited_task",
                        message=b"limited",
                        labels={},
                    ),
                )
                # The sweep locks the row FOR UPDATE, which needs UPDATE.
                claimed = await limited._claim_due()
                assert claimed is not None
                assert claimed.data == b"limited"
        finally:
            await close_component(limited)


def privileges_message(payload: bytes) -> BrokerMessage:
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="privileges_task",
        message=payload,
        labels={},
    )


@pytest.mark.anyio
async def test_the_documented_privileges_split_between_producer_and_worker(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    """Each role has exactly what the docs list for it, and lacks the other's."""
    owner = build_component("broker", postgresql_dsn, postgres_table, driver)
    await owner.startup()
    await close_component(owner)

    try:
        async with (
            # INSERT returns the new id, so a producer also needs SELECT.
            role_with(
                admin,
                postgresql_dsn,
                postgres_table,
                "producer",
                "INSERT, SELECT",
                sequence=True,
            ) as producer_dsn,
            role_with(
                admin,
                postgresql_dsn,
                postgres_table,
                "worker",
                "SELECT, UPDATE, DELETE",
                sequence=False,
            ) as worker_dsn,
        ):
            producer = build_component("broker", producer_dsn, postgres_table, driver)
            worker = build_component("broker", worker_dsn, postgres_table, driver)
            try:
                await producer.startup()
                await worker.startup()
                await producer.kick(privileges_message(b"swept"))
                await producer.kick(privileges_message(b"notified"))
                notified_id = await admin.fetchval(
                    f"SELECT max(id) FROM {postgres_table}",  # noqa: S608
                )
                swept = await worker._claim_due()
                notified = await worker._claim_notified(notified_id)

                with pytest.raises(Exception, match="permission denied"):
                    await worker.kick(privileges_message(b"from-worker"))
                with pytest.raises(Exception, match="permission denied"):
                    await producer.driver.fetch_row(
                        producer._claim_due_query,
                        producer.channel_name,
                    )
            finally:
                await close_component(producer)
                await close_component(worker)
    finally:
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")

    assert swept is not None
    assert notified is not None
    assert (swept.data, notified.data) == (b"swept", b"notified")


@pytest.mark.anyio
async def test_startup_without_ddl_privilege_fails_on_a_0_4_table(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    await admin.execute(BROKER_TABLE_0_4_SQL.format(table=postgres_table))

    async with dml_role(admin, postgresql_dsn, postgres_table, "broker") as role_dsn:
        limited = build_component("broker", role_dsn, postgres_table, driver)
        try:
            with pytest.raises(Exception, match=r"permission denied|must be owner"):
                await limited.startup()
        finally:
            await close_component(limited)

        # The failed migration left the table as it was.
        assert await claim_schema(admin, postgres_table) == ([], False)


@pytest.mark.anyio
async def test_startup_adds_the_claim_columns_to_a_0_4_table(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    await admin.execute(BROKER_TABLE_0_4_SQL.format(table=postgres_table))
    await admin.execute(
        f"INSERT INTO {postgres_table} (task_id, task_name, message, labels) "  # noqa: S608
        "VALUES (gen_random_uuid(), 'old_task', 'old', '{}')",
    )
    broker = build_component("broker", postgresql_dsn, postgres_table, driver)
    try:
        await asyncio.wait_for(broker.startup(), WAIT_SECONDS)

        assert await claim_schema(admin, postgres_table) == (
            ["channel", "not_before"],
            True,
        )
        assert await admin.fetchval(
            "SELECT pg_get_indexdef(to_regclass($1))",
            f"{postgres_table}_not_before_idx",
        ) == (
            f"CREATE INDEX {postgres_table}_not_before_idx ON public.{postgres_table}"
            " USING btree (channel, not_before, id)"
        )
        rows = await admin.fetch(
            f"SELECT message, channel IS NULL AS legacy, not_before <= NOW() AS due "  # noqa: S608
            f"FROM {postgres_table}",
        )
        assert [tuple(row) for row in rows] == [(b"old", True, True)]
    finally:
        await close_component(broker)
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")


@pytest.mark.anyio
async def test_startup_after_the_upgrade_takes_the_fast_path(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    asyncpg = pytest.importorskip("asyncpg")
    await admin.execute(BROKER_TABLE_0_4_SQL.format(table=postgres_table))
    first = build_component("broker", postgresql_dsn, postgres_table, driver)
    try:
        await first.startup()
    finally:
        await close_component(first)

    second = build_component("broker", postgresql_dsn, postgres_table, driver)
    blocker = await asyncpg.connect(postgresql_dsn)
    try:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.execute(
            MIGRATION_LOCK_SQL,
            MIGRATION_LOCK_NAMESPACE,
            postgres_table,
        )

        # A startup that took the lock would wait for the blocker forever.
        await asyncio.wait_for(second.startup(), WAIT_SECONDS)

        assert await claim_schema(admin, postgres_table) == (
            ["channel", "not_before"],
            True,
        )
    finally:
        await blocker.close()
        await close_component(second)
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")


@pytest.mark.anyio
async def test_a_driver_no_component_owns_takes_no_lock_once_its_table_exists(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    """Without a kind it cannot compare, so it probes its relations as before."""
    asyncpg = pytest.importorskip("asyncpg")
    owner = build_component("result_backend", postgresql_dsn, postgres_table, driver)
    try:
        await owner.startup()
    finally:
        await close_component(owner)
    alone = get_db_driver(driver)(
        postgresql_dsn,
        postgres_table,
        columns=owner.driver.columns,
        primary_key=owner.driver.primary_key,
        index_columns=owner.driver.index_columns,
        run_migrations=True,
    )
    assert alone.component_kind is None
    blocker = await asyncpg.connect(postgresql_dsn)
    try:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.execute(
            MIGRATION_LOCK_SQL,
            MIGRATION_LOCK_NAMESPACE,
            postgres_table,
        )

        # A startup that took the lock would wait for the blocker forever.
        await asyncio.wait_for(alone.on_startup(), WAIT_SECONDS)
    finally:
        await blocker.close()
        await alone.on_shutdown()
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", COMPONENTS)
async def test_a_startup_that_migrates_logs_a_problem_no_step_fixes(
    kind: str,
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    index = table_schema(kind, table_name=postgres_table).indexes[0].name
    # The name of the table's first index, taken first by another table.
    await admin.execute(f"CREATE TABLE {postgres_table}_other (id INTEGER)")
    await admin.execute(f"CREATE INDEX {index} ON {postgres_table}_other (id)")
    component = build_component(kind, postgresql_dsn, postgres_table, driver)
    try:
        with caplog.at_level(logging.WARNING):
            await component.startup()
    finally:
        await close_component(component)
        await admin.execute(
            f"DROP TABLE IF EXISTS {postgres_table}, {postgres_table}_other",
        )

    # IF NOT EXISTS skipped that index; without the warning nothing would say.
    (warning,) = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.name.startswith("taskiq_postgresql")
    ]
    assert "index_on_other_table" in warning
    assert index in warning


@pytest.mark.anyio
async def test_concurrent_startups_upgrade_a_0_4_table(
    driver: str,
    postgresql_dsn: str,
    postgres_table: str,
    admin: Connection,
) -> None:
    await admin.execute(BROKER_TABLE_0_4_SQL.format(table=postgres_table))
    components = [
        build_component("broker", postgresql_dsn, postgres_table, driver)
        for _ in range(8)
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
            repr(outcome) for outcome in outcomes if isinstance(outcome, BaseException)
        ] == []
        assert await claim_schema(admin, postgres_table) == (
            ["channel", "not_before"],
            True,
        )
    finally:
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")


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
