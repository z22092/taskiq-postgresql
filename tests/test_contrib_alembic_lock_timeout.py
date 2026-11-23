"""
The lock timeout of the taskiq steps in an Alembic revision.

Alembic runs a revision in a transaction it shares with the user's own
migrations. The taskiq steps bound their lock waits there as the script of
``taskiq postgresql generate`` does (``DEFAULT_LOCK_TIMEOUT`` seconds unless
configured, nothing with ``0``), then give the transaction back the
``lock_timeout`` it had, so the user's statements after them wait as before.
The upgrade runs through each SQLAlchemy dialect the integration supports: a
sync one (psycopg) and an async one (asyncpg).

Every test here carries the ``alembic`` mark: ``pytest -m alembic``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
import string
import types
from typing import Any, AsyncGenerator, Callable, Iterator, Optional

import asyncpg
import pytest
from alembic import op
from alembic.autogenerate import render_python_code
from alembic.operations import ops
from sqlalchemy import text

from taskiq_postgresql.cli import PostgresqlCMD
from taskiq_postgresql.contrib.alembic import (
    TaskiqStepOp,
    alembic_format,
    taskiq_metadata,
    upgrade_taskiq_tables,
)
from taskiq_postgresql.migrations import (
    DEFAULT_LOCK_TIMEOUT,
    FormatOptions,
    MigrationStep,
    StepObjects,
    TableSchema,
    migration_statements,
    render_sql,
    table_schema,
)
from tests.helpers import (
    create_database,
    drop_database,
    fetch_on_server,
    in_database,
    run_script,
)
from tests.test_contrib_alembic import (
    SQLALCHEMY_DRIVERS,
    bounded,
    offline_sql,
    revision_namespace,
    run_upgrade,
)
from tests.test_contrib_alembic_autogenerate import executed, produce
from tests.test_contrib_sqlalchemy import databases
from tests.test_migration_script import LOCK_TIMEOUT, WAIT_SECONDS, lock_waited_on

pytestmark = pytest.mark.alembic

# The two ways a revision runs the taskiq steps: the op.execute(...) lines
# autogenerate and the alembic format write, and the helper a hand-written
# revision calls.
PATHS = ("revision", "upgrade_taskiq_tables")
# The option of context.configure(...) in env.py that autogenerate reads.
OPTION = "taskiq_postgresql_lock_timeout"
REFUSED = "lock_timeout must be a whole number of seconds from 0 to 2147483"
# What the user's own migration set before the taskiq steps ran.
EARLIER = "7s"


@pytest.fixture
def new_database(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Create empty databases for one test, and drop them after it."""
    yield from databases(postgresql_dsn)


@pytest.fixture
async def empty_dsn(postgresql_dsn: str) -> AsyncGenerator[str, None]:
    """DSN of a database created empty for one test and dropped after it."""
    name = "alembic_" + "".join(random.choices(string.ascii_lowercase, k=10))
    await create_database(postgresql_dsn, name)
    yield in_database(postgresql_dsn, name)
    await drop_database(postgresql_dsn, name)


@pytest.fixture
async def inspector(empty_dsn: str) -> AsyncGenerator[asyncpg.Connection, None]:
    """A superuser connection to the empty database, for setup and checks."""
    connection = await asyncpg.connect(empty_dsn)
    yield connection
    await connection.close()


def lock_of(schema: TableSchema) -> str:
    return migration_statements([schema])[0]


def taskiq_steps(
    path: str,
    schema: TableSchema,
    lock_timeout: Optional[int] = None,
) -> Callable[[], None]:
    """
    Return what a revision's ``upgrade()`` runs for every step of ``schema``.

    ``None`` leaves the lock timeout at its default.
    """
    options: dict[str, Any] = {}
    if lock_timeout is not None:
        options["lock_timeout"] = lock_timeout
    if path == "upgrade_taskiq_tables":
        through = schema.steps[-1].number
        return lambda: upgrade_taskiq_tables(op, schema, through=through, **options)
    source = alembic_format(
        [schema],
        FormatOptions(**options, extra={"revision": "taskiq_steps"}),
    )
    return revision_namespace(source)["upgrade"]


def recording(table_name: str) -> TableSchema:
    """A table whose one step stores the ``lock_timeout`` it runs with."""
    statement = (
        f"CREATE TABLE IF NOT EXISTS {table_name} AS "
        "SELECT current_setting('lock_timeout') AS lock_timeout"
    )
    return dataclasses.replace(
        table_schema("scheduler_source", table_name=table_name),
        steps=(
            MigrationStep(
                number=1,
                statements=(statement,),
                objects=StepObjects(
                    creates_table=True,
                    columns=("lock_timeout",),
                    indexes=(),
                ),
            ),
        ),
    )


# --- what the revision runs -----------------------------------------------------


def test_each_step_of_a_revision_bounds_its_own_lock_waits() -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    for lock_timeout in (DEFAULT_LOCK_TIMEOUT, 3, 0):
        source = alembic_format(
            schemas,
            FormatOptions(lock_timeout=lock_timeout, extra={"revision": "x"}),
        )

        upgrade = offline_sql(revision_namespace(source)["upgrade"])
        expected = []
        for schema in sorted(schemas, key=lambda table: table.name):
            for step in schema.steps:
                expected.extend(
                    bounded([lock_of(schema), *step.statements], lock_timeout),
                )
        assert upgrade == "".join(f"{statement};\n\n" for statement in expected)
        if not lock_timeout:
            assert "lock_timeout" not in source


def test_generate_writes_the_lock_timeout_into_the_revision(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = PostgresqlCMD().exec(
        [
            "generate",
            "--component",
            "broker",
            "--format",
            "alembic",
            "--lock-timeout",
            "3",
            "--option",
            "revision=taskiq_tables",
        ],
    )

    captured = capsys.readouterr()
    assert code == 0, captured.err
    assert captured.out == alembic_format(
        [table_schema("broker")],
        FormatOptions(lock_timeout=3, extra={"revision": "taskiq_tables"}),
    )
    assert "SET LOCAL lock_timeout = '3s'" in captured.out


@pytest.mark.parametrize(
    "lock_timeout",
    [None, 3, 0],
    ids=["default", "configured", "none"],
)
def test_upgrade_taskiq_tables_bounds_the_lock_waits_of_its_statements(
    lock_timeout: Optional[int],
) -> None:
    broker = table_schema("broker")

    printed = offline_sql(taskiq_steps("upgrade_taskiq_tables", broker, lock_timeout))

    seconds = DEFAULT_LOCK_TIMEOUT if lock_timeout is None else lock_timeout
    # One bound around all the statements of the call, the advisory lock's
    # wait included.
    assert printed == "".join(
        f"{statement};\n\n"
        for statement in bounded(migration_statements([broker]), seconds)
    )
    if lock_timeout == 0:
        assert "lock_timeout" not in printed


@pytest.mark.parametrize(
    "lock_timeout",
    [-1, 2_147_484, True, 1.5, "10"],
    ids=["negative", "above-the-server-maximum", "bool", "fraction", "text"],
)
def test_a_lock_timeout_the_revision_cannot_set_is_refused(lock_timeout: Any) -> None:
    broker = table_schema("broker")
    calls: list[str] = []
    # Stands for alembic.op and records what would run.
    recording_op = types.SimpleNamespace(execute=calls.append)

    with pytest.raises(ValueError, match=REFUSED):
        upgrade_taskiq_tables(
            recording_op,  # type: ignore[arg-type]
            broker,
            through=2,
            lock_timeout=lock_timeout,
        )
    with pytest.raises(ValueError, match=REFUSED):
        TaskiqStepOp(
            broker.name,
            broker.kind,
            broker.steps[0],
            lock_timeout=lock_timeout,
        )
    # Refused before anything runs.
    assert calls == []


def test_autogenerate_writes_the_lock_timeout_env_py_configures(
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    broker = table_schema("broker")

    for options, seconds in (
        ({}, DEFAULT_LOCK_TIMEOUT),
        ({OPTION: 3}, 3),
        ({OPTION: 0}, 0),
    ):
        # Keyword arguments of context.configure(...) reach the comparator as
        # these options.
        script = produce(dsn, taskiq_metadata(broker), **options)

        upgrade = render_python_code(script.upgrade_ops)
        assert executed(upgrade) == [
            statement
            for step in broker.steps
            for statement in bounded([lock_of(broker), *step.statements], seconds)
        ]
        assert [operation.lock_timeout for operation in script.upgrade_ops.ops] == [
            seconds,
            seconds,
        ]


@pytest.mark.parametrize("lock_timeout", [-1, "10"], ids=["negative", "text"])
def test_autogenerate_refuses_a_lock_timeout_the_revision_cannot_set(
    lock_timeout: Any,
    new_database: Callable[[], str],
) -> None:
    with pytest.raises(ValueError, match=REFUSED):
        produce(
            new_database(),
            taskiq_metadata(table_schema("broker")),
            **{OPTION: lock_timeout},
        )


# --- against PostgreSQL, through each SQLAlchemy dialect -----------------------


@pytest.mark.parametrize("earlier", ["SET", "SET LOCAL"])
@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
def test_after_the_steps_the_transaction_has_its_own_lock_timeout_again(
    sqlalchemy_driver: str,
    path: str,
    earlier: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    cases = [
        ("default", None, f"{DEFAULT_LOCK_TIMEOUT}s"),
        ("configured", 3, "3s"),
        # Without a bound of their own, the steps wait as the transaction does.
        ("none", 0, EARLIER),
    ]

    for label, lock_timeout, during in cases:
        name = f"recorded_{label}"
        steps = taskiq_steps(path, recording(name), lock_timeout)
        after: list[str] = []

        def upgrade(
            steps: Callable[[], None] = steps,
            after: list[str] = after,
        ) -> None:
            # The user's own migration, earlier in the same transaction.
            op.execute(f"{earlier} lock_timeout = '{EARLIER}'")
            steps()
            # current_setting() reads what SHOW lock_timeout shows.
            query = text("SELECT current_setting('lock_timeout')")
            after.append(op.get_bind().execute(query).scalar_one())

        run_upgrade(dsn, sqlalchemy_driver, upgrade)

        query = f"SELECT lock_timeout FROM {name}"  # noqa: S608
        recorded = asyncio.run(fetch_on_server(dsn, query))
        assert [row["lock_timeout"] for row in recorded] == [during], label
        # SET LOCAL lock_timeout TO DEFAULT would give the server's default
        # here, not the value the transaction had.
        assert after == [EARLIER], label


@pytest.mark.anyio
@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
async def test_the_lock_timeout_bounds_the_wait_for_the_advisory_lock(
    sqlalchemy_driver: str,
    path: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    schema = table_schema("broker", table_name=postgres_table)
    steps = taskiq_steps(path, schema, LOCK_TIMEOUT)
    loop = asyncio.get_running_loop()
    # Another revision, or a worker's run_migrations=True, holds the lock.
    holder = await asyncpg.connect(empty_dsn)
    try:
        holding = holder.transaction()
        await holding.start()
        await holder.execute(lock_of(schema))
        started = loop.time()
        # Alembic's run is blocking: a thread of its own keeps this loop free.
        migration = asyncio.create_task(
            asyncio.to_thread(run_upgrade, empty_dsn, sqlalchemy_driver, steps),
        )
        try:
            waited_on = await lock_waited_on(
                inspector,
                holder.get_server_pid(),
                migration,
            )
            with pytest.raises(Exception, match="lock timeout"):
                await asyncio.wait_for(migration, WAIT_SECONDS)
            waited = loop.time() - started
        finally:
            await holding.rollback()
            await asyncio.gather(migration, return_exceptions=True)
    finally:
        await holder.close()

    assert waited_on == ("advisory", "ExclusiveLock")
    assert LOCK_TIMEOUT <= waited < WAIT_SECONDS
    # Alembic's transaction rolled back: the table it would have created is
    # not there.
    assert await inspector.fetchval("SELECT to_regclass($1)", postgres_table) is None


@pytest.mark.anyio
@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
async def test_the_lock_timeout_bounds_the_wait_of_alter_table_on_a_live_table(
    sqlalchemy_driver: str,
    path: str,
    empty_dsn: str,
    inspector: asyncpg.Connection,
    postgres_table: str,
) -> None:
    schema = table_schema("broker", table_name=postgres_table)
    await run_script(empty_dsn, "asyncpg", render_sql([schema]))
    steps = taskiq_steps(path, schema, LOCK_TIMEOUT)
    insert = (
        f"INSERT INTO {postgres_table} (task_id, task_name, message, labels) "  # noqa: S608
        "VALUES (gen_random_uuid(), 'task', 'message', '{}')"
    )
    loop = asyncio.get_running_loop()
    # A worker's transaction that read the table and has not ended yet.
    reader = await asyncpg.connect(empty_dsn)
    writer = await asyncpg.connect(empty_dsn)
    produced: Optional[asyncio.Task[str]] = None
    try:
        reading = reader.transaction()
        await reading.start()
        await reader.fetchval(f"SELECT count(*) FROM {postgres_table}")  # noqa: S608
        started = loop.time()
        migration = asyncio.create_task(
            asyncio.to_thread(run_upgrade, empty_dsn, sqlalchemy_driver, steps),
        )
        try:
            waited_on = await lock_waited_on(
                inspector,
                reader.get_server_pid(),
                migration,
            )
            # A producer's INSERT, queued behind the waiting ALTER TABLE.
            produced = asyncio.create_task(writer.execute(insert))
            with pytest.raises(Exception, match="lock timeout"):
                await asyncio.wait_for(migration, WAIT_SECONDS)
            waited = loop.time() - started
            await asyncio.wait_for(produced, WAIT_SECONDS)
        finally:
            await reading.rollback()
            await asyncio.gather(
                migration,
                *([produced] if produced else []),
                return_exceptions=True,
            )
    finally:
        await reader.close()
        await writer.close()

    assert waited_on == ("relation", "AccessExclusiveLock")
    assert LOCK_TIMEOUT <= waited < WAIT_SECONDS
    # The INSERT went through while the reader's transaction was still open.
    assert await inspector.fetchval(f"SELECT count(*) FROM {postgres_table}") == 1  # noqa: S608


def test_a_step_operation_renders_its_bound_for_autogenerate() -> None:
    broker = table_schema("broker")
    step = broker.steps[1]

    rendered = render_python_code(
        ops.UpgradeOps([TaskiqStepOp(broker.name, broker.kind, step, lock_timeout=3)]),
    )

    assert executed(rendered) == bounded([lock_of(broker), *step.statements], 3)
