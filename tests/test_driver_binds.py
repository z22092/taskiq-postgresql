"""
Values cross every driver as bind parameters and come back as they went in.

The tests drive the public operations (``kick``, ``listen``, ``set_result``,
``get_result``, ``is_result_ready``) and look at the server from a raw asyncpg
connection, so they say what a user of the package gets, not what a driver
method returns for SQL written in the test. Each one takes the ``driver``
fixture (directly or through ``broker``), so it runs once per driver.
"""

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage, TaskiqResult

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from tests.helpers import (
    WAIT_SECONDS,
    build_component,
    drop_and_shutdown,
    eventually,
    fetch_on_server,
)

pytestmark = pytest.mark.anyio


async def first_delivery(broker: PostgresqlBroker) -> AckableMessage:
    """Return the first message ``broker.listen()`` delivers."""
    async for message in broker.listen():
        return message
    msg = "listen() ended without delivering a message"
    raise AssertionError(msg)


async def query_blocked_on(
    dsn: str,
    table: str,
    operation: Callable[[], Awaitable[object]],
) -> str:
    """
    Run ``operation`` while ``table`` is locked and return the SQL it waits with.

    A raw connection takes an exclusive lock on the table, so the statement the
    operation sends stays active on the server; the same connection reads it
    from ``pg_stat_activity``. The lock is released, and the operation awaited
    within ``WAIT_SECONDS``, however the wait ends.

    Args:
        dsn: The DSN of the database that holds ``table``.
        table: The table the operation writes to.
        operation: The public call to run; it must write to ``table``.

    Returns:
        The text of the statement, as the server shows it.
    """
    connection = await asyncpg.connect(dsn)
    holder = connection.transaction()
    await holder.start()
    seen: list[str] = []

    async def observed() -> bool:
        rows = await connection.fetch(
            "SELECT query FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid() AND state = 'active' "
            "AND wait_event_type = 'Lock' AND query LIKE $1",
            f"%{table}%",
        )
        seen[:] = [row["query"] for row in rows]
        return bool(seen)

    try:
        await connection.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
        pending = asyncio.ensure_future(operation())
        try:
            await eventually(observed, timeout=WAIT_SECONDS)
        finally:
            try:
                await holder.rollback()
            finally:
                await asyncio.wait_for(pending, timeout=WAIT_SECONDS)
    finally:
        await connection.close()

    (statement,) = seen
    return statement


async def test_a_kicked_message_is_stored_and_delivered_as_it_went_in(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    postgres_table: str,
) -> None:
    """
    JSONB, UUID and bytea values reach the table and the worker as they went in.

    Each driver writes and reads these column types through its own API, so
    the row read through a raw connection and the message ``listen()`` hands
    to a worker must both equal what was kicked.
    """
    task_id = uuid.uuid4().hex
    labels = {"queue": "default", "text": "ação ✓", "nested": '{"a": 1}'}
    # Every byte value, so nothing is lost to a text encoding.
    payload = bytes(range(256))

    await broker.kick(
        BrokerMessage(
            task_id=task_id,
            task_name="round_trip",
            message=payload,
            labels=labels,
        ),
    )
    (row,) = await fetch_on_server(
        postgresql_dsn,
        f"SELECT task_id, task_name, message, labels FROM {postgres_table}",  # noqa: S608
    )
    delivered = await asyncio.wait_for(first_delivery(broker), timeout=WAIT_SECONDS)

    assert str(row["task_id"]) == str(uuid.UUID(task_id))
    assert row["task_name"] == "round_trip"
    assert bytes(row["message"]) == payload
    assert json.loads(row["labels"]) == labels
    assert delivered.data == payload


@pytest.mark.parametrize(
    ("field_for_task_id", "task_id"),
    [
        pytest.param("Uuid", uuid.uuid4().hex, id="uuid"),
        pytest.param("Text", "task-é-" + uuid.uuid4().hex, id="text"),
        pytest.param("VarChar", "task-é-" + uuid.uuid4().hex, id="varchar"),
    ],
)
async def test_a_result_round_trips_under_every_task_id_type(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    field_for_task_id: str,
    task_id: str,
) -> None:
    """
    A result is stored and read back whichever type its task_id column has.

    The Uuid, Text and VarChar task id types all bind the same Python string
    and must find the row again on a second write.
    """
    sent: TaskiqResult[Any] = TaskiqResult(
        is_err=False,
        log=None,
        return_value={"numbers": [1, 2, 3], "text": "ação ✓"},
        execution_time=0.25,
    )
    backend = build_component(
        "result_backend",
        postgresql_dsn,
        postgres_table,
        driver,
        field=field_for_task_id,
    )
    assert isinstance(backend, PostgresqlResultBackend)
    await backend.startup()
    try:
        await backend.set_result(task_id, sent)
        ready = await backend.is_result_ready(task_id)
        received = await backend.get_result(task_id)
        # The same id again replaces the row: the key's type must compare equal.
        await backend.set_result(task_id, sent)
        (count,) = await fetch_on_server(
            postgresql_dsn,
            f"SELECT count(*) AS total FROM {postgres_table}",  # noqa: S608
        )
    finally:
        await drop_and_shutdown(backend, postgres_table)

    # Truthy, not `is True`: a driver may report existence as the selected 1.
    assert ready
    assert received.return_value == sent.return_value
    assert received.execution_time == sent.execution_time
    assert count["total"] == 1


async def test_set_result_binds_the_task_id_on_the_server(
    result_backend: PostgresqlResultBackend[Any],
    postgresql_dsn: str,
    postgres_table: str,
) -> None:
    """
    The statement ``set_result`` sends shows ``$1`` on the server, not the task id.

    Every driver binds on the server, so values are never part of the SQL text
    and cannot change its meaning.
    """
    task_id = uuid.uuid4().hex
    result: TaskiqResult[Any] = TaskiqResult(
        is_err=False,
        log=None,
        return_value="bind_probe",
        execution_time=0.1,
    )

    statement = await query_blocked_on(
        postgresql_dsn,
        postgres_table,
        lambda: result_backend.set_result(task_id, result),
    )

    assert "$1" in statement
    assert task_id not in statement
    assert await result_backend.is_result_ready(task_id)


async def test_kick_binds_every_value_on_the_server(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    postgres_table: str,
) -> None:
    """
    The INSERT ``kick`` sends shows ``$1`` on the server, none of the values.

    A task name that is SQL text would change the statement's meaning if it
    were formatted into it; bound, it is only data.
    """
    task_id = uuid.uuid4().hex
    task_name = f"bind_secret_'); DROP TABLE {postgres_table}; --{uuid.uuid4().hex}"
    message = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"bind_probe",
        labels={"queue": "default"},
    )

    statement = await query_blocked_on(
        postgresql_dsn,
        postgres_table,
        lambda: broker.kick(message),
    )
    (row,) = await fetch_on_server(
        postgresql_dsn,
        f"SELECT task_name FROM {postgres_table}",  # noqa: S608
    )

    assert "$1" in statement
    assert task_id not in statement
    assert task_name not in statement
    assert row["task_name"] == task_name
