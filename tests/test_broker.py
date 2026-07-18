import asyncio
import json
import os
import uuid
from collections.abc import AsyncGenerator
from typing import Literal, Union

import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql.abc.query import BROKER_CHANNEL_LABEL
from taskiq_postgresql.exceptions import DatabaseConnectionError

pytestmark = pytest.mark.anyio


async def get_first_task(
    broker: PostgresqlBroker,
) -> Union[AckableMessage, bytes]:
    """
    Get the first message from the broker's listen method.

    :param broker: Instance of AsyncpgBroker.
    :return: The first AckableMessage received.
    """
    async for message in broker.listen():
        return message
    return b""


@pytest.fixture
async def recovery_broker(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[PostgresqlBroker, None]:
    """Create the asyncpg broker used by listener-recovery regressions."""
    if os.environ.get("TEST_DRIVER") not in (None, "asyncpg"):
        pytest.skip("listener recovery is implemented for asyncpg")
    pytest.importorskip("asyncpg")
    dsn = os.environ.get("TEST_DATABASE_URL") or postgresql_dsn
    broker = PostgresqlBroker(
        dsn=dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
        driver="asyncpg",
        run_migrations=True,
    )
    await broker.startup()
    yield broker

    async with broker.driver, broker.driver.connection() as connection:
        await connection.execute(f"DROP TABLE {postgres_table}")
    await broker.shutdown()


@pytest.mark.parametrize("driver", ["asyncpg", "psqlpy", "psycopg"])
async def test_failure_connection_database(
    driver: Literal["asyncpg", "psqlpy", "psycopg"],
) -> None:
    """Test exception raising in connection database."""
    with pytest.raises(expected_exception=DatabaseConnectionError):
        await PostgresqlBroker(
            dsn="postgresql://postgres:postgres@localhost:5432/aaaaaaaaa",
            table_name="postgres_table",
            driver=driver,
        ).startup()


async def test_when_broker_deliver_message__then_worker_receive_message(
    broker: PostgresqlBroker,
) -> None:
    """
    Test that messages are published and read correctly.

    We kick the message, listen to the queue, and check that
    the received message matches what was sent.
    """
    valid_broker_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )

    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)

    # Send the message
    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.2)

    message = next(iter(await asyncio.gather(worker_task)))
    assert message.data == valid_broker_message.message


@pytest.mark.parametrize(
    "table_already_exists",
    [
        pytest.param(True, id="table_already_exists"),
        pytest.param(False, id="table_does_not_exist"),
    ],
)
async def test_when_startup__then_table_should_be_created(
    broker: PostgresqlBroker,
    table_already_exists: bool,
) -> None:
    """
    Test the startup process of the broker.

    We drop the messages table, restart the broker, and ensure
    that the table is recreated.
    """
    await broker.shutdown()

    if not table_already_exists:
        await broker.driver.execute(
            f"DROP TABLE IF EXISTS {broker.table_name}",
        )

    await broker.startup()

    # Verify that the table exists
    table_exists = await broker.driver.execute(
        f"SELECT * FROM {broker.table_name}",  # noqa: S608
    )
    assert table_exists == []  # Table should be empty


async def test_listen(
    broker: PostgresqlBroker,
) -> None:
    """
    Test listening to messages.

    Test that the broker can listen to messages inserted directly into the database
    and notified via the channel.
    """
    # Insert a message directly into the database
    message_content = b"test_message"
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    labels = {"label1": "label_val"}
    message_id = await broker.driver.insert(
        columns=[
            broker.columns.task_id,
            broker.columns.task_name,
            broker.columns.message,
            broker.columns.labels,
        ],
        values=[task_id, task_name, message_content, json.dumps(labels)],
        returning=[broker.columns.primary_key],
    )
    # Send a NOTIFY with the message ID
    await broker.driver.execute(
        f"NOTIFY {broker.channel_name}, '{message_id}'",
    )

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)
    assert message.data == message_content

    # Acknowledge the message
    await maybe_awaitable(message.ack())


async def test_reconciliation_is_scoped_to_message_channel(
    recovery_broker: PostgresqlBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missed notification is recovered only by its logical channel."""
    broker = recovery_broker

    original_labels = {"source": "channel-scope-test"}
    expected = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test_task",
        message=b"missed-notification",
        labels=original_labels,
    )

    async def skip_notification(_message_id: int) -> None:
        return None

    monkeypatch.setattr(broker, "_send_notification", skip_notification)
    await broker.kick(expected)

    rows = await broker.driver.select([broker.columns.labels])
    stored_labels = rows[0][broker.columns.labels.name]
    if isinstance(stored_labels, str):
        stored_labels = json.loads(stored_labels)
    assert isinstance(stored_labels, dict)
    assert stored_labels[BROKER_CHANNEL_LABEL] == broker.channel_name
    assert expected.labels == original_labels

    other_broker = PostgresqlBroker(
        dsn=broker.dsn,
        channel_name=f"{broker.channel_name}_other",
        table_name=broker.table_name,
        driver="asyncpg",
    )
    await other_broker.startup()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(get_first_task(other_broker), timeout=0.1)

        message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)
    finally:
        await other_broker.shutdown()

    assert message.data == expected.message


async def test_reconciliation_and_queued_notification_claim_once(
    recovery_broker: PostgresqlBroker,
) -> None:
    """A scanned row and its queued NOTIFY must not yield two messages."""
    broker = recovery_broker

    message_id = await broker.driver.insert(
        columns=[
            broker.columns.task_id,
            broker.columns.task_name,
            broker.columns.message,
            broker.columns.labels,
        ],
        values=[
            uuid.uuid4().hex,
            "test_task",
            b"single-delivery",
            json.dumps({BROKER_CHANNEL_LABEL: broker.channel_name}),
        ],
        returning=[broker.columns.primary_key],
    )
    await broker.driver.execute(
        f"NOTIFY {broker.channel_name}, '{message_id}'",
    )
    await asyncio.sleep(0.05)

    message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)

    assert message.data == b"single-delivery"
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(get_first_task(broker), timeout=0.1)


async def test_reconciliation_delivers_only_due_rows(
    recovery_broker: PostgresqlBroker,
) -> None:
    """Due rows are recovered without releasing future or malformed delays."""
    broker = recovery_broker

    rows = [
        (b"due-numeric", {"delay": 1.9}),
        (b"future", {"delay": 60}),
        (b"malformed", {"delay": "not-an-integer"}),
        (b"due-boolean", {"delay": False}),
    ]
    for message, labels in rows:
        await broker.driver.insert(
            columns=[
                broker.columns.task_id,
                broker.columns.task_name,
                broker.columns.message,
                broker.columns.labels,
            ],
            values=[
                uuid.uuid4().hex,
                "test_task",
                message,
                json.dumps(
                    {
                        **labels,
                        BROKER_CHANNEL_LABEL: broker.channel_name,
                    },
                ),
            ],
            returning=[broker.columns.primary_key],
        )
    legacy_message_id = await broker.driver.insert(
        columns=[
            broker.columns.task_id,
            broker.columns.task_name,
            broker.columns.message,
            broker.columns.labels,
        ],
        values=[
            uuid.uuid4().hex,
            "test_task",
            b"legacy-unmarked",
            json.dumps({}),
        ],
        returning=[broker.columns.primary_key],
    )
    await broker.driver.execute(
        f"INSERT INTO {broker.table_name} "  # noqa: S608
        "(task_id, task_name, message, labels) "
        "VALUES ($1::text::uuid, $2, $3, "
        "to_jsonb('invalid-label-shape'::text))",
        uuid.uuid4().hex,
        "test_task",
        b"scalar-labels",
    )

    await asyncio.sleep(1.1)
    iterator = broker.listen()
    recovered = [(await iterator.__anext__()).data for _ in range(2)]
    await iterator.aclose()

    assert recovered == [b"due-numeric", b"due-boolean"]
    remaining = await broker.driver.select([broker.columns.message])
    assert {row[broker.columns.message.name] for row in remaining} == {
        b"future",
        b"malformed",
        b"legacy-unmarked",
        b"scalar-labels",
    }

    await broker.driver.execute(
        f"NOTIFY {broker.channel_name}, '{legacy_message_id}'",
    )
    legacy_message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)
    assert legacy_message.data == b"legacy-unmarked"


async def test_asyncpg_listener_recovers_without_restarting_consumer(
    recovery_broker: PostgresqlBroker,
) -> None:
    """A killed asyncpg LISTEN backend must reconnect and reconcile its gap."""
    broker = recovery_broker

    old_listener = broker.listen_driver
    old_connection = old_listener.connection
    listener_pid = await old_connection.fetchval("SELECT pg_backend_pid()")
    broker._listener_retry_delay = lambda _attempt: 0.2
    consumer = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.05)

    await broker.driver.execute("SELECT pg_terminate_backend($1)", listener_pid)
    for _ in range(20):
        if old_connection.is_closed():
            break
        await asyncio.sleep(0.01)
    assert old_connection.is_closed()

    expected = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test_task",
        message=b"published-during-listener-gap",
        labels={},
    )
    await broker.kick(expected)

    received = await asyncio.wait_for(consumer, timeout=3.0)
    assert received.data == expected.message
    assert broker.listen_driver is not old_listener


async def test_wrong_format(
    broker: PostgresqlBroker,
) -> None:
    """Test that messages with incorrect formats are still received."""
    # Insert a message with missing task_id and task_name

    message_id = await broker.driver.insert(
        columns=[
            broker.columns.task_id,
            broker.columns.task_name,
            broker.columns.message,
            broker.columns.labels,
        ],
        values=[
            uuid.uuid4().hex,  # Missing task_id
            "",  # Missing task_name
            b"wrong",  # Message content
            json.dumps({}),  # Empty labels
        ],
        returning=[broker.columns.primary_key],
    )
    # Send a NOTIFY with the message ID
    await broker.driver.execute(
        f"NOTIFY {broker.channel_name}, '{message_id}'",
    )

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)
    assert message.data == b"wrong"

    # Acknowledge the message
    await maybe_awaitable(message.ack())


async def test_delayed_message(broker: PostgresqlBroker) -> None:
    """Test that delayed messages are delivered correctly after the specified delay."""
    # Send a message with a delay
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"delayed_message",
        labels={
            "delay": "1",  # Delay in seconds
        },
    )
    await broker.kick(sent)

    # Try to get the message immediately (should not be available yet)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(get_first_task(broker), timeout=1.0)

    # Wait for the delay to pass and receive the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=2.0)
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())
