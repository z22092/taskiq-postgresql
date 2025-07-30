import asyncio
import json
import uuid
from typing import Literal, Union

import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_postgresql import PostgresqlBroker
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
