import asyncio
import uuid

import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql import broker as broker_module

pytestmark = pytest.mark.anyio


async def get_first_task(broker: PostgresqlBroker) -> AckableMessage:
    """
    Get the first message from the broker's listen method.

    :param broker: Instance of PostgresqlBroker.
    :return: The first AckableMessage received.
    """
    async for message in broker.listen():
        return message
    msg = "listen() ended without yielding a message"
    raise AssertionError(msg)


def make_message(delay: str) -> BrokerMessage:
    """Build a broker message carrying a ``delay`` label."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"delayed_msg",
        labels={"delay": delay},
    )


async def test_when_delay_label_is_fractional__then_message_is_delivered_late(
    broker: PostgresqlBroker,
) -> None:
    """A fractional ``delay`` label must not raise and must hold the message back."""
    message = make_message("1.5")

    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)

    await broker.kick(message)

    # A second of margin, so a slow runner cannot see the message early.
    done, _ = await asyncio.wait({worker_task}, timeout=0.5)
    assert not done

    received = await asyncio.wait_for(worker_task, timeout=5)
    assert received.data == message.message


async def test_when_task_declares_float_delay__then_kiq_delivers_the_message(
    broker: PostgresqlBroker,
) -> None:
    """``delay=1.5`` on a task reaches the broker as the label ``"1.5"``."""

    async def handler() -> None:
        return None

    task = broker.register_task(handler, task_name=uuid.uuid4().hex, delay=1.5)

    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)

    await task.kiq()

    done, _ = await asyncio.wait({worker_task}, timeout=0.5)
    assert not done

    received = await asyncio.wait_for(worker_task, timeout=5)
    sent = broker.formatter.loads(received.data)
    assert sent.task_name == task.task_name
    assert sent.labels["delay"] == "1.5"


@pytest.mark.parametrize("delay", ["abc", "nan", "inf", "-inf", ""])
async def test_when_delay_label_is_invalid__then_kick_raises_and_stores_nothing(
    broker: PostgresqlBroker,
    delay: str,
) -> None:
    """An invalid ``delay`` must be rejected before the row is inserted."""
    with pytest.raises(ValueError) as error:
        await broker.kick(make_message(delay))

    rows = await broker.driver.execute(
        f"SELECT * FROM {broker.table_name}",  # noqa: S608
    )
    assert rows == []
    assert "'delay' label" in str(error.value)


async def test_when_delay_label_is_negative__then_message_is_delivered_at_once(
    broker: PostgresqlBroker,
) -> None:
    """A negative delay means the timer already expired."""
    message = make_message("-1")

    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)

    await broker.kick(message)

    received = await asyncio.wait_for(worker_task, timeout=2)
    assert received.data == message.message


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2.5", 2.5, id="float_string"),
        pytest.param("5.0", 5.0, id="default_retry_delay"),
        pytest.param(3, 3.0, id="int"),
        pytest.param("-1", 0.0, id="negative_clamped"),
    ],
)
def test_parse_delay__accepts_finite_numbers(value: object, expected: float) -> None:
    """Finite numbers parse to seconds, negatives clamp to zero."""
    assert broker_module._parse_delay(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        None,
        [],
        "abc",
        "nan",
        "inf",
        "",
        pytest.param(10**400, id="int_beyond_float_range"),
    ],
)
def test_parse_delay__rejects_non_delays(value: object) -> None:
    """Booleans, non-numbers and non-finite numbers are not delays."""
    with pytest.raises(ValueError, match="delay"):
        broker_module._parse_delay(value)
