import asyncio
import json
import logging
import uuid
from typing import Union

import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql.exceptions import DatabaseConnectionError

pytestmark = pytest.mark.anyio

# How late a 0.05 s sleep may wake up while listen() runs beside it.
RESPONSIVE_WITHIN = 0.5
# How long a NOTIFY goes out every 0.05 s while a delayed message waits.
NOTIFY_FLOOD_SECONDS = 3
# A poll_interval no test waits for, so a message received came by NOTIFY.
SWEEP_OUT_OF_THE_WAY = 60.0
# How long a listening worker may take to claim a message it is notified of.
NOTIFY_DELIVERS_WITHIN = 3
# A poll_interval short enough for many retries within a two-second test.
FAST_SWEEP = 0.2
# Sleeps of 0.05 s in a row beside listen(): two seconds of them.
TICKS = 40


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


@pytest.mark.parametrize(
    "argument",
    ["poll_interval", "health_check_interval", "query_timeout"],
)
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0, id="zero"),
        pytest.param(-1, id="negative"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="inf"),
        pytest.param(True, id="bool"),
        pytest.param("1", id="str"),
    ],
)
def test_when_a_duration_is_not_a_positive_number__then_broker_refuses_it(
    argument: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=argument):
        PostgresqlBroker(dsn="postgresql://localhost/db", **{argument: value})


def test_poll_interval_defaults_to_one_second() -> None:
    quarter_second = 0.25

    assert PostgresqlBroker(dsn="postgresql://localhost/db").poll_interval == 1.0
    assert (
        PostgresqlBroker(dsn="postgresql://localhost/db", poll_interval=quarter_second)
    ).poll_interval == quarter_second


def test_when_no_recovery_setting_is_given__then_the_drivers_get_30_seconds() -> None:
    broker = PostgresqlBroker(dsn="postgresql://localhost/db")
    half_minute = 30.0

    assert broker.driver.query_timeout == half_minute
    assert broker.listen_driver.health_check_interval == half_minute
    assert broker.listen_driver.query_timeout == half_minute


async def test_failure_connection_database(
    driver: str,
    missing_database_dsn: str,
) -> None:
    """Test exception raising in connection database."""
    with pytest.raises(expected_exception=DatabaseConnectionError):
        await PostgresqlBroker(
            dsn=missing_database_dsn,
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


def immediate_message(payload: bytes) -> BrokerMessage:
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=payload,
        labels={},
    )


async def insert_due_later(
    broker: PostgresqlBroker, payload: bytes, delay: float
) -> None:
    """Insert a row of the broker's channel without any NOTIFY."""
    await broker.driver.insert(
        [
            broker.columns.task_id,
            broker.columns.task_name,
            broker.columns.message,
            broker.columns.labels,
            broker.columns.channel,
            broker.columns.not_before,
        ],
        [uuid.uuid4().hex, "sweep_test", payload, "{}", broker.channel_name, delay],
        [broker.columns.primary_key],
    )


async def test_when_the_listen_connection_is_closed__then_the_sweep_still_delivers(
    broker: PostgresqlBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without LISTEN the worker keeps delivering, warns once and does not spin."""
    loop = asyncio.get_running_loop()
    message = immediate_message(b"polled")
    # listen() reopens it; the fixture's teardown closes the reopened one.
    await broker.listen_driver.on_shutdown()
    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)
    await broker.kick(message)

    # A listen() spinning without awaiting would hold this sleep back.
    slept_from = loop.time()
    await asyncio.sleep(0.05)
    assert loop.time() - slept_from < RESPONSIVE_WITHIN

    received = await asyncio.wait_for(
        worker_task,
        timeout=broker.poll_interval + 3,
    )

    assert received.data == message.message
    polling_warnings = [
        record
        for record in caplog.records
        if record.levelname == "WARNING" and "polling" in record.getMessage()
    ]
    assert len(polling_warnings) == 1


async def test_when_a_notify_is_not_a_message_id__then_listen_warns_and_keeps_listening(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An invalid payload is skipped with a warning; the next NOTIFY still delivers."""
    worker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=broker.channel_name,
        table_name=broker.table_name,
        driver=driver,
        poll_interval=SWEEP_OUT_OF_THE_WAY,
    )
    message = immediate_message(b"after-invalid")
    await worker.startup()
    try:
        worker_task = asyncio.create_task(get_first_task(worker))
        # The sweep that starts listen() finds nothing; then it reads LISTEN.
        await asyncio.sleep(0.2)
        await broker.driver.execute(f"NOTIFY {broker.channel_name}, 'abc'")
        await broker.kick(message)

        received = await asyncio.wait_for(worker_task, timeout=NOTIFY_DELIVERS_WITHIN)
    finally:
        await worker.shutdown()

    assert received.data == message.message
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelname == "WARNING"
    ]
    assert [text for text in warnings if "NOTIFY payload" in text] == [
        f"Invalid NOTIFY payload 'abc' on channel {broker.channel_name}",
    ]
    assert [text for text in warnings if "LISTEN on channel" in text] == []


async def test_when_the_server_ends_the_listen_connection__then_listen_backs_off(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Once LISTEN dies, listen() warns once, does not spin and still delivers."""
    loop = asyncio.get_running_loop()
    # A channel of its own, so only this worker's LISTEN backend is ended.
    channel = f"{broker.channel_name}_ended"
    worker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=channel,
        table_name=broker.table_name,
        driver=driver,
        poll_interval=FAST_SWEEP,
    )
    message = immediate_message(b"after-terminate")
    received: list[bytes] = []

    async def collect() -> None:
        async for delivered in worker.listen():
            received.append(delivered.data)

    async def slowest_tick() -> float:
        slowest = 0.0
        for _ in range(TICKS):
            started = loop.time()
            await asyncio.sleep(0.05)
            slowest = max(slowest, loop.time() - started)
        return slowest

    await worker.startup()
    listening = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.3)
        logged_before = len(caplog.records)
        terminated_at = loop.time()
        terminated = await broker.driver.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid() AND query LIKE $1",
            f"%LISTEN%{channel}%",
        )
        ticking = asyncio.create_task(slowest_tick())
        # Long enough for LISTEN to be reopened, about half a second after it
        # died: the message comes by NOTIFY or by a sweep, and either way the
        # loop must neither spin nor flood the log meanwhile.
        await asyncio.sleep(0.5)
        await worker.kick(message)
        slowest = await asyncio.wait_for(ticking, timeout=TICKS * 0.05 + 3)
        elapsed = loop.time() - terminated_at
        logged = [
            record
            for record in caplog.records[logged_before:]
            if record.levelno >= logging.WARNING
        ]
    finally:
        listening.cancel()
        await asyncio.gather(listening, return_exceptions=True)
        await worker.shutdown()

    assert len(terminated) >= 1
    assert slowest < RESPONSIVE_WITHIN
    # A spinning retry logs on every turn; one that waits a sweep logs at
    # most once per poll_interval.
    assert len(logged) <= 2 + elapsed / worker.poll_interval
    listen_stopped = [
        record.getMessage()
        for record in logged
        if "LISTEN on channel" in record.getMessage()
    ]
    assert len(listen_stopped) == 1
    assert received == [message.message]


async def test_when_notifies_never_stop__then_the_sweep_still_runs_on_time(
    broker: PostgresqlBroker,
) -> None:
    """A NOTIFY every 0.05 s does not postpone the sweep for a delayed message."""
    loop = asyncio.get_running_loop()
    arrivals: dict[bytes, float] = {}

    async def collect() -> None:
        async for message in broker.listen():
            arrivals.setdefault(message.data, loop.time())

    worker = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.2)
        kicked_at = loop.time()
        await broker.kick(
            BrokerMessage(
                task_id=uuid.uuid4().hex,
                task_name=uuid.uuid4().hex,
                message=b"delayed",
                labels={"delay": "1"},
            ),
        )
        while loop.time() - kicked_at < NOTIFY_FLOOD_SECONDS:
            await broker.kick(immediate_message(b"immediate"))
            await asyncio.sleep(0.05)
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    # Due after 1 s, swept at most one poll_interval (1 s) later, plus 1 s.
    assert arrivals[b"delayed"] - kicked_at <= 1 + 1 + 1


async def test_when_listen_is_closed__then_it_leaves_no_task_behind(
    broker: PostgresqlBroker,
) -> None:
    """aclose() also ends the read of the LISTEN connection that was pending."""
    await broker.driver.execute("SELECT 1")
    before = asyncio.all_tasks()

    listening = broker.listen()
    # The start sweep finds nothing, so listen() reads LISTEN until the next
    # sweep yields this row; that read is still pending at the yield.
    await insert_due_later(broker, b"swept", 0.5)
    message = await asyncio.wait_for(anext(listening), timeout=broker.poll_interval + 3)
    await listening.aclose()

    assert message.data == b"swept"
    assert asyncio.all_tasks() == before


async def test_when_broker_shuts_down__then_listen_ends_without_reconnecting(
    broker: PostgresqlBroker,
) -> None:
    """A listen() still running when shutdown() starts ends at its next turn."""
    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)

    await broker.shutdown()
    try:
        ended_with = await asyncio.wait_for(
            worker_task,
            timeout=broker.poll_interval + 2,
        )
        pool_after_listen = broker.driver.pool
    finally:
        # The fixture's teardown needs a started broker.
        await broker.startup()

    assert ended_with == b""
    assert pool_after_listen is None


async def test_when_a_claim_fails__then_listen_logs_it_and_retries_at_the_next_sweep(
    broker: PostgresqlBroker,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed claim is logged and tried again one poll_interval later."""
    loop = asyncio.get_running_loop()
    real_fetch_row = broker.driver.fetch_row
    failures: list[int] = []

    async def claim_failing_once(query: str, *values: object) -> object:
        if not failures:
            failures.append(1)
            msg = "simulated claim failure"
            raise ConnectionError(msg)
        return await real_fetch_row(query, *values)

    monkeypatch.setattr(broker.driver, "fetch_row", claim_failing_once)
    await insert_due_later(broker, b"retried", 0.0)

    started = loop.time()
    received = await asyncio.wait_for(
        get_first_task(broker),
        timeout=broker.poll_interval + 2,
    )
    elapsed = loop.time() - started

    assert received.data == b"retried"
    # Retried at the next sweep, not at once.
    assert elapsed >= broker.poll_interval * 0.9
    logged = [
        str(record.exc_info[1])
        for record in caplog.records
        if record.levelname == "ERROR" and record.exc_info is not None
    ]
    assert logged == ["simulated claim failure"]


async def collect_for(broker: PostgresqlBroker, seconds: float) -> list[bytes]:
    """Return the payload of every message ``broker.listen()`` yields in ``seconds``."""
    received: list[bytes] = []

    async def collect() -> None:
        async for message in broker.listen():
            received.append(message.data)

    listening = asyncio.create_task(collect())
    await asyncio.sleep(seconds)
    listening.cancel()
    await asyncio.gather(listening, return_exceptions=True)
    return received


async def test_when_the_notify_fails_after_the_insert__then_kick_returns(
    broker: PostgresqlBroker,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The row is committed, so raising would make a retrying caller run it twice."""
    real_execute = broker.driver.execute
    notifies: list[str] = []

    async def execute_failing_notify(query: str, *values: object) -> object:
        if query.startswith("NOTIFY"):
            notifies.append(query)
            msg = "simulated NOTIFY failure"
            raise ConnectionError(msg)
        return await real_execute(query, *values)

    monkeypatch.setattr(broker.driver, "execute", execute_failing_notify)
    message = immediate_message(b"not-notified")

    await broker.kick(message)
    # The sweep that starts listen(), then one more poll_interval for a
    # duplicate to show up.
    received = await collect_for(broker, broker.poll_interval + 0.5)

    assert len(notifies) == 1
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "next sweep" in warnings[0]
    assert received == [message.message]


async def test_when_the_insert_fails__then_kick_raises_and_sends_nothing_more(
    broker: PostgresqlBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A driver error on the INSERT reaches the caller; the library does not resend."""
    real_insert = broker.driver.insert
    real_execute = broker.driver.execute
    inserts: list[int] = []
    notifies: list[str] = []

    async def counting_insert(*args: object, **kwargs: object) -> object:
        inserts.append(1)
        return await real_insert(*args, **kwargs)  # type: ignore[arg-type]

    async def recording_execute(query: str, *values: object) -> object:
        if query.startswith("NOTIFY"):
            notifies.append(query)
        return await real_execute(query, *values)

    # A real error from the server: the table of a 0.4.x release.
    await broker.driver.execute(f"ALTER TABLE {broker.table_name} DROP COLUMN channel")
    monkeypatch.setattr(broker.driver, "insert", counting_insert)
    monkeypatch.setattr(broker.driver, "execute", recording_execute)

    with pytest.raises(Exception, match='column "channel"'):
        await broker.kick(immediate_message(b"never-stored"))

    assert inserts == [1]
    assert notifies == []


async def test_when_the_task_reading_listen_is_cancelled__then_no_task_is_left(
    broker: PostgresqlBroker,
) -> None:
    """Cancelling the consumer of listen() also ends every task listen() started."""
    await broker.driver.execute("SELECT 1")
    before = asyncio.all_tasks()

    consumer = asyncio.create_task(get_first_task(broker))
    # Past the sweep that starts listen(), so it waits for a NOTIFY.
    await asyncio.sleep(0.3)
    consumer.cancel()
    await asyncio.gather(consumer, return_exceptions=True)

    assert consumer.cancelled()
    assert asyncio.all_tasks() == before


async def test_when_the_broker_starts_again__then_a_new_listen_delivers(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
) -> None:
    """After shutdown() and startup(), listen() delivers by sweep and by NOTIFY."""
    worker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=broker.channel_name,
        table_name=broker.table_name,
        driver=driver,
        poll_interval=SWEEP_OUT_OF_THE_WAY,
    )
    await worker.startup()
    first = asyncio.create_task(get_first_task(worker))
    await asyncio.sleep(0.2)
    await worker.shutdown()
    # Well under poll_interval: shutdown() wakes the waiting listen() up.
    ended_with = await asyncio.wait_for(first, timeout=NOTIFY_DELIVERS_WITHIN)

    await worker.startup()
    try:
        # Stored without a NOTIFY: only the sweep that starts listen() finds it.
        await insert_due_later(broker, b"swept", 0.0)
        listening = worker.listen()
        swept = await asyncio.wait_for(anext(listening), NOTIFY_DELIVERS_WITHIN)
        # The next sweep is a minute away, so only the NOTIFY delivers this.
        await broker.kick(immediate_message(b"notified"))
        notified = await asyncio.wait_for(anext(listening), NOTIFY_DELIVERS_WITHIN)
        await listening.aclose()
    finally:
        await worker.shutdown()

    assert ended_with == b""
    assert (swept.data, notified.data) == (b"swept", b"notified")
