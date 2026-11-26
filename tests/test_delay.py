import asyncio
import functools
import types
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Optional

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql import broker as broker_module
from taskiq_postgresql.abc import query as sql_builders
from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from tests.helpers import NOTIFY_DELIVERS_WITHIN

pytestmark = pytest.mark.anyio


# A string, so that this module still imports on a release without
# ClaimColumns and its tests fail there one by one instead of the collection.
def claim_columns() -> "sql_builders.ClaimColumns":
    """The claim columns the broker declares, built from the query module."""
    return sql_builders.ClaimColumns(
        channel=Column("channel", "VARCHAR", nullable=True),
        not_before=sql_builders.NotBeforeColumn(),
    )


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


def run_producer(
    dsn: str,
    table_name: str,
    channel_name: str,
    driver: str,
    message: BrokerMessage,
) -> None:
    """Kick ``message`` from a broker on an event loop that ends right after."""

    async def produce() -> None:
        producer = PostgresqlBroker(
            dsn=dsn,
            channel_name=channel_name,
            table_name=table_name,
            driver=driver,
        )
        await producer.startup()
        try:
            await producer.kick(message)
        finally:
            await producer.shutdown()

    asyncio.run(produce())


async def test_when_producer_exits_before_the_delay__then_another_worker_delivers_it(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
) -> None:
    """A delayed message must outlive the process that sent it."""
    message = make_message("1")

    # asyncio.run closes the producer's loop and drops every timer still
    # scheduled on it, as the exit of the producer's process would.
    await asyncio.to_thread(
        run_producer,
        postgresql_dsn,
        broker.table_name,
        broker.channel_name,
        driver,
        message,
    )

    received = await asyncio.wait_for(get_first_task(broker), timeout=6.0)
    assert received.data == message.message


@asynccontextmanager
async def started_broker(
    dsn: str,
    table_name: str,
    channel_name: str,
    driver: str,
    poll_interval: float = 1.0,
) -> AsyncIterator[PostgresqlBroker]:
    """Start one more broker on ``table_name``, without migrations."""
    other = PostgresqlBroker(
        dsn=dsn,
        channel_name=channel_name,
        table_name=table_name,
        driver=driver,
        poll_interval=poll_interval,
    )
    await other.startup()
    try:
        yield other
    finally:
        await other.shutdown()


async def collect(broker: PostgresqlBroker, received: list[bytes]) -> None:
    """Append the payload of every message ``broker.listen()`` yields."""
    async for message in broker.listen():
        received.append(message.data)


async def stop(tasks: list["asyncio.Task[None]"]) -> None:
    """Cancel ``tasks`` and wait until they end."""
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def plain_message(
    payload: bytes, labels: Optional[dict[str, str]] = None
) -> BrokerMessage:
    """Build a broker message with ``payload`` and no ``delay`` unless given."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=payload,
        labels=labels or {},
    )


async def row_ids(broker: PostgresqlBroker) -> list[int]:
    """Return the ids of the rows in the broker's table, in order."""
    rows = await broker.driver.execute(
        f"SELECT id FROM {broker.table_name} ORDER BY id",  # noqa: S608
    )
    return [row[0] for row in as_tuples(rows)]


async def test_when_a_delayed_message_is_announced_early__then_it_waits_until_due(
    broker: PostgresqlBroker,
) -> None:
    """Neither the sweep nor a NOTIFY of its id delivers a message before it is due."""
    loop = asyncio.get_running_loop()
    message = make_message("2")
    worker_task = asyncio.create_task(get_first_task(broker))
    await asyncio.sleep(0.2)

    kicked_at = loop.time()
    await broker.kick(message)
    (row_id,) = await row_ids(broker)
    await broker.driver.execute(f"NOTIFY {broker.channel_name}, '{row_id}'")

    done, _ = await asyncio.wait({worker_task}, timeout=1.5)
    assert not done

    # Due after 2 s, then at most one poll_interval (1 s) and a margin.
    received = await asyncio.wait_for(
        worker_task,
        timeout=kicked_at + 2 + 1 + 3 - loop.time(),
    )
    assert received.data == message.message


async def test_when_two_workers_listen__then_each_message_is_delivered_once(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
) -> None:
    """Immediate and delayed messages reach exactly one of two workers."""
    loop = asyncio.get_running_loop()
    received: list[bytes] = []
    sent = [
        plain_message(f"m{number}".encode(), {"delay": "0.5"} if number % 2 else {})
        for number in range(20)
    ]
    async with started_broker(
        postgresql_dsn,
        broker.table_name,
        broker.channel_name,
        driver,
    ) as other:
        workers = [
            asyncio.create_task(collect(worker, received)) for worker in (broker, other)
        ]
        try:
            await asyncio.sleep(0.2)
            for message in sent:
                await broker.kick(message)

            deadline = loop.time() + 10
            while len(received) < len(sent) and loop.time() < deadline:
                await asyncio.sleep(0.05)
            # One more poll_interval and a margin, for a duplicate to show up.
            await asyncio.sleep(1.5)
        finally:
            await stop(workers)

    assert sorted(received) == sorted(message.message for message in sent)


async def test_when_nobody_listens__then_listen_delivers_the_waiting_message(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
) -> None:
    """A message whose NOTIFY reached no worker goes out when a worker starts."""
    message = plain_message(b"unheard")
    await broker.kick(message)

    async with started_broker(
        postgresql_dsn,
        broker.table_name,
        broker.channel_name,
        driver,
    ) as worker:
        # Under poll_interval (1 s): only the sweep that starts listen() runs.
        received = await asyncio.wait_for(get_first_task(worker), timeout=0.9)

    assert received.data == message.message


async def test_when_two_channels_share_a_table__then_each_worker_gets_its_own(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
) -> None:
    """Neither the sweep nor a NOTIFY of its id moves a message to another channel."""
    mine = plain_message(b"mine")
    theirs = plain_message(b"theirs")
    mine_received: list[bytes] = []
    their_received: list[bytes] = []
    async with started_broker(
        postgresql_dsn,
        broker.table_name,
        f"{broker.channel_name}_other",
        driver,
    ) as other:
        # Due at once, while no worker of its channel listens.
        await broker.kick(mine)
        (mine_id,) = await row_ids(broker)
        workers = [asyncio.create_task(collect(other, their_received))]
        try:
            await asyncio.sleep(0.2)
            await broker.driver.execute(f"NOTIFY {other.channel_name}, '{mine_id}'")
            # Over two poll_intervals with only the other channel's worker
            # running, so its sweeps passed over the due row more than once.
            await asyncio.sleep(2.5)
            their_received_alone = list(their_received)
            rows_left_alone = await row_ids(broker)

            await other.kick(theirs)
            workers.append(asyncio.create_task(collect(broker, mine_received)))
            # Over one poll_interval, so both workers swept the table again.
            await asyncio.sleep(1.5)
        finally:
            await stop(workers)

    assert their_received_alone == []
    assert rows_left_alone == [mine_id]
    assert mine_received == [mine.message]
    assert their_received == [theirs.message]


async def test_when_a_row_has_no_channel__then_only_a_notify_of_its_id_delivers_it(
    broker: PostgresqlBroker,
) -> None:
    """The sweep leaves a 0.4.x row alone; a NOTIFY of its id still delivers it."""
    row_id = await insert_row(broker, b"legacy", channel=None)
    worker_task = asyncio.create_task(get_first_task(broker))

    # Over one poll_interval, so the sweep ran and left the row.
    done, _ = await asyncio.wait({worker_task}, timeout=1.5)
    assert not done

    await broker.driver.execute(f"NOTIFY {broker.channel_name}, '{row_id}'")
    received = await asyncio.wait_for(worker_task, timeout=3)
    assert received.data == b"legacy"


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


def as_tuples(rows: Any) -> list[tuple[Any, ...]]:
    """Rows of ``QueryDriver.execute`` as tuples: psqlpy returns dicts."""
    return [
        tuple(row.values()) if isinstance(row, dict) else tuple(row) for row in rows
    ]


@pytest.mark.parametrize(
    ("labels", "expected_delay"),
    [
        pytest.param({}, 0.0, id="no_delay"),
        pytest.param({"delay": "0"}, 0.0, id="zero"),
        pytest.param({"delay": "-1"}, 0.0, id="negative"),
        pytest.param({"delay": "2.5"}, 2.5, id="fractional"),
    ],
)
async def test_when_kicked__then_the_row_holds_its_channel_and_server_due_time(
    broker: PostgresqlBroker,
    labels: dict[str, str],
    expected_delay: float,
) -> None:
    """``not_before`` is the insert's own ``NOW()`` plus the delay, exactly."""
    await broker.kick(
        BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name=uuid.uuid4().hex,
            message=b"msg",
            labels=labels,
        ),
    )

    rows = await broker.driver.execute(
        "SELECT channel, "  # noqa: S608
        "EXTRACT(EPOCH FROM not_before - created_at)::double precision "
        f"FROM {broker.table_name}",
    )
    assert as_tuples(rows) == [(broker.channel_name, expected_delay)]


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


# A poll_interval no test waits for, so a message received came by NOTIFY.
SWEEP_OUT_OF_THE_WAY = 60.0


@pytest.mark.parametrize("delay", ["0", "-1"])
async def test_when_delay_is_zero_or_negative__then_its_notify_delivers_it(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
    delay: str,
) -> None:
    """A message due at once goes out by its NOTIFY, long before the next sweep."""
    loop = asyncio.get_running_loop()
    message = make_message(delay)
    async with started_broker(
        postgresql_dsn,
        broker.table_name,
        broker.channel_name,
        driver,
        poll_interval=SWEEP_OUT_OF_THE_WAY,
    ) as worker:
        worker_task = asyncio.create_task(get_first_task(worker))
        # The sweep that starts listen() finds nothing; the next is a minute away.
        await asyncio.sleep(0.2)
        kicked_at = loop.time()
        await broker.kick(message)
        received = await asyncio.wait_for(worker_task, timeout=5)
        delivered_after = loop.time() - kicked_at

    assert received.data == message.message
    assert delivered_after < NOTIFY_DELIVERS_WITHIN


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


def test_column_placeholder__is_the_bind_parameter() -> None:
    """A plain column takes its value as is."""
    assert Column("task_name", "VARCHAR").placeholder(3) == "$3"


def test_insert_query__computes_not_before_on_the_server() -> None:
    """The bound value of ``not_before`` is a delay added to the server's clock."""
    columns = [
        Column("task_id", "UUID"),
        Column("task_name", "VARCHAR"),
        Column("message", "BYTEA"),
        Column("labels", "JSONB"),
        *claim_columns(),
    ]

    query = sql_builders.InsertQuery("jobs").make_query(
        columns,
        [PrimaryKeyColumn("id", "SERIAL")],
    )

    assert query == (
        "INSERT INTO jobs (task_id, task_name, message, labels, channel, not_before) "
        "VALUES ($1, $2, $3, $4, $5, "
        "NOW() + make_interval(secs => $6::double precision)) "
        "RETURNING id"
    )


def test_claim_query__sweep_takes_the_oldest_due_row_of_the_channel() -> None:
    """The sweep skips rows another claim holds instead of waiting for them."""
    query = sql_builders.ClaimDueQuery("jobs").make_query(
        PrimaryKeyColumn("id", "SERIAL"),
        claim_columns(),
        [Column("message", "BYTEA")],
    )

    assert query == (
        "DELETE FROM jobs WHERE id = ("
        "SELECT id FROM jobs WHERE channel = $1 AND not_before <= NOW() "
        "ORDER BY not_before, id LIMIT 1 FOR UPDATE SKIP LOCKED"
        ") RETURNING message"
    )


def test_claim_query__by_id_takes_a_due_row_of_the_channel_or_without_one() -> None:
    """A notified id is claimed only once due, and never from another channel."""
    query = sql_builders.ClaimByIdQuery("jobs").make_query(
        PrimaryKeyColumn("id", "SERIAL"),
        claim_columns(),
        [Column("message", "BYTEA")],
    )

    assert query == (
        "DELETE FROM jobs WHERE id = $2 AND not_before <= NOW() "
        "AND (channel = $1 OR channel IS NULL) RETURNING message"
    )


async def insert_row(
    broker: PostgresqlBroker,
    message: bytes,
    *,
    channel: Optional[str],
    delay: float = 0.0,
) -> int:
    """
    Insert a row straight through the driver and return its id.

    Without a channel the row is inserted as 0.4.x did, so it gets the column
    defaults: no channel, due at once.
    """
    columns = [
        broker.columns.task_id,
        broker.columns.task_name,
        broker.columns.message,
        broker.columns.labels,
    ]
    values: list[object] = [uuid.uuid4().hex, "claim_test", message, "{}"]
    if channel is not None:
        columns += [broker.columns.channel, broker.columns.not_before]
        values += [channel, delay]
    return await broker.driver.insert(columns, values, [broker.columns.primary_key])


def data_of(message: Optional[AckableMessage]) -> Optional[bytes]:
    """The payload of a claimed message, or None when nothing was claimed."""
    return None if message is None else message.data


async def test_broker_claim__takes_due_rows_of_its_channel_only(
    broker: PostgresqlBroker,
) -> None:
    """The sweep takes due rows of the channel, oldest first; by id adds legacy."""
    mine = broker.channel_name
    other_due = await insert_row(broker, b"other-due", channel=f"{mine}_other")
    mine_future = await insert_row(broker, b"mine-future", channel=mine, delay=60)
    await insert_row(broker, b"mine-due-late", channel=mine)
    await insert_row(broker, b"mine-due-early", channel=mine, delay=-5)
    legacy = await insert_row(broker, b"legacy", channel=None)

    swept = [data_of(await broker._claim_due()) for _ in range(3)]
    by_id = {
        name: data_of(await broker._claim_notified(row_id))
        for name, row_id in [
            ("other_channel", other_due),
            ("future", mine_future),
            ("legacy", legacy),
        ]
    }

    assert swept == [b"mine-due-early", b"mine-due-late", None]
    assert by_id == {
        "other_channel": None,
        "future": None,
        "legacy": b"legacy",
    }


async def test_when_another_transaction_holds_a_due_row__then_the_sweep_skips_it(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
) -> None:
    """A row locked elsewhere neither blocks the sweep nor reaches it."""
    # The oldest due row, so a sweep that waited for locks would stop on it.
    held = await insert_row(broker, b"held", channel=broker.channel_name, delay=-5)
    await insert_row(broker, b"free", channel=broker.channel_name)
    holder = await asyncpg.connect(postgresql_dsn)
    try:
        async with holder.transaction():
            await holder.execute(
                f"SELECT id FROM {broker.table_name} WHERE id = $1 FOR UPDATE",  # noqa: S608
                held,
            )
            # Under poll_interval (1 s): only the sweep that starts listen() runs.
            received = await asyncio.wait_for(get_first_task(broker), timeout=0.9)
            # Only the held row is left: the sweep returns at once, empty.
            left = await asyncio.wait_for(broker._claim_due(), timeout=0.9)
    finally:
        await holder.close()

    assert received.data == b"free"
    assert left is None
    assert await row_ids(broker) == [held]


def callback_module(callback: Callable[..., Any]) -> str:
    """
    Return the module a timer callback comes from.

    It looks through ``functools.partial`` and, for a bound method, at the
    class of the object it is bound to, so that a timer of this package
    cannot pass for one of ``functools`` or ``builtins``.
    """
    while isinstance(callback, functools.partial):
        callback = callback.func
    owner = getattr(callback, "__self__", None)
    if owner is not None and not isinstance(owner, types.ModuleType):
        return type(owner).__module__
    return getattr(callback, "__module__", None) or ""


def test_callback_module__sees_through_partials_and_bound_methods() -> None:
    """A wrapped or bound callback of the broker module is still the broker's."""
    broker = PostgresqlBroker(dsn="unused")
    broker_callbacks = [
        broker_module._ack,
        functools.partial(broker_module._ack),
        functools.partial(functools.partial(broker.kick, None)),
        broker.kick,
        functools.partial(broker.columns.not_before.placeholder, 1),
    ]

    assert [callback_module(callback) for callback in broker_callbacks] == [
        "taskiq_postgresql.broker",
        "taskiq_postgresql.broker",
        "taskiq_postgresql.broker",
        "taskiq_postgresql.broker",
        "taskiq_postgresql.core.sql.catalog.columns",
    ]
    # A callback of asyncio or of the builtins stays theirs.
    assert callback_module(print) == "builtins"
    assert callback_module(asyncio.Queue().put_nowait) == "asyncio.queues"


async def test_when_kicked_with_a_delay__then_the_producer_keeps_no_timer_or_notify(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The producer schedules nothing and announces nothing; the sweep delivers."""
    loop = asyncio.get_running_loop()
    message = make_message("1")
    notified: list[str] = []
    listener = await asyncpg.connect(postgresql_dsn)
    try:
        await listener.add_listener(
            broker.channel_name,
            lambda *args: notified.append(args[-1]),
        )

        # Every timer kick() schedules, by the module of its callback. The
        # drivers schedule their own (asyncpg on releasing a pooled
        # connection), so the timers are recorded instead of refused.
        timers: list[str] = []

        def recording(schedule: Callable[..., Any]) -> Callable[..., Any]:
            def record(
                when: float,
                callback: Callable[..., Any],
                *args: Any,
                **kwargs: Any,
            ) -> Any:
                timers.append(callback_module(callback))
                return schedule(when, callback, *args, **kwargs)

            return record

        tasks_before = asyncio.all_tasks()
        with monkeypatch.context() as patched:
            patched.setattr(loop, "call_at", recording(loop.call_at))
            patched.setattr(loop, "call_later", recording(loop.call_later))
            await broker.kick(message)
        new_tasks = asyncio.all_tasks() - tasks_before

        await asyncio.sleep(0.5)
    finally:
        await listener.close()

    assert [module for module in timers if module.startswith("taskiq_postgresql")] == []
    assert new_tasks == set()
    assert notified == []
    # Due 1 s after the kick, swept at most one poll_interval (1 s) later.
    received = await asyncio.wait_for(get_first_task(broker), timeout=1 + 1 + 2)
    assert received.data == message.message
