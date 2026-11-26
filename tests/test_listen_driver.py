"""Integration tests for the listen drivers against a real PostgreSQL."""

import asyncio
import logging
import uuid
from contextlib import aclosing

import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql.core.sql.identifier import excerpt
from taskiq_postgresql.utils import get_db_listen_driver
from tests.helpers import NOTIFY_DELIVERS_WITHIN

pytestmark = pytest.mark.anyio

MESSAGE_ID = 42
LONG_PAYLOAD_LENGTH = 7000  # under the 8000 byte NOTIFY limit
LOG_LINE_BUDGET = 1000
# Integers no row of the broker's table can have: its id is a SERIAL, from 1
# to 2147483647. The longest is the most digits int() parses.
OUT_OF_RANGE_IDS = ("9" * 4300, "2147483648", "0", "-1")
# Far beyond any test, so only the sweep that starts listen() runs.
SWEEP_OUT_OF_THE_WAY = 60.0


async def first_message(worker: PostgresqlBroker) -> AckableMessage:
    """Return the first message ``worker.listen()`` yields, then close it."""
    async with aclosing(worker.listen()) as messages:
        return await anext(messages)


async def test_when_a_payload_is_not_an_integer__then_the_listen_driver_yields_it(
    broker: PostgresqlBroker,
    driver: str,
    postgres_table: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The broker is the only place a payload is parsed, so a driver passes
    # every one through as the str it received, and logs nothing about it.
    channel = f"{postgres_table}_unparsed"
    listener = get_db_listen_driver(driver)(  # type: ignore[arg-type]
        broker.dsn,
        channel,
    )
    await listener.on_startup()
    sent = ["not-a-number", str(MESSAGE_ID)]
    received = []
    try:
        await asyncio.sleep(0.2)  # let the listener subscribe before notifying
        for payload in sent:
            # psqlpy cannot map the void result of pg_notify, hence the cast.
            await broker.driver.execute(
                "SELECT pg_notify($1, $2)::text",
                channel,
                payload,
            )

        async with aclosing(listener.__aiter__()) as notifications:
            with caplog.at_level(logging.WARNING):
                for _ in sent:
                    received.append(
                        await asyncio.wait_for(anext(notifications), timeout=5),
                    )
    finally:
        await listener.on_shutdown()

    assert received == sent
    assert all(type(payload) is str for payload in received)
    assert "Invalid NOTIFY payload" not in caplog.text


async def test_when_payload_is_long_and_not_an_integer__then_log_line_is_bounded(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=broker.channel_name,
        table_name=broker.table_name,
        driver=driver,
        poll_interval=SWEEP_OUT_OF_THE_WAY,
    )
    message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"after-long-payload",
        labels={},
    )
    await worker.startup()
    try:
        worker_task = asyncio.create_task(first_message(worker))
        # The sweep that starts listen() finds nothing; then it reads LISTEN.
        await asyncio.sleep(0.2)
        # psqlpy cannot map the void result of pg_notify, hence the cast.
        await broker.driver.execute(
            "SELECT pg_notify($1, $2)::text",
            broker.channel_name,
            "x" * LONG_PAYLOAD_LENGTH,
        )
        await broker.kick(message)

        received = await asyncio.wait_for(worker_task, timeout=NOTIFY_DELIVERS_WITHIN)
    finally:
        await worker.shutdown()

    assert received.data == message.message
    invalid = [r for r in caplog.records if "Invalid NOTIFY payload" in r.getMessage()]
    assert len(invalid) == 1
    logged = invalid[0].getMessage()
    assert len(logged) < LOG_LINE_BUDGET
    assert "truncated" in logged
    assert str(LONG_PAYLOAD_LENGTH) in logged


async def test_when_payload_is_an_integer_out_of_the_id_range__then_it_is_skipped(
    broker: PostgresqlBroker,
    postgresql_dsn: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Claiming such an id fails on the server, or matches nothing, so it is
    # skipped like any other invalid payload: one bounded line, no traceback.
    worker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=broker.channel_name,
        table_name=broker.table_name,
        driver=driver,
        poll_interval=SWEEP_OUT_OF_THE_WAY,
    )
    message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"after-out-of-range-ids",
        labels={},
    )
    await worker.startup()
    try:
        worker_task = asyncio.create_task(first_message(worker))
        # The sweep that starts listen() finds nothing; then it reads LISTEN.
        await asyncio.sleep(0.2)
        for payload in OUT_OF_RANGE_IDS:
            # psqlpy cannot map the void result of pg_notify, hence the cast.
            await broker.driver.execute(
                "SELECT pg_notify($1, $2)::text",
                broker.channel_name,
                payload,
            )
        await broker.kick(message)

        received = await asyncio.wait_for(worker_task, timeout=NOTIFY_DELIVERS_WITHIN)
    finally:
        await worker.shutdown()

    assert received.data == message.message
    broker_records = [r for r in caplog.records if r.name == "taskiq.asyncpg_broker"]
    assert [r.getMessage() for r in broker_records] == [
        f"Invalid NOTIFY payload {excerpt(payload)} on channel {broker.channel_name}"
        for payload in OUT_OF_RANGE_IDS
    ]
    assert all(record.exc_info is None for record in broker_records)
    assert all(len(record.getMessage()) < LOG_LINE_BUDGET for record in broker_records)


async def test_when_on_startup_returns__then_a_notify_sent_at_once_is_read(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    """on_startup() returns once LISTEN is in effect, before anything reads."""
    psycopg = pytest.importorskip("psycopg")
    channel = f"{postgres_table}_at_once"
    listener = get_db_listen_driver(driver)(  # type: ignore[arg-type]
        postgresql_dsn,
        channel,
    )
    await listener.on_startup()
    try:
        # Blocking on purpose: the event loop runs nothing of the listener's
        # between on_startup() and this NOTIFY.
        with psycopg.connect(postgresql_dsn, autocommit=True) as sender:
            sender.execute("SELECT pg_notify(%s, %s)", (channel, "at-once"))
        async with aclosing(listener.__aiter__()) as notifications:
            received = await asyncio.wait_for(
                anext(notifications),
                timeout=NOTIFY_DELIVERS_WITHIN,
            )
    finally:
        await listener.on_shutdown()

    assert received == "at-once"
