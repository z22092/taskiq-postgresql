"""Integration tests for the listen drivers against a real PostgreSQL."""

import asyncio
import logging
from contextlib import aclosing

import pytest

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql.utils import get_db_listen_driver

pytestmark = pytest.mark.anyio

MESSAGE_ID = 42
LONG_PAYLOAD_LENGTH = 7000  # under the 8000 byte NOTIFY limit
LOG_LINE_BUDGET = 1000


async def test_when_psycopg_payload_is_not_an_integer__then_it_is_skipped(
    postgresql_dsn: str,
    postgres_table: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from taskiq_postgresql.drivers._psycopg import (  # noqa: PLC0415
        PsycopgListenDriver,
    )

    channel = f"{postgres_table}_channel"
    listener = PsycopgListenDriver(postgresql_dsn, channel)
    await listener.on_startup()
    try:
        async with await psycopg.AsyncConnection.connect(
            postgresql_dsn,
            autocommit=True,
        ) as sender:
            for payload in ("not-a-number", str(MESSAGE_ID)):
                await sender.execute("SELECT pg_notify(%s, %s)", (channel, payload))

        async with aclosing(listener.__aiter__()) as notifications:
            with caplog.at_level(logging.WARNING):
                received = await asyncio.wait_for(anext(notifications), timeout=5)
    finally:
        await listener.on_shutdown()

    assert received == MESSAGE_ID
    assert type(received) is int
    assert f"Invalid NOTIFY payload 'not-a-number' on channel {channel}" in (
        caplog.text
    )


async def test_when_payload_is_long_and_not_an_integer__then_log_line_is_bounded(
    broker: PostgresqlBroker,
    driver: str,
    postgres_table: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    channel = f"{postgres_table}_bounded"
    listener = get_db_listen_driver(driver)(  # type: ignore[arg-type]
        broker.dsn,
        channel,
    )
    await listener.on_startup()
    try:
        await asyncio.sleep(0.2)  # let the listener subscribe before notifying
        for payload in ("x" * LONG_PAYLOAD_LENGTH, str(MESSAGE_ID)):
            # psqlpy cannot map the void result of pg_notify, hence the cast.
            await broker.driver.execute(
                "SELECT pg_notify($1, $2)::text",
                channel,
                payload,
            )

        async with aclosing(listener.__aiter__()) as notifications:
            with caplog.at_level(logging.WARNING):
                received = await asyncio.wait_for(anext(notifications), timeout=5)
    finally:
        await listener.on_shutdown()

    assert received == MESSAGE_ID
    invalid = [r for r in caplog.records if "Invalid NOTIFY payload" in r.getMessage()]
    assert len(invalid) == 1
    message = invalid[0].getMessage()
    assert len(message) < LOG_LINE_BUDGET
    assert "truncated" in message
    assert str(LONG_PAYLOAD_LENGTH) in message
