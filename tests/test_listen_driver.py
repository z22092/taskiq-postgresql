"""Integration tests for the listen drivers against a real PostgreSQL."""

import asyncio
import logging
from contextlib import aclosing

import pytest

pytestmark = pytest.mark.anyio

MESSAGE_ID = 42


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
