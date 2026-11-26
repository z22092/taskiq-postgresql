"""
Table and channel names are checked when a component is built (ADR 0008).

Every test takes the ``driver`` fixture, so each runs once per driver. The
first group needs no server; the second sends and receives real
notifications.
"""

import asyncio
import logging
import uuid
import warnings
from collections.abc import Callable
from contextlib import aclosing
from typing import Any, Optional

import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.core.sql.catalog import notify_statement
from taskiq_postgresql.core.sql.identifier import IdentifierError
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from taskiq_postgresql.utils import get_db_listen_driver
from tests.helpers import (
    NOTIFY_DELIVERS_WITHIN,
    UNUSED_DSN,
    build_component,
    drop_and_shutdown,
    fetch_on_server,
)

# Far beyond any test, so only a NOTIFY can deliver in time.
SWEEP_OUT_OF_THE_WAY = 60.0
FAST_SWEEP = 0.2

# The first table name length whose derived names collide, per component: the
# sequence and index names of the broker and the scheduler source are cut at
# 62, the result backend's at 63.
FIRST_TOO_LONG = [("broker", 62), ("result_backend", 63), ("scheduler_source", 62)]

REFUSED_NAMES = [
    pytest.param("", id="empty"),
    pytest.param("1jobs", id="starts-with-a-digit"),
    pytest.param("jobs-table", id="outside-the-grammar"),
    pytest.param("a" * 64, id="64-bytes"),
    pytest.param("select", id="reserved-word"),
    pytest.param("LEFT", id="reserved-word-upper-case"),
]


def message_for(payload: bytes) -> BrokerMessage:
    """Return a message that is due at once."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=payload,
        labels={},
    )


@pytest.mark.parametrize("kind", ["broker", "result_backend", "scheduler_source"])
@pytest.mark.parametrize("name", REFUSED_NAMES)
def test_a_table_name_outside_the_rule_is_refused_when_the_component_is_built(
    kind: str,
    name: str,
    driver: str,
) -> None:
    with pytest.raises(IdentifierError, match="table name"):
        build_component(kind, table_name=name, driver=driver)


@pytest.mark.parametrize("name", REFUSED_NAMES)
def test_a_channel_name_outside_the_rule_is_refused_when_the_broker_is_built(
    name: str,
    driver: str,
) -> None:
    with pytest.raises(IdentifierError, match="channel name"):
        PostgresqlBroker(dsn=UNUSED_DSN, driver=driver, channel_name=name)  # type: ignore[arg-type]


def build_with_table(kind: str, table_name: str, driver: str) -> object:
    """Build a component on ``table_name``; the broker keeps its default channel."""
    if kind == "broker":
        return PostgresqlBroker(dsn=UNUSED_DSN, driver=driver, table_name=table_name)  # type: ignore[arg-type]
    return build_component(kind, table_name=table_name, driver=driver)


@pytest.mark.parametrize(("kind", "first_refused"), FIRST_TOO_LONG)
def test_the_first_table_name_whose_derived_names_collide_is_refused(
    kind: str,
    first_refused: int,
    driver: str,
) -> None:
    build_with_table(kind, "t" * (first_refused - 1), driver)

    with pytest.raises(IdentifierError, match="too long"):
        build_with_table(kind, "t" * first_refused, driver)


MIXED_CASE_BUILDERS = [
    pytest.param(
        lambda driver: PostgresqlBroker(
            dsn=UNUSED_DSN, driver=driver, table_name="Jobs_Mixed"
        ),
        "table_name",
        id="broker-table",
    ),
    pytest.param(
        lambda driver: PostgresqlBroker(
            dsn=UNUSED_DSN, driver=driver, channel_name="Jobs_Mixed"
        ),
        "channel_name",
        id="broker-channel",
    ),
    pytest.param(
        lambda driver: PostgresqlResultBackend(
            dsn=UNUSED_DSN, driver=driver, table_name="Jobs_Mixed"
        ),
        "table_name",
        id="result-backend-table",
    ),
    pytest.param(
        lambda driver: PostgresqlSchedulerSource(
            dsn=UNUSED_DSN, driver=driver, table_name="Jobs_Mixed"
        ),
        "table_name",
        id="scheduler-source-table",
    ),
]


@pytest.mark.parametrize(("build", "attribute"), MIXED_CASE_BUILDERS)
def test_a_mixed_case_name_warns_once_and_is_kept_as_configured(
    build: Callable[[str], Any],
    attribute: str,
    driver: str,
) -> None:
    # The warning points at the line that built the component, which is the
    # builder's own.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        component = build(driver)

    assert len(caught) == 1
    assert issubclass(caught[0].category, DeprecationWarning)
    assert caught[0].filename == __file__
    assert caught[0].lineno == build.__code__.co_firstlineno
    assert getattr(component, attribute) == "Jobs_Mixed"


async def first_message(worker: PostgresqlBroker) -> AckableMessage:
    """Return the first message ``worker.listen()`` yields, then close it."""
    async with aclosing(worker.listen()) as messages:
        return await anext(messages)


@pytest.mark.anyio
async def test_a_mixed_case_channel_is_delivered_by_notify_on_every_driver(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    channel = f"Jobs_{postgres_table}"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        worker = PostgresqlBroker(
            dsn=postgresql_dsn,
            channel_name=channel,
            table_name=postgres_table,
            driver=driver,
            poll_interval=SWEEP_OUT_OF_THE_WAY,
            run_migrations=True,
        )
    claimed: list[Optional[AckableMessage]] = []
    claim_notified = worker._claim_notified

    async def spy(message_id: int) -> Optional[AckableMessage]:
        claimed.append(await claim_notified(message_id))
        return claimed[-1]

    worker._claim_notified = spy  # type: ignore[method-assign]
    await worker.startup()
    try:
        task = asyncio.create_task(first_message(worker))
        # The sweep that starts listen() finds nothing; then it reads LISTEN.
        await asyncio.sleep(0.5)
        sent = message_for(b"mixed-case-channel")
        await worker.kick(sent)

        # The next sweep is 60 s away, so only the NOTIFY can deliver here.
        received = await asyncio.wait_for(task, timeout=NOTIFY_DELIVERS_WITHIN)
    finally:
        await drop_and_shutdown(worker, postgres_table)

    assert [message is not None for message in claimed] == [True]
    assert received.data == sent.message


@pytest.mark.anyio
async def test_a_listen_driver_built_with_mixed_case_receives_the_folded_notify(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    channel = f"Jobs_{postgres_table}"
    listener = get_db_listen_driver(driver)(postgresql_dsn, channel)  # type: ignore[arg-type]
    await listener.on_startup()
    try:
        assert listener.channel_name == channel.lower()
        await asyncio.sleep(0.3)  # let the listener subscribe before notifying
        await fetch_on_server(
            postgresql_dsn,
            "SELECT pg_notify($1, $2)::text",
            channel.lower(),
            "7",
        )

        async with aclosing(listener.__aiter__()) as notifications:
            received = await asyncio.wait_for(anext(notifications), timeout=5)
    finally:
        await listener.on_shutdown()

    assert received == "7"


@pytest.mark.anyio
async def test_a_kick_sends_the_notify_statement_with_the_channel_and_id_as_binds(
    broker: PostgresqlBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The values reach the server as binds: tests/test_driver_binds.py proves
    # that for every driver. Here the broker is held to what it hands over.
    executed: list[tuple[Any, ...]] = []
    inserted: list[int] = []
    execute = broker.driver.execute
    insert = broker.driver.insert

    async def spy_execute(*args: Any) -> Any:
        executed.append(args)
        return await execute(*args)

    async def spy_insert(*args: Any) -> int:
        inserted.append(await insert(*args))
        return inserted[-1]

    broker.driver.execute = spy_execute  # type: ignore[method-assign]
    broker.driver.insert = spy_insert  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING):
        await broker.kick(message_for(b"binds"))

    # A positive control: the spies saw the kick, so the empty log below means
    # no failure rather than no call.
    assert len(inserted) == 1
    assert executed == [
        (notify_statement().text, broker._channel.name, str(inserted[0])),
    ]
    assert "NOTIFY of message" not in caplog.text


@pytest.mark.anyio
async def test_a_row_stored_with_the_mixed_case_channel_is_claimed(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    channel = f"Jobs_{postgres_table}"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        worker = PostgresqlBroker(
            dsn=postgresql_dsn,
            channel_name=channel,
            table_name=postgres_table,
            driver=driver,
            poll_interval=FAST_SWEEP,
            run_migrations=True,
        )
    await worker.startup()
    try:
        sent = message_for(b"stored-as-configured")
        await worker.kick(sent)
        (row,) = await fetch_on_server(
            postgresql_dsn,
            f"SELECT channel FROM {postgres_table}",  # noqa: S608
        )

        received = await asyncio.wait_for(first_message(worker), timeout=5)
    finally:
        await drop_and_shutdown(worker, postgres_table)

    assert row["channel"] == channel
    assert received.data == sent.message
