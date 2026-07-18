from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.exceptions import (
    ListenerDisconnectedError,
    TransientDatabaseConnectionError,
)

pytestmark = pytest.mark.anyio

_DISCONNECT = object()
_EXPECTED_QUERY_ATTEMPTS = 2
_MAX_RETRY_DELAY = 30.0
_RECONCILIATION_BATCH_SIZE = 100
_RECONCILIATION_WATERMARK = 205


class FakeQueryDriver:
    """Provide only the query operations exercised by broker.listen()."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.shutdown_calls = 0

    async def on_startup(self) -> None:
        """Start the fake query driver."""

    async def on_shutdown(self) -> None:
        """Stop the fake query driver."""
        self.shutdown_calls += 1

    async def delete_returning(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, bytes]:
        """Return a message for every claimed notification ID."""
        return {"message": b"payload"}

    async def max_value(self, *args: Any, **kwargs: Any) -> int:
        """Return an empty reconciliation high-watermark."""
        return 0

    async def select_available_ids(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> list[int]:
        """Return no rows for reconciliation."""
        return []


class ReconciliationQueryDriver(FakeQueryDriver):
    """Expose a backlog large enough to require multiple bounded pages."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.page_requests: list[tuple[int, int, str, int]] = []

    async def max_value(self, *args: Any, **kwargs: Any) -> int:
        """Return a stable high-watermark for this reconciliation pass."""
        return _RECONCILIATION_WATERMARK

    async def select_available_ids(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> list[int]:
        """Return one keyset page and record its explicit bounds."""
        after_id = int(kwargs["after_id"])
        through_id = int(kwargs["through_id"])
        channel_name = str(kwargs["channel_name"])
        limit = int(kwargs["limit"])
        self.page_requests.append((after_id, through_id, channel_name, limit))
        page_end = min(after_id + limit, through_id)
        return list(range(after_id + 1, page_end + 1))


class FlakyQueryDriver(FakeQueryDriver):
    """Fail the first reconciliation query with a transient connection error."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.max_value_calls = 0

    async def max_value(self, *args: Any, **kwargs: Any) -> int:
        """Fail once, then allow the listener path to continue."""
        self.max_value_calls += 1
        if self.max_value_calls == 1:
            raise TransientDatabaseConnectionError("temporarily unavailable")
        return 0


class ScriptedListenDriver:
    """Run a deterministic listener startup and iteration script."""

    def __init__(
        self,
        events: Sequence[object] = (),
        startup_error: Exception | None = None,
    ) -> None:
        self.events = events
        self.startup_error = startup_error
        self.started = asyncio.Event()
        self.iterated = asyncio.Event()
        self.startup_calls = 0
        self.shutdown_calls = 0

    async def on_startup(self) -> None:
        """Start this listener generation or raise its scripted error."""
        self.startup_calls += 1
        self.started.set()
        if self.startup_error is not None:
            raise self.startup_error

    async def on_shutdown(self) -> None:
        """Record cleanup of this listener generation."""
        self.shutdown_calls += 1

    async def __aiter__(self) -> AsyncIterator[int]:
        """Yield IDs, disconnect, or complete according to the script."""
        self.iterated.set()
        for event in self.events:
            if event is _DISCONNECT:
                raise ListenerDisconnectedError("listener disconnected")
            yield int(event)


class BlockingStartupListenDriver(ScriptedListenDriver):
    """Keep listener startup pending until the broker cancels it."""

    async def on_startup(self) -> None:
        """Publish startup progress and wait indefinitely."""
        self.startup_calls += 1
        self.started.set()
        await asyncio.Event().wait()


class ListenerFactory:
    """Return one prebuilt listener for each requested generation."""

    def __init__(self, listeners: Sequence[ScriptedListenDriver]) -> None:
        self.listeners = listeners
        self.calls: list[ScriptedListenDriver] = []

    def __call__(self, *args: Any, **kwargs: Any) -> ScriptedListenDriver:
        """Return the next scripted listener."""
        listener = self.listeners[len(self.calls)]
        self.calls.append(listener)
        return listener


async def make_broker(
    monkeypatch: pytest.MonkeyPatch,
    listeners: Sequence[ScriptedListenDriver],
    query_driver_class: type[FakeQueryDriver] = FakeQueryDriver,
) -> tuple[PostgresqlBroker, ListenerFactory]:
    """Construct and start a broker backed entirely by deterministic fakes."""
    factory = ListenerFactory(listeners)
    monkeypatch.setattr(
        "taskiq_postgresql.broker.get_db_driver",
        lambda _driver: query_driver_class,
    )
    monkeypatch.setattr(
        "taskiq_postgresql.broker.get_db_listen_driver",
        lambda _driver: factory,
    )

    broker = PostgresqlBroker(dsn="postgresql://unused")
    broker._listener_retry_delay = lambda _attempt: 0.0
    await broker.startup()
    return broker, factory


async def next_message(broker: PostgresqlBroker) -> bytes:
    """Return the data from the next broker message."""
    iterator = broker.listen()
    try:
        return (await iterator.__anext__()).data
    finally:
        await iterator.aclose()


async def test_disconnect_replaces_listener_with_fresh_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = ScriptedListenDriver([_DISCONNECT])
    recovered = ScriptedListenDriver([1])
    broker, factory = await make_broker(monkeypatch, [initial, recovered])

    assert await next_message(broker) == b"payload"
    assert factory.calls == [initial, recovered]
    assert broker.listen_driver is recovered
    assert initial.shutdown_calls == 1
    assert recovered.startup_calls == 1

    await broker.shutdown()


async def test_startup_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = ScriptedListenDriver([1])
    broker, factory = await make_broker(monkeypatch, [listener])

    await broker.startup()

    assert factory.calls == [listener]
    assert listener.startup_calls == 1

    await broker.shutdown()


async def test_transient_startup_failure_is_retried_with_new_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = ScriptedListenDriver([_DISCONNECT])
    failed = ScriptedListenDriver(
        startup_error=TransientDatabaseConnectionError("temporarily unavailable"),
    )
    recovered = ScriptedListenDriver([1])
    broker, factory = await make_broker(monkeypatch, [initial, failed, recovered])

    assert await next_message(broker) == b"payload"
    assert factory.calls == [initial, failed, recovered]
    assert failed.shutdown_calls == 1
    assert broker.listen_driver is recovered

    await broker.shutdown()


async def test_terminal_reconnect_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = ScriptedListenDriver([_DISCONNECT])
    terminal_error = RuntimeError("invalid listener contract")
    failed = ScriptedListenDriver(startup_error=terminal_error)
    broker, factory = await make_broker(monkeypatch, [initial, failed])

    iterator = broker.listen()
    with pytest.raises(RuntimeError, match="invalid listener contract"):
        await iterator.__anext__()
    await iterator.aclose()

    assert factory.calls == [initial, failed]
    assert failed.shutdown_calls == 1

    await broker.shutdown()


async def test_shutdown_interrupts_reconnect_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = ScriptedListenDriver([_DISCONNECT])
    unused = ScriptedListenDriver([1])
    broker, factory = await make_broker(monkeypatch, [initial, unused])
    broker._listener_retry_delay = lambda _attempt: 3600.0

    iterator = broker.listen()
    pending_message = asyncio.create_task(iterator.__anext__())
    await asyncio.wait_for(initial.iterated.wait(), timeout=1)
    await asyncio.sleep(0)

    await broker.shutdown()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending_message, timeout=1)
    assert factory.calls == [initial]
    assert broker._shutdown_event.is_set()
    await iterator.aclose()


async def test_clean_iterator_completion_triggers_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = ScriptedListenDriver()
    recovered = ScriptedListenDriver([1])
    broker, factory = await make_broker(monkeypatch, [completed, recovered])

    assert await next_message(broker) == b"payload"
    assert factory.calls == [completed, recovered]
    assert completed.shutdown_calls == 1

    await broker.shutdown()


async def test_concurrent_consumers_share_one_recovery_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = ScriptedListenDriver([_DISCONNECT])
    recovered = ScriptedListenDriver([1])
    broker, factory = await make_broker(monkeypatch, [initial, recovered])

    messages = await asyncio.gather(next_message(broker), next_message(broker))

    assert messages == [b"payload", b"payload"]
    assert factory.calls == [initial, recovered]
    assert initial.shutdown_calls == 1
    assert recovered.startup_calls == 1

    await broker.shutdown()


async def test_shutdown_cancels_initial_listener_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = BlockingStartupListenDriver()
    factory = ListenerFactory([listener])
    monkeypatch.setattr(
        "taskiq_postgresql.broker.get_db_driver",
        lambda _driver: FakeQueryDriver,
    )
    monkeypatch.setattr(
        "taskiq_postgresql.broker.get_db_listen_driver",
        lambda _driver: factory,
    )
    broker = PostgresqlBroker(dsn="postgresql://unused")
    startup_task = asyncio.create_task(broker.startup())
    await asyncio.wait_for(listener.started.wait(), timeout=1)

    await broker.shutdown()

    with pytest.raises(asyncio.CancelledError):
        await startup_task
    assert listener.shutdown_calls == 1
    assert broker.driver.shutdown_calls == 1
    assert broker._shutdown_event.is_set()


async def test_transient_query_failure_retries_with_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = ScriptedListenDriver([1])
    broker, _ = await make_broker(
        monkeypatch,
        [listener],
        query_driver_class=FlakyQueryDriver,
    )

    assert await next_message(broker) == b"payload"
    assert broker.driver.max_value_calls == _EXPECTED_QUERY_ATTEMPTS

    await broker.shutdown()


async def test_unexpected_iterator_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = ScriptedListenDriver([object()])
    broker, factory = await make_broker(monkeypatch, [broken])

    with pytest.raises(TypeError):
        await next_message(broker)
    assert factory.calls == [broken]

    await broker.shutdown()


async def test_reconciliation_uses_bounded_keyset_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = ScriptedListenDriver()
    broker, _ = await make_broker(
        monkeypatch,
        [listener],
        query_driver_class=ReconciliationQueryDriver,
    )
    iterator = broker.listen()

    messages = [await iterator.__anext__() for _ in range(_RECONCILIATION_WATERMARK)]
    await iterator.aclose()

    assert len(messages) == _RECONCILIATION_WATERMARK
    assert broker.driver.page_requests == [
        (0, _RECONCILIATION_WATERMARK, "taskiq", _RECONCILIATION_BATCH_SIZE),
        (
            _RECONCILIATION_BATCH_SIZE,
            _RECONCILIATION_WATERMARK,
            "taskiq",
            _RECONCILIATION_BATCH_SIZE,
        ),
        (
            2 * _RECONCILIATION_BATCH_SIZE,
            _RECONCILIATION_WATERMARK,
            "taskiq",
            _RECONCILIATION_BATCH_SIZE,
        ),
    ]

    await broker.shutdown()


async def test_listener_backoff_is_capped_without_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = ScriptedListenDriver()
    broker, _ = await make_broker(monkeypatch, [listener])
    monkeypatch.setattr("taskiq_postgresql.broker.uniform", lambda _low, _high: 1.0)

    delays = [
        PostgresqlBroker._listener_retry_delay(broker, attempt)
        for attempt in range(1, 9)
    ]

    assert delays == [
        0.5,
        1.0,
        2.0,
        4.0,
        8.0,
        16.0,
        _MAX_RETRY_DELAY,
        _MAX_RETRY_DELAY,
    ]
    assert PostgresqlBroker._listener_retry_delay(broker, 1_000_000) == _MAX_RETRY_DELAY

    await broker.shutdown()
