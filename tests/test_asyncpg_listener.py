import asyncio
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("asyncpg")

from taskiq_postgresql.drivers import _asyncpg
from taskiq_postgresql.drivers._asyncpg import AsyncpgListenDriver
from taskiq_postgresql.exceptions import (
    DatabaseConnectionError,
    ListenerDisconnectedError,
    TransientDatabaseConnectionError,
)

pytestmark = pytest.mark.anyio
_MESSAGE_ID = 42


def make_connection() -> MagicMock:
    """Create the asyncpg connection boundary used by listener tests."""
    connection = MagicMock()
    connection.add_listener = AsyncMock()
    connection.remove_listener = AsyncMock()
    connection.close = AsyncMock()
    connection.is_closed.return_value = False
    return connection


async def test_termination_wakes_pending_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unexpected connection close must be observable without a notification."""
    connection = make_connection()
    termination_callback: Callable[[object], None] | None = None

    def capture_termination_callback(callback: Callable[[object], None]) -> None:
        nonlocal termination_callback
        termination_callback = callback

    connection.add_termination_listener.side_effect = capture_termination_callback
    monkeypatch.setattr(_asyncpg, "connect", AsyncMock(return_value=connection))
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")
    await driver.on_startup()

    pending_message = asyncio.create_task(driver.__aiter__().__anext__())
    await asyncio.sleep(0)
    assert termination_callback is not None
    termination_callback(connection)

    with pytest.raises(ListenerDisconnectedError):
        await asyncio.wait_for(pending_message, timeout=0.1)


async def test_shutdown_wakes_listener_and_closes_connection_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Intentional shutdown must stop iteration and remain idempotent."""
    connection = make_connection()
    monkeypatch.setattr(_asyncpg, "connect", AsyncMock(return_value=connection))
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")
    await driver.on_startup()
    pending_message = asyncio.create_task(driver.__aiter__().__anext__())
    await asyncio.sleep(0)

    await driver.on_shutdown()
    await driver.on_shutdown()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending_message, timeout=0.1)
    connection.remove_termination_listener.assert_called_once()
    connection.remove_listener.assert_awaited_once()
    connection.close.assert_awaited_once()


async def test_partial_listener_startup_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed LISTEN registration must not leak the connected session."""
    connection = make_connection()
    connection.add_listener.side_effect = OSError("connection lost")
    monkeypatch.setattr(_asyncpg, "connect", AsyncMock(return_value=connection))
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")

    with pytest.raises(TransientDatabaseConnectionError):
        await driver.on_startup()

    assert driver.connection is None
    connection.close.assert_awaited_once()


async def test_listener_timeout_is_transient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asyncio timeouts remain retryable on Python versions before 3.11."""
    monkeypatch.setattr(
        _asyncpg,
        "connect",
        AsyncMock(side_effect=asyncio.TimeoutError),
    )
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")

    with pytest.raises(TransientDatabaseConnectionError):
        await driver.on_startup()


async def test_closed_connection_interface_error_is_transient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A socket closed during LISTEN setup is a transient transport race."""
    connection = make_connection()
    connection.is_closed.return_value = True
    connection.add_listener.side_effect = _asyncpg.InterfaceError("closed")
    monkeypatch.setattr(_asyncpg, "connect", AsyncMock(return_value=connection))
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")

    with pytest.raises(TransientDatabaseConnectionError):
        await driver.on_startup()

    connection.close.assert_awaited_once()


async def test_invalid_listener_configuration_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configuration failures must not enter transient reconnect policy."""
    monkeypatch.setattr(
        _asyncpg,
        "connect",
        AsyncMock(side_effect=ValueError("invalid connection option")),
    )
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")

    with pytest.raises(DatabaseConnectionError) as error_info:
        await driver.on_startup()

    assert not isinstance(error_info.value, TransientDatabaseConnectionError)


async def test_stale_termination_callback_cannot_poison_new_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A callback queued by the old connection must not stop the new listener."""
    first_connection = make_connection()
    second_connection = make_connection()
    callbacks: list[Callable[[object], None]] = []
    first_connection.add_termination_listener.side_effect = callbacks.append
    second_connection.add_termination_listener.side_effect = callbacks.append
    monkeypatch.setattr(
        _asyncpg,
        "connect",
        AsyncMock(side_effect=[first_connection, second_connection]),
    )
    driver = AsyncpgListenDriver("postgresql://unused", "taskiq")
    await driver.on_startup()
    await driver.on_shutdown()
    await driver.on_startup()

    callbacks[0](first_connection)
    driver._notification_handler(
        second_connection,
        1,
        "taskiq",
        str(_MESSAGE_ID),
    )

    assert (
        await asyncio.wait_for(driver.__aiter__().__anext__(), timeout=0.1)
        == _MESSAGE_ID
    )
