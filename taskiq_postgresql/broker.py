from __future__ import annotations

import math
from asyncio import Event, create_task, gather, get_running_loop, wait_for
from asyncio import Queue as AsyncQueue
from asyncio import TimeoutError as AsyncTimeoutError
from contextlib import suppress
from dataclasses import dataclass
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    TypeVar,
)

from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_postgresql.abc.driver import ListenDriver
from taskiq_postgresql.abc.query import (
    AddedIndex,
    ClaimByIdQuery,
    ClaimColumns,
    ClaimDueQuery,
    Column,
    CreatedAtColumn,
    NotBeforeColumn,
    PrimaryKeyColumn,
    index_name,
)
from taskiq_postgresql.utils import (
    Dsn,
    dsn_before_startup,
    get_db_driver,
    get_db_listen_driver,
    require_dsn,
    resolve_dsn,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


_T = TypeVar("_T")

logger = getLogger("taskiq.asyncpg_broker")

# The most characters of an invalid NOTIFY payload that a log line shows.
_MAX_LOGGED_PAYLOAD = 64


def _parse_delay(value: Any) -> float:
    """
    Read the ``delay`` label as a number of seconds.

    taskiq sends every label as ``str(value)``, so a float delay, such as the
    one ``SmartRetryMiddleware`` computes, arrives as ``"2.5"`` or ``"5.0"``.

    Args:
        value: the ``delay`` label of a message.

    Returns:
        The delay in seconds; a negative delay is ``0.0``.

    Raises:
        ValueError: if the label is not a finite number of seconds.
    """
    # bool is an int subclass, so float(True) would silently mean 1 second.
    if not isinstance(value, bool):
        # OverflowError: float() of an int beyond the float range.
        with suppress(TypeError, ValueError, OverflowError):
            seconds = float(value)
            # Neither NaN nor infinity is a delay the server can add to NOW(),
            # so the message would never be due.
            if math.isfinite(seconds):
                return max(seconds, 0.0)
    msg = f"The 'delay' label must be a finite number of seconds, got {value!r:.50}"
    raise ValueError(msg)


def _parse_poll_interval(value: Any) -> float:
    """
    Check the ``poll_interval`` argument of the broker.

    Args:
        value: the ``poll_interval`` given to the broker.

    Returns:
        The interval in seconds.

    Raises:
        ValueError: if it is not a finite number of seconds above zero.
    """
    # bool is an int subclass, so True would silently mean 1 second.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # OverflowError: float() of an int beyond the float range.
        with suppress(OverflowError):
            seconds = float(value)
            if math.isfinite(seconds) and seconds > 0:
                return seconds
    msg = (
        "poll_interval must be a finite number of seconds above zero, "
        f"got {value!r:.50}"
    )
    raise ValueError(msg)


def _parse_message_id(payload: str) -> Optional[int]:
    """Return the message id a NOTIFY carries, or None if it is not one."""
    # The only place a payload is parsed: the drivers yield it unparsed.
    try:
        return int(payload)
    except ValueError:
        return None


def _payload_excerpt(payload: str) -> str:
    """Return ``payload`` as a log line may carry it: quoted, cut if long."""
    # Any role with CONNECT can NOTIFY the channel, and a payload can hold
    # close to 8 KB, so a log line must not carry it whole.
    excerpt = repr(payload[:_MAX_LOGGED_PAYLOAD])
    if len(payload) > _MAX_LOGGED_PAYLOAD:
        excerpt += f" (truncated, {len(payload)} characters)"
    return excerpt


async def _ack() -> None:
    """Do nothing: the row was deleted when the message was claimed."""


async def _wake_when_set(event: Event, queue: AsyncQueue[Optional[int]]) -> None:
    """Put None in ``queue`` once ``event`` is set, to wake whoever waits on it."""
    await event.wait()
    queue.put_nowait(None)


@dataclass
class Table:
    """Columns for the result backend."""

    primary_key = PrimaryKeyColumn(
        name="id",
        type_="SERIAL",
    )
    task_id: Column
    task_name = Column(
        name="task_name",
        type_="VARCHAR",
    )
    message = Column(
        name="message",
        type_="BYTEA",
    )
    labels = Column(
        name="labels",
        type_="JSONB",
    )
    created_at = CreatedAtColumn()
    # Added to the table by the upgrade step, never by CREATE TABLE, so a new
    # table and one created by 0.4.x end up with the same columns.
    channel = Column(
        name="channel",
        type_="VARCHAR",
        nullable=True,
    )
    not_before = NotBeforeColumn()


class PostgresqlBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: Dsn,
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        field_for_task_id: Literal["VarChar", "Text", "Uuid"] = "Uuid",
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        driver: Literal["asyncpg", "psqlpy", "psycopg"] = "asyncpg",
        max_retry_attempts: int = 5,
        connection_kwargs: dict[str, Any] | None = None,
        pool_kwargs: dict[str, Any] | None = None,
        run_migrations: bool = False,
        poll_interval: float = 1.0,
    ) -> None:
        """
        Construct a new broker.

        Args:
            dsn (Dsn): \
                connection string to PostgreSQL, or callable returning one. \
                A callable is resolved once, by startup().
            result_backend (AsyncResultBackend[_T] | None, optional): \
                Custom result backend.
            task_id_generator (Callable[[], str] | None, optional): \
                Custom task_id generator.
            field_for_task_id (Literal["VarChar", "Text", "Uuid"], optional): \
                Field for task_id. Defaults to "Uuid".
            channel_name (str, optional): \
                Name of the channel to listen on.
            table_name (str, optional): \
                Name of the table to store messages.
            driver (Literal["asyncpg"], optional): \
                Driver to use. Defaults to "asyncpg".
            max_retry_attempts (int, optional): \
                Maximum number of message processing attempts.
            connection_kwargs (dict[str, Any] | None, optional): \
                Additional arguments for asyncpg connection.
            pool_kwargs (dict[str, Any] | None, optional): \
                Additional arguments for asyncpg pool creation.
            run_migrations (bool, optional): \
                Create the tables and indexes on startup. Defaults to False.
            poll_interval (float, optional): \
                Seconds between two sweeps of listen() for due messages, so \
                a delayed message goes out at most this long after it is \
                due. Defaults to 1.0.

        Raises:
            TypeError: if ``dsn`` is ``None``, empty or blank.
            ValueError: if ``poll_interval`` is not a finite number of \
                seconds above zero.
        """
        dsn = require_dsn(dsn, type(self).__name__)
        self.poll_interval: float = _parse_poll_interval(poll_interval)
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: Dsn = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.connection_kwargs: dict[str, Any] = (
            connection_kwargs if connection_kwargs else {}
        )
        self.pool_kwargs: dict[str, Any] = pool_kwargs if pool_kwargs else {}
        self.max_retry_attempts: int = max_retry_attempts
        self._queue: AsyncQueue[str] | None = None
        # Set by shutdown(), so a running listen() ends instead of reopening
        # the pool the drivers just closed. startup() makes a new one rather
        # than clearing it: an Event belongs to the loop that first waits on it.
        self._stopping = Event()

        self.columns = Table(task_id=Column(name="task_id", type_=field_for_task_id))
        claim_columns = ClaimColumns(
            channel=self.columns.channel,
            not_before=self.columns.not_before,
        )

        self.driver = get_db_driver(driver)(
            connection_string=dsn_before_startup(dsn),
            table_name=self.table_name,
            columns=[
                self.columns.task_id,
                self.columns.task_name,
                self.columns.message,
                self.columns.labels,
            ],
            primary_key=self.columns.primary_key,
            created_at=self.columns.created_at,
            index_columns=[self.columns.primary_key],
            run_migrations=run_migrations,
            added_columns=claim_columns,
            added_indexes=[
                AddedIndex(
                    name=index_name(self.table_name, self.columns.not_before),
                    columns=[*claim_columns, self.columns.primary_key],
                ),
            ],
            **self.connection_kwargs,
        )
        returning = [self.columns.message]
        self._claim_due_query = ClaimDueQuery(self.table_name).make_query(
            self.columns.primary_key,
            claim_columns,
            returning,
        )
        self._claim_by_id_query = ClaimByIdQuery(self.table_name).make_query(
            self.columns.primary_key,
            claim_columns,
            returning,
        )
        self.listen_driver: ListenDriver = get_db_listen_driver(driver)(
            connection_string=dsn_before_startup(dsn),
            channel_name=self.channel_name,
            **self.connection_kwargs,
        )

    @property
    def dsn(self) -> str:
        """
        Get the DSN string, calling the DSN callable if one was given.

        Raises:
            TypeError: if the DSN callable returns ``None`` or an empty string.
        """
        return resolve_dsn(self._dsn, type(self).__name__)

    async def startup(self) -> None:
        """
        Initialize the broker.

        Raises:
            TypeError: if the DSN callable returns ``None`` or an empty string.
        """
        # Resolved once, so both drivers connect to the same server.
        dsn = self.dsn
        self.driver.connection_string = dsn
        self.listen_driver.connection_string = dsn
        await super().startup()

        await self.driver.on_startup()
        await self.listen_driver.on_startup()
        self._stopping = Event()

    async def shutdown(self) -> None:
        """Close all connections on shutdown; a running listen() then ends."""
        self._stopping.set()
        await super().shutdown()
        await self.driver.on_shutdown()
        await self.listen_driver.on_shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message with the channel and the time it is due, which the
        server computes from its own clock and the ``delay`` label. A message
        due at once is also announced with a NOTIFY; a delayed one is left to
        the sweep of listen(), so it does not depend on this process. A failed
        NOTIFY is logged as a warning, and the sweep delivers that message.

        :param message: Message to send.
        :raises ValueError: if the ``delay`` label is not a finite number of
            seconds; nothing is stored then.
        :raises Exception: whatever the driver raises for the INSERT; the
            message is not stored then, and not sent again.
        """
        # Parsed before the insert, so an invalid delay stores nothing.
        delay_value = message.labels.get("delay")
        delay_seconds = 0.0 if delay_value is None else _parse_delay(delay_value)

        message_inserted_id = await self.driver.insert(
            [
                self.columns.task_id,
                self.columns.task_name,
                self.columns.message,
                self.columns.labels,
                self.columns.channel,
                self.columns.not_before,
            ],
            [
                message.task_id,
                message.task_name,
                message.message,
                message.labels,
                self.channel_name,
                delay_seconds,
            ],
            [
                self.columns.primary_key,
            ],
        )

        # No NOTIFY for a delayed message: a 0.4.x worker claims any id it is
        # notified of, so it would deliver the message early.
        if delay_seconds == 0.0:
            await self._send_notification(message_inserted_id)

    async def _send_notification(self, message_id: int) -> None:
        """Send a notification with the message ID as payload."""
        try:
            await self.driver.execute(f"NOTIFY {self.channel_name}, '{message_id}'")
        except Exception as error:
            # The row is committed and the sweep delivers it: raising would
            # make a caller that retries send the task twice.
            logger.warning(
                "NOTIFY of message %s on channel %s failed (%r): it will be "
                "delivered by the next sweep",
                message_id,
                self.channel_name,
                error,
            )

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields the due messages of the channel. A sweep claims them one by one,
        oldest first, when listen() starts and then every ``poll_interval``
        seconds, however many NOTIFYs arrive in between. A NOTIFY claims its
        message at once if it is due. Each claim is an atomic DELETE ...
        RETURNING, so a message reaches one worker only, and at most once: a
        claimed row is gone even if its processing never ends.

        Once reading the LISTEN connection ends or fails, a warning is logged
        and this listen() delivers by the sweep alone; only startup() opens
        the connection again. A failed claim is logged and tried again at the
        next sweep. listen() ends once shutdown() starts.

        :yields: AckableMessage instances.
        """
        loop = get_running_loop()
        stopping = self._stopping
        # The ids of the NOTIFYs; None only wakes the loop up for shutdown().
        notified: AsyncQueue[Optional[int]] = AsyncQueue()
        helpers = (
            create_task(self._queue_notified_ids(notified, stopping)),
            create_task(_wake_when_set(stopping, notified)),
        )
        message_id: Optional[int] = None
        next_sweep = loop.time()
        try:
            # The only shutdown check, so every claim comes right after it.
            while not stopping.is_set():
                if message_id is not None:
                    message = await self._claim_notified(message_id)
                    message_id = None
                elif loop.time() >= next_sweep:
                    message = await self._claim_due()
                    if message is None:
                        next_sweep = loop.time() + self.poll_interval
                else:
                    # Cancel-safe: a cancelled Queue.get() leaves the id queued.
                    with suppress(AsyncTimeoutError):
                        message_id = await wait_for(
                            notified.get(),
                            next_sweep - loop.time(),
                        )
                    continue
                if message is not None:
                    yield message
        finally:
            for helper in helpers:
                helper.cancel()
            await gather(*helpers, return_exceptions=True)

    async def _queue_notified_ids(
        self,
        notified: AsyncQueue[Optional[int]],
        stopping: Event,
    ) -> None:
        """
        Queue the message id of each NOTIFY until reading LISTEN ends.

        It is read once per listen(): reopening the connection is up to the
        ListenDriver, so once it ends this warns and returns, and the sweep
        delivers alone.
        """
        reason = "its iterator ended"
        try:
            async for payload in self.listen_driver:
                message_id = _parse_message_id(payload)
                if message_id is None:
                    logger.warning(
                        "Invalid NOTIFY payload %s on channel %s",
                        _payload_excerpt(payload),
                        self.channel_name,
                    )
                else:
                    notified.put_nowait(message_id)
        except Exception as error:
            reason = repr(error)
        # shutdown() closes the connection itself.
        if not stopping.is_set():
            logger.warning(
                "LISTEN on channel %s stopped (%s): delivering only by polling "
                "every %s s; the LISTEN connection is not reopened until the "
                "worker restarts",
                self.channel_name,
                reason,
                self.poll_interval,
            )

    async def _claim_due(self) -> Optional[AckableMessage]:
        """Claim the oldest due message of the channel, see ``ClaimDueQuery``."""
        return await self._claim(
            "a due message",
            self._claim_due_query,
            self.channel_name,
        )

    async def _claim_notified(self, message_id: int) -> Optional[AckableMessage]:
        """Claim the message a NOTIFY named, see ``ClaimByIdQuery``."""
        return await self._claim(
            f"message {message_id}",
            self._claim_by_id_query,
            self.channel_name,
            message_id,
        )

    async def _claim(
        self,
        what: str,
        query: str,
        *values: Any,
    ) -> Optional[AckableMessage]:
        """
        Run one claim; log a failure instead of raising, so listen() goes on.

        :param what: the claimed message, as the log names it.
        :return: the message, or None if no row matched or the claim failed.
        """
        try:
            row = await self.driver.fetch_row(query, *values)
        except Exception:
            logger.exception("Error claiming %s on channel %s", what, self.channel_name)
            return None
        if row is None:
            return None
        return AckableMessage(data=row[self.columns.message.name], ack=_ack)
