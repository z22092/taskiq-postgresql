from __future__ import annotations

import math
import random
from asyncio import (
    Event,
    Task,
    create_task,
    gather,
    get_running_loop,
    wait,
    wait_for,
)
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
from taskiq_postgresql.core.sql.catalog import (
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
    from collections.abc import AsyncGenerator, Coroutine


_T = TypeVar("_T")

logger = getLogger("taskiq.asyncpg_broker")

# The most characters of an invalid NOTIFY payload that a log line shows.
_MAX_LOGGED_PAYLOAD = 64
# The ids a SERIAL primary key gives, the broker table's id column.
_MIN_MESSAGE_ID = 1
_MAX_MESSAGE_ID = 2_147_483_647
# The ceilings of the first wait before reopening LISTEN and of the longest.
_FIRST_WAIT = 0.5
_LONGEST_WAIT = 30.0
# How long a reopened LISTEN connection must last to start the waits over.
_STABLE_AFTER = 30.0


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


def _parse_seconds(value: Any, name: str) -> float:
    """
    Check a duration argument of the broker.

    Args:
        value: the value given to the broker.
        name: the argument's name, for the error message.

    Returns:
        The duration in seconds.

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
    msg = f"{name} must be a finite number of seconds above zero, got {value!r:.50}"
    raise ValueError(msg)


def _parse_message_id(payload: str) -> Optional[int]:
    """Return the message id a NOTIFY carries, or None if it is not one."""
    # The only place a payload is parsed: the drivers yield it unparsed.
    try:
        message_id = int(payload)
    except ValueError:
        return None
    # Out of the id column's range, a claim matches no row or fails on the
    # server, whose error would log the whole payload with a traceback.
    if not _MIN_MESSAGE_ID <= message_id <= _MAX_MESSAGE_ID:
        return None
    return message_id


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


class _ReopenWaits:
    """
    The waits before the attempts to reopen LISTEN.

    The ceiling doubles from 0.5 s with each failure, up to 30 s, and each
    wait is drawn between half of it and all of it, so the workers a failover
    dropped together do not all come back in the same instant. A reopened
    connection that stops before 30 s counts as one more failure: a server
    that drops each session soon after accepting it is not reopened several
    times a second for good.
    """

    def __init__(self) -> None:
        self._failures = 0

    def next_wait(self) -> float:
        """Seconds to wait before the next attempt."""
        # Capped, so the power stays a small number.
        ceiling = min(_LONGEST_WAIT, _FIRST_WAIT * 2 ** min(self._failures, 10))
        return ceiling / 2 + random.uniform(0, ceiling / 2)  # noqa: S311 (jitter)

    def failed(self) -> None:
        """Count an attempt that could not reopen the connection."""
        self._failures += 1

    def served(self, seconds: float) -> None:
        """Count a reopened connection that stopped after ``seconds``."""
        if seconds >= _STABLE_AFTER:
            self._failures = 0
        else:
            self._failures += 1


async def _is_set_within(event: Event, seconds: float) -> bool:
    """Wait up to ``seconds`` for ``event`` and tell whether it is set."""
    with suppress(AsyncTimeoutError):
        await wait_for(event.wait(), seconds)
    return event.is_set()


async def _end_on_shutdown(
    stopping: Event,
    ended: Event,
    notified: AsyncQueue[Optional[int]],
) -> None:
    """Once ``stopping`` is set, set ``ended`` and wake the loop of listen()."""
    await stopping.wait()
    ended.set()
    notified.put_nowait(None)


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
        health_check_interval: float = 30.0,
        query_timeout: float = 30.0,
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
                Create the tables and indexes on startup. Defaults to False: \
                startup() then only checks that they exist.
            poll_interval (float, optional): \
                Seconds between two sweeps of listen() for due messages, so \
                a delayed message goes out at most this long after it is \
                due. Defaults to 1.0.
            health_check_interval (float, optional): \
                Seconds the LISTEN connection may stay silent before the \
                worker checks it by sending the same LISTEN again. A check \
                that fails reopens the connection. Defaults to 30.0.
            query_timeout (float, optional): \
                Seconds the worker waits for that check, for a claim (not \
                with psqlpy) and for a close before it gives the connection \
                up. Defaults to 30.0.

        Raises:
            TypeError: if ``dsn`` is ``None``, empty or blank.
            ValueError: if ``poll_interval``, ``health_check_interval`` or \
                ``query_timeout`` is not a finite number of seconds above \
                zero.
        """
        dsn = require_dsn(dsn, type(self).__name__)
        self.poll_interval: float = _parse_seconds(poll_interval, "poll_interval")
        self.health_check_interval: float = _parse_seconds(
            health_check_interval,
            "health_check_interval",
        )
        self.query_timeout: float = _parse_seconds(query_timeout, "query_timeout")
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
        # The tasks that read LISTEN for each running listen(); shutdown()
        # cancels them so that none reopens the connection after it.
        self._listen_readers: set[Task[None]] = set()

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
        self._configure_drivers()

    def _configure_drivers(self) -> None:
        """Hand the recovery settings, and the kind of the table, to the drivers."""
        # Attributes, not constructor arguments: the bundled drivers pass every
        # keyword argument on to the connection.
        self.driver.component_kind = "broker"
        self.driver.query_timeout = self.query_timeout
        self.listen_driver.health_check_interval = self.health_check_interval
        self.listen_driver.query_timeout = self.query_timeout

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
            SchemaNotReadyError: with ``run_migrations=False``, if the table
                or one of its columns is missing; see
                ``QueryDriver.on_startup``.
        """
        # Resolved once, so both drivers connect to the same server.
        dsn = self.dsn
        self.driver.connection_string = dsn
        self.listen_driver.connection_string = dsn
        # Again here, for drivers swapped in after the constructor.
        self._configure_drivers()
        await super().startup()

        await self.driver.on_startup()
        await self.listen_driver.on_startup()
        self._stopping = Event()

    async def shutdown(self) -> None:
        """Close all connections on shutdown; a running listen() then ends."""
        self._stopping.set()
        # A reopen in flight would open a LISTEN after on_shutdown() below.
        readers = list(self._listen_readers)
        for reader in readers:
            reader.cancel()
        await super().shutdown()
        # Side by side, so a silent pool and a silent LISTEN cost one
        # query_timeout, not two; closing the LISTEN socket also ends a health
        # check the reader may still be waiting on.
        closed = await gather(
            self._close(self.driver.on_shutdown()),
            self._close(self.listen_driver.on_shutdown()),
            return_exceptions=True,
        )
        if readers:
            await wait(readers, timeout=self.query_timeout)
        for outcome in closed:
            if isinstance(outcome, BaseException):
                raise outcome

    async def _close(self, closing: Coroutine[Any, Any, None]) -> None:
        """
        Await a driver's ``on_shutdown()`` for at most twice ``query_timeout``.

        A driver bounds its own close by ``query_timeout`` and then aborts the
        connection; this wider bound only stops a custom driver that does not,
        and leaves a bundled one the time to abort.
        """
        await wait_for(closing, 2 * self.query_timeout)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message with the channel and the time it is due, which the
        server computes from its own clock and the ``delay`` label. A message
        due at once is also announced with a NOTIFY; a delayed one is left to
        the sweep of listen(), so it does not depend on this process. A failed
        NOTIFY is logged as a warning, and the sweep delivers that message.

        Args:
            message: Message to send.

        Raises:
            ValueError: if the ``delay`` label is not a finite number of
                seconds; nothing is stored then.
            Exception: whatever the driver raises for the INSERT; the
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

        Once reading the LISTEN connection ends or fails, a warning is logged,
        the sweep keeps delivering and the connection is reopened, waiting
        longer after each failed attempt. A bundled driver also checks a
        connection that stays silent for ``health_check_interval`` seconds. A
        claim that fails, or gets no answer within ``query_timeout`` (asyncpg
        and psycopg only), is logged and tried again at the next sweep.
        listen() ends once shutdown() starts.

        Yields:
            AckableMessage instances.
        """
        loop = get_running_loop()
        stopping = self._stopping
        # Set once this listen() ends, however it does, so that its reader
        # stops even if a wait_for() in it swallowed the cancellation, as it
        # can before Python 3.12.
        ended = Event()
        # The ids of the NOTIFYs; None only wakes the loop up for shutdown().
        notified: AsyncQueue[Optional[int]] = AsyncQueue()
        reader = create_task(self._queue_notified_ids(notified, ended))
        self._listen_readers.add(reader)
        reader.add_done_callback(self._listen_readers.discard)
        helpers = (reader, create_task(_end_on_shutdown(stopping, ended, notified)))
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
            ended.set()
            for helper in helpers:
                helper.cancel()
            await gather(*helpers, return_exceptions=True)

    async def _queue_notified_ids(
        self,
        notified: AsyncQueue[Optional[int]],
        ended: Event,
    ) -> None:
        """
        Queue the message id of each NOTIFY, reopening LISTEN when it stops.

        Each time reading the ListenDriver ends or fails, this warns once and
        reopens the connection through the driver's own on_shutdown() and
        on_startup(); the sweep delivers meanwhile. It returns once its
        listen() has ended.
        """
        loop = get_running_loop()
        waits = _ReopenWaits()
        reopened_at: Optional[float] = None
        while True:
            reason = await self._read_notified_ids(notified)
            # The connection is left for shutdown() to close, or for the next
            # listen() to read.
            if ended.is_set():
                return
            if reopened_at is not None:
                waits.served(loop.time() - reopened_at)
            logger.warning(
                "LISTEN on channel %s stopped (%s): delivering by polling every "
                "%s s while it is reopened",
                self.channel_name,
                reason,
                self.poll_interval,
            )
            if not await self._reopen_listen(ended, waits):
                return
            reopened_at = loop.time()

    async def _read_notified_ids(self, notified: AsyncQueue[Optional[int]]) -> str:
        """Queue the id of each NOTIFY until reading LISTEN ends; say why it did."""
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
            return repr(error)
        return "its iterator ended"

    async def _reopen_listen(self, ended: Event, waits: _ReopenWaits) -> bool:
        """
        Reopen the LISTEN connection, retrying until it works.

        Args:
            ended: set once the listen() that reads the connection ends.
            waits: the waits before each attempt; each failure counts there.

        Returns:
            True once the connection is open again, False if that listen()
            ended first.
        """
        while not await _is_set_within(ended, waits.next_wait()):
            # Whatever is left of the old connection; a dead one may refuse.
            try:
                await self._close(self.listen_driver.on_shutdown())
            except Exception:
                logger.debug("Closing the old LISTEN connection failed", exc_info=True)
            try:
                await wait_for(self.listen_driver.on_startup(), self.query_timeout)
            except Exception as error:
                waits.failed()
                logger.warning(
                    "Could not reopen the LISTEN connection of channel %s (%r); "
                    "trying again",
                    self.channel_name,
                    error,
                )
                continue
            if ended.is_set():
                # wait_for() may return a result that raced its cancellation.
                await self._close(self.listen_driver.on_shutdown())
                return False
            logger.info("LISTEN on channel %s reopened", self.channel_name)
            return True
        return False

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

        Args:
            what: the claimed message, as the log names it.
            query: the claim query, run through ``fetch_row``.
            values: the query's bind parameters.

        Returns:
            The message, or None if no row matched or the claim failed.
        """
        try:
            # Not cancelled from here: asyncpg and psycopg bound the claim by
            # query_timeout themselves, and cancelling it would leave psycopg
            # waiting on a silent socket, see drivers/_psycopg.py. psqlpy's
            # claims are not bounded at all, see the README.
            row = await self.driver.fetch_row(query, *values)
        except AsyncTimeoutError:
            # A connection that went silent: a traceback would only show the
            # wait. asyncpg and psycopg give that connection up.
            logger.error(
                "Claiming %s on channel %s got no answer within %s s",
                what,
                self.channel_name,
                self.query_timeout,
            )
            return None
        except Exception:
            logger.exception("Error claiming %s on channel %s", what, self.channel_name)
            return None
        if row is None:
            return None
        return AckableMessage(data=row[self.columns.message.name], ack=_ack)
