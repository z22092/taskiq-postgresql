from __future__ import annotations

from asyncio import (
    CancelledError,
    Event,
    Lock,
    Task,
    TimeoutError,
    create_task,
    get_running_loop,
    wait_for,
)
from dataclasses import dataclass
from logging import getLogger
from random import uniform
from time import monotonic
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, TypeVar, Union

from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_postgresql.abc.driver import ListenDriver
from taskiq_postgresql.abc.query import (
    BROKER_CHANNEL_LABEL,
    Column,
    CreatedAtColumn,
    PrimaryKeyColumn,
)
from taskiq_postgresql.exceptions import (
    ListenerDisconnectedError,
    TransientDatabaseConnectionError,
)
from taskiq_postgresql.utils import get_db_driver, get_db_listen_driver

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator


_T = TypeVar("_T")
_RECONCILIATION_BATCH_SIZE = 100
_MAX_LISTENER_RETRY_DELAY = 30.0

logger = getLogger("taskiq.asyncpg_broker")


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


class PostgresqlBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: Union[
            str,
            Callable[
                [],
                str,
            ],
        ] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        field_for_task_id: Literal["VarChar", "Text", "Uuid"] = "Uuid",
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        driver: Literal["asyncpg", "psqlpy", "psycopg", "pg8000"] = "asyncpg",
        max_retry_attempts: int = 5,
        connection_kwargs: dict[str, Any] | None = None,
        pool_kwargs: dict[str, Any] | None = None,
        run_migrations: bool = False,
    ) -> None:
        """
        Construct a new broker.

        Args:
            dsn (Union[str, Callable[[], str]], optional): \
                connection string to PostgreSQL, or callable returning one.
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
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: str | Callable[[], str] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.connection_kwargs: dict[str, Any] = (
            connection_kwargs if connection_kwargs else {}
        )
        self.pool_kwargs: dict[str, Any] = pool_kwargs if pool_kwargs else {}
        self.max_retry_attempts: int = max_retry_attempts
        self._driver_name = driver
        self._shutdown_event = Event()
        self._lifecycle_lock = Lock()
        self._listener_recovery_lock = Lock()
        self._shutdown_requests = 0
        self._base_started = False
        self._query_driver_started = False
        self._listener_started = False
        self._listener_generation = 0
        self._pending_listen_driver: ListenDriver | None = None
        self._listener_start_task: Task[None] | None = None

        self.columns = Table(task_id=Column(name="task_id", type_=field_for_task_id))

        self.driver = get_db_driver(driver)(
            connection_string=self.dsn,
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
            **self.connection_kwargs,
        )
        self._listen_driver_factory = get_db_listen_driver(driver)
        self.listen_driver: ListenDriver = self._new_listen_driver()

    def _new_listen_driver(self) -> ListenDriver:
        """Create an isolated listener generation."""
        return self._listen_driver_factory(
            connection_string=self.dsn,
            channel_name=self.channel_name,
            **self.connection_kwargs,
        )

    @property
    def dsn(self) -> str:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    async def startup(self) -> None:
        """Initialize the broker."""
        async with self._lifecycle_lock:
            if self._shutdown_requests:
                raise RuntimeError("Broker shutdown is in progress.")
            if self._base_started:
                return
            self._shutdown_event.clear()
            try:
                self._base_started = True
                await super().startup()
                self._raise_if_shutdown_requested()

                self._query_driver_started = True
                await self.driver.on_startup()
                self._raise_if_shutdown_requested()

                self._listener_started = True
                startup_task = create_task(self.listen_driver.on_startup())
                self._listener_start_task = startup_task
                try:
                    await startup_task
                finally:
                    if self._listener_start_task is startup_task:
                        self._listener_start_task = None
                self._raise_if_shutdown_requested()
                self._listener_generation = 1
            except BaseException:
                self._shutdown_event.set()
                await self._shutdown_active_listener()
                await self._shutdown_query_driver()
                await self._shutdown_base_broker()
                raise

    def _raise_if_shutdown_requested(self) -> None:
        """Cancel startup when shutdown won the lifecycle race."""
        if self._shutdown_event.is_set():
            raise CancelledError

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        self._shutdown_requests += 1
        self._shutdown_event.set()
        startup_task = self._listener_start_task
        if startup_task is not None and not startup_task.done():
            startup_task.cancel()
        try:
            async with self._lifecycle_lock:
                errors = [
                    await self._cancel_listener_startup(),
                    await self._shutdown_pending_listener(),
                    await self._shutdown_base_broker(),
                    await self._shutdown_query_driver(),
                    await self._shutdown_active_listener(),
                ]
                first_error = next(
                    (error for error in errors if error is not None),
                    None,
                )
                if first_error is not None:
                    raise first_error
        finally:
            self._shutdown_requests -= 1

    async def _cancel_listener_startup(self) -> Exception | None:
        """Cancel a listener generation that is still connecting."""
        startup_task, self._listener_start_task = self._listener_start_task, None
        if startup_task is None:
            return None
        if not startup_task.done():
            startup_task.cancel()
        try:
            await startup_task
        except CancelledError:
            return None
        except Exception as error:  # pragma: no cover - defensive cleanup
            return error
        return None

    async def _shutdown_pending_listener(self) -> Exception | None:
        """Close a listener generation that has not been promoted."""
        candidate, self._pending_listen_driver = self._pending_listen_driver, None
        if candidate is None:
            return None
        return await self._shutdown_listener(candidate)

    async def _shutdown_active_listener(self) -> Exception | None:
        """Close the active listener generation once."""
        if not self._listener_started:
            return None
        self._listener_started = False
        return await self._shutdown_listener(self.listen_driver)

    async def _shutdown_listener(
        self,
        listen_driver: ListenDriver,
    ) -> Exception | None:
        """Close a listener without allowing cleanup to hide a primary error."""
        try:
            await listen_driver.on_shutdown()
        except Exception as error:  # pragma: no cover - defensive cleanup
            logger.warning(
                "PostgreSQL listener cleanup failed "
                "(driver=%s channel=%s exception=%s)",
                self._driver_name,
                self.channel_name,
                type(error).__name__,
            )
            return error
        return None

    async def _shutdown_query_driver(self) -> Exception | None:
        """Close the query driver and record its lifecycle state."""
        if not self._query_driver_started:
            return None
        self._query_driver_started = False
        try:
            await self.driver.on_shutdown()
        except Exception as error:  # pragma: no cover - defensive cleanup
            logger.warning(
                "PostgreSQL query driver cleanup failed (driver=%s exception=%s)",
                self._driver_name,
                type(error).__name__,
            )
            return error
        return None

    async def _shutdown_base_broker(self) -> Exception | None:
        """Close Taskiq-owned resources and record their lifecycle state."""
        if not self._base_started:
            return None
        self._base_started = False
        try:
            await super().shutdown()
        except Exception as error:  # pragma: no cover - defensive cleanup
            logger.warning(
                "Taskiq broker cleanup failed (exception=%s)",
                type(error).__name__,
            )
            return error
        return None

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        stored_labels = {
            **message.labels,
            BROKER_CHANNEL_LABEL: self.channel_name,
        }
        message_inserted_id = await self.driver.insert(
            [
                self.columns.task_id,
                self.columns.task_name,
                self.columns.message,
                self.columns.labels,
            ],
            [
                message.task_id,
                message.task_name,
                message.message,
                stored_labels,
            ],
            [
                self.columns.primary_key,
            ],
        )

        delay_value = message.labels.get("delay")
        if delay_value is not None:
            delay_seconds = int(delay_value)
            return await self._schedule_notification(message_inserted_id, delay_seconds)

        return await self._send_notification(message_inserted_id)

    async def _send_notification(self, message_id: int) -> None:
        """Send a notification with the message ID as payload."""
        try:
            await self.driver.execute(f"NOTIFY {self.channel_name}, '{message_id}'")
        except Exception as error:
            logger.exception("Error sending notification: %s", error)
            raise

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedules the next task based on the schedule object."""
        loop = get_running_loop()

        loop_now = loop.time()
        when = loop_now + delay_seconds

        loop.call_at(
            when,
            lambda: Task(
                self._send_notification(message_id),
                loop=loop,
            ),
        )

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        This method atomically claims messages using DELETE ... RETURNING, ensuring
        that only a single worker processes each message even though NOTIFY is
        broadcast to all listeners.

        :yields: AckableMessage instances.
        """
        query_failure_attempt = 0
        while not self._shutdown_event.is_set():
            listen_driver = self.listen_driver
            listener_generation = self._listener_generation
            try:
                async for message in self._messages_from_ids(self._reconcile_ids()):
                    yield message
                query_failure_attempt = 0

                async for message in self._messages_from_ids(listen_driver):
                    yield message

                if self._shutdown_event.is_set():
                    return
                self._handle_listener_completion()
            except ListenerDisconnectedError:
                if not await self._recover_listener(
                    listen_driver,
                    listener_generation,
                ):
                    return
            except TransientDatabaseConnectionError as error:
                query_failure_attempt += 1
                delay = self._listener_retry_delay(query_failure_attempt)
                logger.warning(
                    "PostgreSQL query connection unavailable "
                    "(driver=%s attempt=%s delay=%.3f exception=%s)",
                    self._driver_name,
                    query_failure_attempt,
                    delay,
                    type(error).__name__,
                )
                if await self._wait_for_shutdown(delay):
                    return
            except Exception as error:
                self._handle_unexpected_listener_error(error)

    def _handle_listener_completion(self) -> None:
        """Keep legacy drivers unchanged and recover asyncpg completion."""
        if self._driver_name == "asyncpg":
            raise ListenerDisconnectedError(
                "PostgreSQL listener stopped unexpectedly.",
            )

    def _handle_unexpected_listener_error(self, error: Exception) -> None:
        """Preserve legacy retries while surfacing asyncpg contract failures."""
        if self._driver_name == "asyncpg":
            raise error
        logger.exception("Error processing message: %s", error)

    async def _reconcile_ids(self) -> AsyncGenerator[int, None]:
        """Yield ready row IDs that may have missed a transient notification."""
        through_id = await self.driver.max_value(self.columns.primary_key)
        after_id = 0
        while after_id < through_id:
            message_ids = await self.driver.select_available_ids(
                primary_key=self.columns.primary_key,
                labels=self.columns.labels,
                created_at=self.columns.created_at,
                channel_name=self.channel_name,
                after_id=after_id,
                through_id=through_id,
                limit=_RECONCILIATION_BATCH_SIZE,
            )
            if not message_ids:
                return
            for message_id in message_ids:
                yield message_id
            after_id = message_ids[-1]

    async def _messages_from_ids(
        self,
        message_ids: AsyncIterator[Any],
    ) -> AsyncGenerator[AckableMessage, None]:
        """Claim and decode IDs without advancing past database failures."""
        async for message_id in message_ids:
            message = await self._claim_message(message_id)
            if message is not None:
                yield message

    async def _claim_message(self, message_id: Any) -> AckableMessage | None:
        """Atomically claim one database row and build its Taskiq message."""
        try:
            normalized_id = int(message_id)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid NOTIFY payload on channel %s",
                self.channel_name,
            )
            return None

        row = await self.driver.delete_returning(
            self.columns.primary_key,
            normalized_id,
            [self.columns.message],
        )
        if row is None:
            return None

        message: Optional[bytes] = row.get(self.columns.message.name)
        if message is None:
            logger.warning("Message with id %s has no payload.", normalized_id)
            return None

        async def ack() -> None:
            return None

        return AckableMessage(data=message, ack=ack)

    async def _recover_listener(
        self,
        failed_driver: ListenDriver,
        failed_generation: int,
    ) -> bool:
        """Recover one failed generation exactly once across consumers."""
        async with self._listener_recovery_lock:
            if self._shutdown_event.is_set():
                return False
            if (
                failed_driver is not self.listen_driver
                or failed_generation != self._listener_generation
            ):
                return True
            return await self._recover_current_listener(failed_generation)

    async def _recover_current_listener(self, generation: int) -> bool:
        """Replace a disconnected listener using bounded exponential backoff."""
        disconnected_at = monotonic()
        logger.warning(
            "PostgreSQL listener disconnected (driver=%s channel=%s generation=%s)",
            self._driver_name,
            self.channel_name,
            generation,
        )

        if self._listener_started:
            self._listener_started = False
            await self._shutdown_listener(self.listen_driver)

        attempt = 0
        while not self._shutdown_event.is_set():
            attempt += 1
            delay = self._listener_retry_delay(attempt)
            logger.info(
                "PostgreSQL listener reconnect attempt "
                "(driver=%s channel=%s attempt=%s delay=%.3f)",
                self._driver_name,
                self.channel_name,
                attempt,
                delay,
            )
            if await self._wait_for_shutdown(delay):
                return False

            candidate = self._new_listen_driver()
            if not await self._start_listener_candidate(candidate, attempt):
                continue

            self.listen_driver = candidate
            self._listener_started = True
            self._listener_generation = generation + 1
            logger.info(
                "PostgreSQL listener reconnected "
                "(driver=%s channel=%s attempts=%s downtime=%.3f generation=%s)",
                self._driver_name,
                self.channel_name,
                attempt,
                monotonic() - disconnected_at,
                self._listener_generation,
            )
            return True
        return False

    async def _start_listener_candidate(
        self,
        candidate: ListenDriver,
        attempt: int,
    ) -> bool:
        """Start and validate one listener reconnect candidate."""
        self._pending_listen_driver = candidate
        startup_task = create_task(candidate.on_startup())
        self._listener_start_task = startup_task
        try:
            await startup_task
        except CancelledError:
            await self._shutdown_listener(candidate)
            if self._shutdown_event.is_set():
                return False
            raise
        except TransientDatabaseConnectionError as error:
            await self._shutdown_listener(candidate)
            logger.warning(
                "PostgreSQL listener reconnect failed "
                "(driver=%s channel=%s attempt=%s exception=%s)",
                self._driver_name,
                self.channel_name,
                attempt,
                type(error).__name__,
            )
            return False
        except BaseException:
            await self._shutdown_listener(candidate)
            raise
        finally:
            if self._listener_start_task is startup_task:
                self._listener_start_task = None
            if self._pending_listen_driver is candidate:
                self._pending_listen_driver = None

        if self._shutdown_event.is_set():
            await self._shutdown_listener(candidate)
            return False
        return True

    def _listener_retry_delay(self, attempt: int) -> float:
        """Return capped exponential backoff with bounded jitter."""
        exponent = max(0, min(attempt - 1, 6))
        base_delay = min(0.5 * (2**exponent), _MAX_LISTENER_RETRY_DELAY)
        return min(
            base_delay * uniform(0.8, 1.2),  # noqa: S311
            _MAX_LISTENER_RETRY_DELAY,
        )

    async def _wait_for_shutdown(self, delay: float) -> bool:
        """Wait for either shutdown or the reconnect delay."""
        try:
            await wait_for(self._shutdown_event.wait(), timeout=delay)
        except TimeoutError:
            return False
        return True
