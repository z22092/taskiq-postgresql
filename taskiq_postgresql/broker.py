from __future__ import annotations

from asyncio import Queue as AsyncQueue
from asyncio import Task, get_running_loop
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, TypeVar, Union

from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_postgresql.abc.driver import ListenDriver
from taskiq_postgresql.abc.query import Column, CreatedAtColumn, PrimaryKeyColumn
from taskiq_postgresql.utils import get_db_driver, get_db_listen_driver

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


_T = TypeVar("_T")

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
        self._queue: AsyncQueue[str] | None = None

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
        self.listen_driver: ListenDriver = get_db_listen_driver(driver)(
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
        await super().startup()

        await self.driver.on_startup()
        await self.listen_driver.on_startup()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        await self.driver.on_shutdown()
        await self.listen_driver.on_shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
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
                message.labels,
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
        while True:
            try:
                async for message_id in self.listen_driver:
                    # Normalize payload to integer ID (psycopg may yield string payloads).
                    try:
                        normalized_id = int(message_id)  # type: ignore[arg-type]
                    except (TypeError, ValueError):
                        logger.warning(
                            "Invalid NOTIFY payload %r on channel %s",
                            message_id,
                            self.channel_name,
                        )
                        continue

                    # Atomically claim the message row. If None is returned, another
                    # worker has already claimed it.
                    row = await self.driver.delete_returning(
                        self.columns.primary_key,
                        normalized_id,
                        [self.columns.message],
                    )

                    if row is None:
                        # Claimed elsewhere or missing; skip.
                        continue

                    message: Optional[bytes] = row.get(self.columns.message.name)

                    if message is None:
                        logger.warning(
                            "Message with id %s has no payload.",
                            message_id,
                        )
                        continue

                    async def ack(*, _message_id: int = message_id) -> None:  # noqa: ARG001
                        # No-op: the row was already deleted when claimed.
                        return None

                    yield AckableMessage(data=message, ack=ack)
            except Exception as error:
                logger.exception("Error processing message: %s", error)
                continue
