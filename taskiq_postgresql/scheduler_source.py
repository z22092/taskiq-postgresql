import json
import math
from dataclasses import dataclass
from datetime import timedelta
from logging import getLogger
from typing import Any, Final, Literal, Optional
from uuid import uuid4

from pydantic import ValidationError
from taskiq import ScheduledTask, ScheduleSource
from taskiq.abc.broker import AsyncBroker

from taskiq_postgresql.abc.query import (
    Column,
    CreatedAtColumn,
    PrimaryKeyColumn,
    UpdatedAtColumn,
)
from taskiq_postgresql.utils import (
    Dsn,
    dsn_before_startup,
    get_db_driver,
    require_dsn,
    resolve_dsn,
)

__all__ = ["PostgresqlSchedulerSource"]

logger = getLogger(__name__)

# The largest whole-day span a timedelta holds, in seconds.
_MAX_OFFSET_SECONDS = timedelta.max.days * 24 * 60 * 60


def _load_schedule(stored: Any) -> ScheduledTask:
    """
    Validate a stored schedule, accepting the 0.4.x pydantic v1 form.

    Raises:
        ValueError: if the schedule is not JSON, or not a valid
            ``ScheduledTask`` (pydantic's ``ValidationError``).
        TypeError: if the schedule is JSON but not an object.
    """
    data = json.loads(stored) if isinstance(stored, (str, bytes)) else stored
    if not isinstance(data, dict):
        msg = f"expected a JSON object, got {type(data).__name__}"
        raise TypeError(msg)
    data = dict(data)
    offset = data.get("cron_offset")
    # pydantic v1 wrote a timedelta as its total seconds. taskiq 0.13 reads a
    # number as a time zone name and rejects it, and a number can only have
    # come from a timedelta, so it is read back as one. An offset timedelta
    # cannot hold stays a number and fails validation like any other bad row.
    if (
        isinstance(offset, (int, float))
        and not isinstance(offset, bool)
        and math.isfinite(offset)
        and abs(offset) <= _MAX_OFFSET_SECONDS
    ):
        data["cron_offset"] = timedelta(seconds=offset)
    return ScheduledTask.model_validate(data)


def _describe_error(error: Exception) -> str:
    # The error type and, for a validation error, the ScheduledTask fields it
    # names; never the stored values, which can hold user data.
    if isinstance(error, ValidationError):
        fields = sorted(
            {
                str(item["loc"][0])
                for item in error.errors(include_url=False, include_input=False)
                if item["loc"]
            },
        )
        return f"ValidationError in {', '.join(fields) or 'the schedule'}"
    return type(error).__name__


@dataclass
class Table:
    """Columns for the result backend."""

    primary_key = PrimaryKeyColumn(
        name="id",
        type_="UUID",
    )
    task_name = Column(
        name="task_name",
        type_="VARCHAR(100)",
    )
    schedule = Column(
        name="schedule",
        type_="JSONB",
    )
    created_at = CreatedAtColumn()
    updated_at = UpdatedAtColumn()


class PostgresqlSchedulerSource(ScheduleSource):
    """Schedule source for PostgreSQL."""

    def __init__(
        self,
        dsn: Dsn,
        table_name: str = "taskiq_schedulers",
        driver: Literal["asyncpg", "psqlpy", "psycopg", "pg8000"] = "asyncpg",
        startup_schedule: Optional[dict[str, list[dict[str, Any]]]] = None,
        broker: Optional[AsyncBroker] = None,
        run_migrations: bool = False,
        **connect_kwargs: Any,
    ) -> None:
        """Initialize the PostgreSQL scheduler source.

        Sets up a scheduler source that stores scheduled tasks in a PostgreSQL database.
        This scheduler source manages task schedules, allowing for persistent storage \
            and
        retrieval of scheduled tasks across application restarts.

        Args:
            dsn: PostgreSQL connection string, or a callable returning one. A
                callable is resolved once, by startup(). Format:
                "postgres://user@host:port/database"
            table_name: Name of the table to store scheduled tasks. Will be created
                automatically if it doesn't exist. Defaults to "taskiq_schedulers".
            driver: Database driver to use for connections. Currently only "asyncpg"
                is supported.
            startup_schedule: Dictionary of task schedules to automatically add when
                the scheduler starts up. Format: {task_name: [schedule_configs]}.
                Each schedule_config should contain 'cron' or 'time' keys along with
                optional 'args', 'kwargs', and 'cron_offset' keys.
            broker: The TaskIQ broker instance to use for finding and managing tasks.
                Required if startup_schedule is provided.
            run_migrations: Create the table and indexes on startup. Defaults to
                False.
            **connect_kwargs: Additional keyword arguments passed to the database
                connection pool. These are driver-specific connection parameters.

        Example:
            ```python
            scheduler = AsyncPgSchedulerSource(
                dsn="postgres://user@localhost:5432/mydb",
                table_name="my_schedules",
                startup_schedule={
                    "my_task": [
                        {"cron": "0 */6 * * *", "cron_offset": "UTC",}}
                    ]
                },
                broker=my_broker,
                max_connections=20,  # connect_kwargs example
            )
            ```

        Note:
            The database table and required indexes will be created automatically
            during the startup() method call.

        Raises:
            TypeError: if ``dsn`` is ``None``, empty or blank.
        """
        dsn = require_dsn(dsn, type(self).__name__)
        self._dsn: Final[Dsn] = dsn
        self.table_name: Final = table_name
        self.connect_kwargs: Final = connect_kwargs
        self.columns = Table()
        self.startup_schedule = startup_schedule
        self.broker = broker
        self.driver = get_db_driver(driver)(
            dsn_before_startup(dsn),
            table_name,
            columns=[
                self.columns.task_name,
                self.columns.schedule,
                self.columns.updated_at,
            ],
            primary_key=self.columns.primary_key,
            created_at=self.columns.created_at,
            index_columns=[self.columns.primary_key, self.columns.task_name],
            run_migrations=run_migrations,
            **connect_kwargs,
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
        Create the table and index for the schedules.

        Raises:
            TypeError: if the DSN callable returns ``None`` or an empty string.
            DatabaseConnectionError: if the connection to the database fails.
        """
        self.driver.connection_string = self.dsn
        await self.driver.on_startup()

        if self.startup_schedule is not None and self.broker is not None:
            existing_schedules = {
                schedule.task_name for schedule in await self.get_schedules()
            }

            for task_name, schedules in self.startup_schedule.items():
                if task_name in existing_schedules:
                    continue

                task = self.broker.find_task(task_name)

                if task is None:
                    continue

                for schedule in schedules:
                    if "cron" not in schedule and "time" not in schedule:
                        continue

                    await self.add_schedule(
                        ScheduledTask(
                            task_name=task_name,
                            labels=task.labels,
                            schedule_id=uuid4().hex,
                            args=schedule.get("args", []),
                            kwargs=schedule.get("kwargs", {}),
                            cron=schedule.get("cron"),
                            time=schedule.get("time"),
                            cron_offset=schedule.get("cron_offset"),
                        ),
                    )

    async def shutdown(self) -> None:
        """Close the connection pool."""
        await self.driver.on_shutdown()

    async def get_schedules(self) -> list[ScheduledTask]:
        """
        Get the schedules from the database.

        A row that is not a valid ``ScheduledTask`` is logged and skipped, so
        one bad row does not stop every other schedule.
        """
        schedule_rows = await self.driver.select(
            [self.columns.primary_key, self.columns.schedule],
        )

        schedules: list[ScheduledTask] = []
        for schedule_row in schedule_rows:
            try:
                schedules.append(
                    _load_schedule(schedule_row[self.columns.schedule.name]),
                )
            except (ValidationError, ValueError, TypeError, OverflowError) as error:
                logger.warning(
                    "Skipping schedule %s in table %s: it cannot be read as a "
                    "ScheduledTask (%s)",
                    schedule_row[self.columns.primary_key.name],
                    self.table_name,
                    _describe_error(error),
                )
        return schedules

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """Add a schedule to the database."""
        await self.driver.insert(
            [self.columns.primary_key, self.columns.task_name, self.columns.schedule],
            [
                schedule.schedule_id,
                schedule.task_name,
                schedule.model_dump_json(),
            ],
        )

    async def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule from the database."""
        await self.driver.delete(
            self.columns.primary_key,
            schedule_id,
        )

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
