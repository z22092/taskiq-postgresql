from dataclasses import dataclass
from typing import Any, Callable, Final, Literal, Optional, Union
from uuid import uuid4

from taskiq import ScheduledTask, ScheduleSource
from taskiq.abc.broker import AsyncBroker
from taskiq.compat import model_dump_json, model_validate_json

from taskiq_postgresql.abc.query import (
    Column,
    CreatedAtColumn,
    PrimaryKeyColumn,
    UpdatedAtColumn,
)
from taskiq_postgresql.utils import get_db_driver

__all__ = ["PostgresqlSchedulerSource"]


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
        dsn: Union[
            str,
            Callable[
                [],
                str,
            ],
        ] = "postgresql://postgres:postgres@localhost:5432/postgres",
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
            dsn: PostgreSQL connection string. Defaults to a local PostgreSQL instance.
                Format: "postgres://user:password@host:port/database"
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
            **connect_kwargs: Additional keyword arguments passed to the database
                connection pool. These are driver-specific connection parameters.

        Example:
            ```python
            scheduler = AsyncPgSchedulerSource(
                dsn="postgres://user:pass@localhost:5432/mydb",
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
        """
        self._dsn: Final = dsn
        self.table_name: Final = table_name
        self.connect_kwargs: Final = connect_kwargs
        self.columns = Table()
        self.startup_schedule = startup_schedule
        self.broker = broker
        self.driver = get_db_driver(driver)(
            dsn,
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
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    async def startup(self) -> None:
        """
        Create the table and index for the schedules.

        Raises:
            DatabaseConnectionError: if the connection to the database fails.
        """
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
        """Get the schedules from the database."""
        schedule_rows = await self.driver.select(
            [self.columns.schedule],
        )

        return [
            model_validate_json(ScheduledTask, schedule_row["schedule"])
            if isinstance(schedule_row["schedule"], str)
            else ScheduledTask(**schedule_row["schedule"])
            for schedule_row in schedule_rows
        ]

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """Add a schedule to the database."""
        await self.driver.insert(
            [self.columns.primary_key, self.columns.task_name, self.columns.schedule],
            [
                schedule.schedule_id,
                schedule.task_name,
                model_dump_json(
                    schedule,
                ),
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
