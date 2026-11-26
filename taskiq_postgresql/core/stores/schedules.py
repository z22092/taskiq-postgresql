"""The schedule store: the scheduler source's table, through its query driver."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.core.stores.schema import SchemaStore
    from taskiq_postgresql.scheduler_source import Table


class ScheduleStore:
    """
    The schedules of one scheduler source.

    Attributes:
        schema: The store of the table, which starts, closes and describes it.
    """

    def __init__(self, schema: SchemaStore, table: Table) -> None:
        """
        Hold the store of the table and its columns.

        Args:
            schema: The store of the table.
            table: The columns of the scheduler source's table.
        """
        self.schema = schema
        self._table = table

    @property
    def driver(self) -> QueryDriver:
        """The query driver in use."""
        return self.schema.driver

    def bind(self, dsn: str) -> None:
        """Point the driver at ``dsn`` and tell it the source owns the table."""
        self.schema.bind(dsn)

    async def start(self) -> None:
        """Check or migrate the table."""
        await self.schema.start()

    async def close(self) -> None:
        """Close the driver's pool."""
        await self.schema.close()

    async def rows(self) -> Any:
        """
        Read every schedule.

        Returns:
            The rows, each a dict of the id and the stored schedule by column
            name.
        """
        columns = self._table
        return await self.driver.select([columns.primary_key, columns.schedule])

    async def add(self, schedule_id: Any, task_name: str, schedule_json: str) -> None:
        """
        Store a schedule.

        Args:
            schedule_id: The id of the schedule.
            task_name: The task it runs.
            schedule_json: The schedule as JSON.
        """
        columns = self._table
        # Two arguments, as the source always sent: the bundled drivers default
        # ``returning`` to none, though the abstract signature has none.
        await self.driver.insert(
            [columns.primary_key, columns.task_name, columns.schedule],
            [schedule_id, task_name, schedule_json],
        )

    async def delete(self, schedule_id: Any) -> None:
        """
        Delete a schedule.

        Args:
            schedule_id: The id of the schedule.
        """
        await self.driver.delete(self._table.primary_key, schedule_id)
