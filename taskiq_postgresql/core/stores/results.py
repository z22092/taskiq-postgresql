"""The result store: the result backend's table, through its query driver."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from taskiq_postgresql.exceptions import ResultIsMissingError

if TYPE_CHECKING:
    from datetime import date, datetime

    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.core.stores.schema import SchemaStore
    from taskiq_postgresql.result_backend import Table


class ResultStore:
    """
    The serialized results of one result backend.

    Attributes:
        schema: The store of the table, which starts, closes and describes it.
    """

    def __init__(self, schema: SchemaStore, table: Table) -> None:
        """
        Hold the store of the table and its columns.

        Args:
            schema: The store of the table.
            table: The columns of the result backend's table.
        """
        self.schema = schema
        self._table = table

    @property
    def driver(self) -> QueryDriver:
        """The query driver in use; assigning one makes the store use it."""
        return self.schema.driver

    @driver.setter
    def driver(self, driver: QueryDriver) -> None:
        self.schema.driver = driver

    def bind(self, dsn: str) -> None:
        """Point the driver at ``dsn`` and tell it the backend owns the table."""
        self.schema.bind(dsn)

    async def start(self) -> None:
        """Check or migrate the table."""
        await self.schema.start()

    async def close(self) -> None:
        """Close the driver's pool."""
        await self.schema.close()

    async def set(self, task_id: Any, data: bytes) -> None:
        """
        Store the serialized result of a task, replacing an earlier one.

        Args:
            task_id: The id of the task.
            data: The serialized result.
        """
        columns = self._table
        # Four arguments, as the backend always sent: the bundled drivers
        # default ``returning`` to none, though the abstract signature has none.
        await self.driver.insert_or_update(  # ty: ignore[missing-argument]
            [columns.primary_key, columns.result],
            [task_id, data],
            [columns.primary_key],
            [columns.result],
        )

    async def exists(self, task_id: Any) -> bool:
        """
        Tell whether a result is stored.

        Args:
            task_id: The id of the task.

        Returns:
            Whether there is a row for the task.
        """
        return await self.driver.exists(task_id)

    async def get(self, task_id: Any) -> Any:
        """
        Return the serialized result of a task.

        Args:
            task_id: The id of the task.

        Returns:
            The serialized result.

        Raises:
            ResultIsMissingError: if no result is stored for the task.
        """
        columns = self._table
        rows = await self.driver.select(
            [columns.result],
            [columns.primary_key],
            [task_id],
        )
        if len(rows) == 0:
            raise ResultIsMissingError(
                f"Cannot find record with task_id = {task_id} in PostgreSQL",
            )
        return next(iter(rows))["result"]

    async def delete(self, task_id: Any) -> None:
        """
        Delete the result of a task.

        Args:
            task_id: The id of the task.
        """
        await self.driver.delete(self._table.primary_key, task_id)

    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]],
    ) -> None:
        """
        Delete the results created in a span, both bounds inclusive.

        Args:
            from_date: Delete results created at or after it.
            to_date: Delete only results created at or before it; ``None``
                sets no upper bound.
        """
        await self.driver.delete_by_date(from_date, to_date)
