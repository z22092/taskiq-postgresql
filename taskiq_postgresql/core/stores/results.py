"""The result store: the result backend's table, through its query driver."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from taskiq_postgresql.core.stores.schema import TableStore
from taskiq_postgresql.exceptions import ResultIsMissingError

if TYPE_CHECKING:
    from datetime import date, datetime

    from taskiq_postgresql.core.stores.schema import SchemaStore
    from taskiq_postgresql.result_backend import Table


class ResultStore(TableStore):
    """The serialized results of one result backend."""

    def __init__(self, schema: SchemaStore, table: Table) -> None:
        """
        Hold the store of the table and its columns.

        Args:
            schema: The store of the table.
            table: The columns of the result backend's table.
        """
        super().__init__(schema)
        self._table = table

    async def set(self, task_id: Any, data: bytes) -> None:
        """
        Store the serialized result of a task, replacing an earlier one.

        Args:
            task_id: The id of the task.
            data: The serialized result.
        """
        columns = self._table
        # Without ``returning``, as the backend has always called it and every
        # bundled driver defaults it, though the abstract signature has none.
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
            Whether there is a row for the task with a result in it: a row that
            holds only progress is not a result yet.
        """
        return await self.driver.exists(task_id, not_null=[self._table.result])

    async def get(self, task_id: Any) -> Any:
        """
        Return the serialized result of a task.

        Args:
            task_id: The id of the task.

        Returns:
            The serialized result.

        Raises:
            ResultIsMissingError: if no result is stored for the task, also
                when its row holds only progress.
        """
        columns = self._table
        rows = await self.driver.select(
            [columns.result],
            [columns.primary_key],
            [task_id],
            not_null=[columns.result],
        )
        if len(rows) == 0:
            raise ResultIsMissingError(
                f"Cannot find record with task_id = {task_id} in PostgreSQL",
            )
        return next(iter(rows))["result"]

    async def set_progress(self, task_id: Any, data: bytes) -> None:
        """
        Store the serialized progress of a task, replacing an earlier one.

        Only the ``progress`` column is written, so the result of the same
        task, if one is stored, stays as it is; ``set`` leaves the progress
        alone the same way.

        Args:
            task_id: The id of the task.
            data: The serialized progress.
        """
        columns = self._table
        # Without ``returning``, as ``set`` calls it: see the comment there.
        await self.driver.insert_or_update(  # ty: ignore[missing-argument]
            [columns.primary_key, columns.progress],
            [task_id, data],
            [columns.primary_key],
            [columns.progress],
        )

    async def get_progress(self, task_id: Any) -> Optional[bytes]:
        """
        Return the serialized progress of a task.

        Args:
            task_id: The id of the task.

        Returns:
            The serialized progress, or ``None`` when the task has no row or
            its row holds no progress.
        """
        columns = self._table
        rows = await self.driver.select(
            [columns.progress],
            [columns.primary_key],
            [task_id],
        )
        if len(rows) == 0:
            return None
        found = next(iter(rows))["progress"]
        # Not every driver returns BYTEA as bytes; the caller parses bytes.
        return None if found is None else bytes(found)

    async def delete(self, task_id: Any) -> None:
        """
        Delete the row of a task: its result and its progress.

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
