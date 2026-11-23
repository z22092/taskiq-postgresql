from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final, Literal, Optional, TypeVar, Union

from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers.pickle import PickleSerializer

from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.exceptions import ResultIsMissingError
from taskiq_postgresql.utils import (
    Dsn,
    dsn_before_startup,
    get_db_driver,
    require_dsn,
    resolve_dsn,
)

_ReturnType = TypeVar("_ReturnType")


@dataclass
class Table:
    """Columns for the result backend."""

    primary_key: PrimaryKeyColumn
    result: Column = Column(  # noqa: RUF009
        name="result",
        type_="BYTEA",
    )


class PostgresqlResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for TaskIQ based on Asyncpg."""

    pg_backend: QueryDriver

    def __init__(
        self,
        dsn: Dsn,
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: Literal["VarChar", "Text", "Uuid"] = "Uuid",
        serializer: Optional[TaskiqSerializer] = None,
        driver: Literal["asyncpg", "psqlpy", "psycopg"] = "asyncpg",
        run_migrations: bool = False,
        **connect_kwargs: Any,
    ) -> None:
        """
        Construct new result backend.

        Args:
            dsn: connection string to PostgreSQL, or callable returning one.
                A callable is resolved once, by startup().
            keep_results: flag to not remove results from Redis after reading.
            table_name: name of the table that stores the results.
            field_for_task_id: SQL type of the ``task_id`` column.
            serializer: serializer of the results; ``PickleSerializer`` when
                ``None``.
            driver: driver to use.
            run_migrations: create the table and its index on startup.
            connect_kwargs: additional arguments for nats `ConnectionPool` class.

        Raises:
            TypeError: if ``dsn`` is ``None``, empty or blank.
        """
        dsn = require_dsn(dsn, type(self).__name__)
        self._dsn: Final[Dsn] = dsn
        self.keep_results: Final = keep_results
        self.table_name: Final = table_name
        self.field_for_task_id: Final = field_for_task_id
        self.serializer: Final = serializer or PickleSerializer()
        self.connect_kwargs: Final = connect_kwargs

        self.columns = Table(
            primary_key=PrimaryKeyColumn(
                name="task_id",
                type_=field_for_task_id,
            ),
        )

        self.driver = get_db_driver(driver)(
            dsn_before_startup(dsn),
            table_name,
            columns=[
                self.columns.result,
            ],
            primary_key=self.columns.primary_key,
            index_columns=[self.columns.primary_key],
            run_migrations=run_migrations,
            **connect_kwargs,
        )

    @property
    def dsn(self) -> str:
        """
        Get the DSN string, calling the DSN callable if one was given.

        Raises:
            TypeError: if the DSN callable returns ``None`` or an empty
                string.
        """
        return resolve_dsn(self._dsn, type(self).__name__)

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool
        and create new table for results if not exists.

        Raises:
            TypeError: if the DSN callable returns ``None`` or an empty
                string.
        """
        self.driver.connection_string = self.dsn
        await self.driver.on_startup()

    async def shutdown(self) -> None:
        """Close the connection pool."""

    async def set_result(
        self,
        task_id: Any,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Set result to the PostgreSQL table.

        Args:
            task_id (Any): ID of the task.
            result (TaskiqResult[_ReturnType]):  result of the task.

        """
        await self.driver.insert_or_update(
            [
                self.columns.primary_key,
                self.columns.result,
            ],
            [
                task_id,
                self.serializer.dumpb(result),
            ],
            [
                self.columns.primary_key,
            ],
            [
                self.columns.result,
            ],
        )

    async def is_result_ready(self, task_id: Any) -> bool:
        """
        Returns whether the result is ready.

        Args:
            task_id (Any): ID of the task.

        Returns:
            bool: True if the result is ready else False.

        """
        return await self.driver.exists(task_id)

    async def get_result(
        self,
        task_id: Any,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Retrieve result from the task.

        Args:
            task_id: task's id.
            with_logs: if True it will download task's logs.

        Returns:
            TaskiqResult.

        Raises:
            ResultIsMissingError: if there is no result when trying to get it.
        """
        data = await self.driver.select(
            [
                self.columns.result,
            ],
            [
                self.columns.primary_key,
            ],
            [task_id],
        )

        if len(data) == 0:
            raise ResultIsMissingError(
                f"Cannot find record with task_id = {task_id} in PostgreSQL",
            )

        result_in_bytes = next(iter(data))["result"]

        if not self.keep_results:
            await self.driver.delete(
                self.columns.primary_key,
                task_id,
            )

        taskiq_result: TaskiqResult[_ReturnType] = self.serializer.loadb(
            result_in_bytes,
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def delete_by_date(
        self,
        from_date: Union[datetime, date],
        to_date: Optional[Union[datetime, date]] = None,
    ) -> None:
        """
        Delete results by date.

        Args:
            from_date (datetime | date): Date from which to delete results.
            to_date (datetime | date | None): Date to which to delete results.
        """
        await self.driver.delete_by_date(from_date, to_date)
