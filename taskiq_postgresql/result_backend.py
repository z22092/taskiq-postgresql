from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Final, Literal, Optional, TypeVar, Union

from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers.pickle import PickleSerializer

from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.exceptions import ResultIsMissingError
from taskiq_postgresql.utils import get_db_driver

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
        dsn: Union[
            str,
            Callable[
                [],
                str,
            ],
        ] = "postgresql://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: Literal["VarChar", "Text", "Uuid"] = "Uuid",
        serializer: Optional[TaskiqSerializer] = None,
        driver: Literal["asyncpg", "psqlpy", "psycopg", "pg8000"] = "asyncpg",
        run_migrations: bool = False,
        **connect_kwargs: Any,
    ) -> None:
        """
        Construct new result backend.

        :param dsn: connection string to PostgreSQL.
        :param keep_results: flag to not remove results from Redis after reading.
        :param connect_kwargs: additional arguments for nats `ConnectionPool` class.
        """
        self._dsn: Final = dsn
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
            dsn,
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
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool
        and create new table for results if not exists.
        """
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

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
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
