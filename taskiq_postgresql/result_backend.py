from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final, Literal, Optional, TypeVar, Union

from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.serializers.pickle import PickleSerializer

from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.core.sql.catalog import (
    Column,
    CreatedAtColumn,
    DeclaredStep,
    PrimaryKeyColumn,
)
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.core.stores.results import ResultStore
from taskiq_postgresql.core.stores.schema import SchemaStore, build_driver
from taskiq_postgresql.utils import (
    Dsn,
    dsn_before_startup,
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
    progress: Column = Column(  # noqa: RUF009
        name="progress",
        type_="BYTEA",
        nullable=True,
    )


# Step 2 of the results table, the one definition: the backend declares it and
# ``migrations`` describes the same step for a kind. ``result`` stops being
# NOT NULL so that a row can hold progress before it holds a result.
RESULTS_STEPS: Final = (
    DeclaredStep(
        added_columns=[Table.progress],
        nullable_columns=(Table.result.name,),
    ),
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
        store_progress: bool = True,
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
            run_migrations: create the table and its index on startup; without
                it, startup() only checks that they exist.
            store_progress: whether ``set_progress`` writes the progress of a
                task, as JSON, in the ``progress`` column of its result row.
                ``False`` makes it do nothing; ``get_progress`` and the table
                do not depend on it. Pass ``False`` while a process that reads
                results still runs a release without progress: it would
                take a row that holds only progress for a result.
            connect_kwargs: additional arguments for nats `ConnectionPool` class.

        Raises:
            TypeError: if ``dsn`` is ``None``, empty or blank.
        """
        # stacklevel 3: the warning points at the line that built the backend.
        table = Identifier.parse(table_name, what="table name", stacklevel=3)
        dsn = require_dsn(dsn, type(self).__name__)
        self._dsn: Final[Dsn] = dsn
        self.keep_results: Final = keep_results
        self.store_progress: Final = store_progress
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

        declaration = TableDeclaration(
            table=table,
            primary_key=self.columns.primary_key,
            columns=[
                self.columns.result,
            ],
            created_at=CreatedAtColumn(),
            index_columns=[self.columns.primary_key],
            steps=RESULTS_STEPS,
        )
        self.driver = build_driver(
            "result_backend",
            declaration,
            driver,
            dsn_before_startup(dsn),
            run_migrations=run_migrations,
            **connect_kwargs,
        )
        self._store = ResultStore(
            SchemaStore("result_backend", declaration, lambda: self.driver),
            self.columns,
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
            SchemaNotReadyError: with ``run_migrations=False``, if the table
                or one of its columns is missing; see
                ``QueryDriver.on_startup``.
        """
        # Binds again here, for a driver swapped in after the constructor.
        self._store.bind(self.dsn)
        await self._store.start()

    async def shutdown(self) -> None:
        """
        Close the connection pool.

        startup() opens it to read the catalog, so a backend that never reads
        a result holds one too. Nothing to close before startup(), or after a
        previous shutdown().
        """
        await self._store.close()

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
        await self._store.set(task_id, self.serializer.dumpb(result))

    async def set_progress(
        self,
        task_id: Any,
        progress: TaskProgress[Any],
    ) -> None:
        """
        Store the progress of a task in its result row.

        The progress is JSON, whatever ``serializer`` is: the column is read
        back by this backend only, and JSON needs no ``pickle.loads`` of what
        a database holds. A row that has no result yet is created. It does
        nothing when ``store_progress`` is ``False``.

        Args:
            task_id: ID of the task.
            progress: The progress; its ``meta`` must be JSON serializable.

        Raises:
            ValueError: if ``progress`` cannot be written as JSON. Nothing is
                written.
        """
        if not self.store_progress:
            return
        await self._store.set_progress(
            task_id,
            progress.model_dump_json().encode("utf-8"),
        )

    async def get_progress(self, task_id: Any) -> Optional[TaskProgress[Any]]:
        """
        Return the progress of a task.

        Args:
            task_id: ID of the task.

        Returns:
            The progress, or ``None`` when the task has no row or its row
            holds no progress.

        Raises:
            ValueError: if the column holds something that is not progress
                written as JSON.
        """
        data = await self._store.get_progress(task_id)
        if data is None:
            return None
        return TaskProgress[Any].model_validate_json(data)

    async def is_result_ready(self, task_id: Any) -> bool:
        """
        Returns whether the result is ready.

        A task whose row holds only progress is not ready.

        Args:
            task_id (Any): ID of the task.

        Returns:
            bool: True if the result is ready else False.

        """
        return await self._store.exists(task_id)

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
            ResultIsMissingError: if there is no result when trying to get it,
                also when the task's row holds only progress. That row is not
                deleted, even with ``keep_results=False``.
        """
        result_in_bytes = await self._store.get(task_id)

        if not self.keep_results:
            await self._store.delete(task_id)

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

        Both bounds are inclusive. Prefer time-zone-aware datetimes: a ``date``
        counts as midnight at the start of that day, in a time zone that depends
        on the driver, so ``to_date`` as a ``date`` keeps that day's results.

        Args:
            from_date (datetime | date): Delete results created at or after it.
            to_date (datetime | date | None): Delete only results created at or
                before it. Left out, every result from ``from_date`` on goes.
        """
        await self._store.delete_by_date(from_date, to_date)
