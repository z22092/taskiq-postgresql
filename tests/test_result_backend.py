import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, TypeVar

import pytest
from taskiq import TaskiqResult

from taskiq_postgresql.exceptions import DatabaseConnectionError, ResultIsMissingError
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from tests.helpers import fetch_on_server

_ReturnType = TypeVar("_ReturnType")

_FROM_DATE = datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc)

pytestmark = pytest.mark.anyio


class ResultForTest:
    """Just test class for testing."""

    def __init__(self) -> None:
        """Generates test class for result testing."""
        self.test_arg = uuid.uuid4()


@pytest.fixture
def task_id() -> str:
    """
    Generates ID for taskiq result.

    :returns: uuid as string.
    """
    return str(uuid.uuid4())


@pytest.fixture
def default_taskiq_result() -> TaskiqResult[Any]:
    """
    Generates default TaskiqResult.

    :returns: TaskiqResult with generic result.
    """
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value="Best test ever.",
        execution_time=0.1,
    )


@pytest.fixture
def custom_taskiq_result() -> TaskiqResult[Any]:
    """
    Generates custom TaskiqResult.

    :returns: TaskiqResult with custom class result.
    """
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value=ResultForTest(),
        execution_time=0.1,
    )


async def test_failure_connection_database(missing_database_dsn: str) -> None:
    """Test exception raising in connection database."""
    with pytest.raises(expected_exception=DatabaseConnectionError):
        await PostgresqlResultBackend(
            dsn=missing_database_dsn,
            table_name="postgres_table",
            run_migrations=True,
        ).startup()


async def test_failure_backend_result(
    result_backend: PostgresqlResultBackend[_ReturnType],
    task_id: str,
) -> None:
    """Test exception raising in `get_result` method."""
    with pytest.raises(expected_exception=ResultIsMissingError):
        await result_backend.get_result(task_id=task_id)


async def test_success_backend_default_result_delete_res(
    postgresql_dsn: str,
    postgres_table: str,
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    backend: PostgresqlResultBackend[_ReturnType] = PostgresqlResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        keep_results=False,
        run_migrations=True,
    )
    await backend.startup()

    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    await backend.get_result(task_id=task_id)

    with pytest.raises(expected_exception=ResultIsMissingError):
        await backend.get_result(task_id=task_id)


async def test_success_backend_default_result(
    result_backend: PostgresqlResultBackend[_ReturnType],
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    """
    Tests normal behavior with default result in TaskiqResult.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param nats_urls: urls to NATS.
    """
    await result_backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    result = await result_backend.get_result(task_id=task_id)

    assert result == default_taskiq_result


async def test_success_backend_custom_result(
    result_backend: PostgresqlResultBackend[_ReturnType],
    custom_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    """
    Tests normal behavior with custom result in TaskiqResult.

    :param custom_taskiq_result: TaskiqResult with custom result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    await result_backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )
    result = await result_backend.get_result(task_id=task_id)

    assert (
        result.return_value.test_arg  # type: ignore
        == custom_taskiq_result.return_value.test_arg  # type: ignore
    )
    assert result.is_err == custom_taskiq_result.is_err
    assert result.execution_time == custom_taskiq_result.execution_time
    assert result.log == custom_taskiq_result.log


async def test_success_backend_is_result_ready(
    result_backend: PostgresqlResultBackend[_ReturnType],
    custom_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    """Tests `is_result_ready` method."""
    assert not await result_backend.is_result_ready(task_id=task_id)
    await result_backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )

    assert await result_backend.is_result_ready(task_id=task_id)


async def test_when_the_backend_shuts_down__then_its_pool_is_closed(
    result_backend: PostgresqlResultBackend[_ReturnType],
) -> None:
    """startup() opened the pool to read the catalog; shutdown() closes it."""
    assert getattr(result_backend.driver, "pool", None) is not None

    await result_backend.shutdown()
    # A second shutdown, such as the broker's after the application's own,
    # finds nothing to close.
    await result_backend.shutdown()

    assert getattr(result_backend.driver, "pool", None) is None


async def _seed_results_around_from_date(
    backend: PostgresqlResultBackend[_ReturnType],
    dsn: str,
    table: str,
    result: TaskiqResult[_ReturnType],
) -> dict[str, str]:
    """
    Store one result per age and pin its ``created_at`` around ``_FROM_DATE``.

    ``created_at`` is set with a direct UPDATE, not by sleeping, so the rows sit
    exactly one day before, at, one day after and two days after the bound.

    Returns:
        The task id stored for each age.
    """
    offsets = {
        "before": timedelta(days=-1),
        "at": timedelta(0),
        "after": timedelta(days=1),
        "later": timedelta(days=2),
    }
    ids = {name: str(uuid.uuid4()) for name in offsets}
    for name, task_id in ids.items():
        await backend.set_result(task_id=task_id, result=result)
        await fetch_on_server(
            dsn,
            f"UPDATE {table} SET created_at = $1 WHERE task_id = $2",  # noqa: S608
            _FROM_DATE + offsets[name],
            task_id,
        )
    return ids


async def _stored_task_ids(dsn: str, table: str) -> set[str]:
    rows = await fetch_on_server(dsn, f"SELECT task_id FROM {table}")  # noqa: S608
    return {str(row["task_id"]) for row in rows}


@pytest.mark.parametrize(
    ("to_date", "kept"),
    [
        pytest.param(None, {"before"}, id="no_to_date"),
        pytest.param(
            _FROM_DATE + timedelta(days=1),
            {"before", "later"},
            id="one_day_after",
        ),
    ],
)
async def test_when_delete_by_date_runs__then_the_rows_in_range_go(
    result_backend: PostgresqlResultBackend[_ReturnType],
    postgresql_dsn: str,
    postgres_table: str,
    default_taskiq_result: TaskiqResult[_ReturnType],
    to_date: Optional[datetime],
    kept: set[str],
) -> None:
    ids = await _seed_results_around_from_date(
        result_backend,
        postgresql_dsn,
        postgres_table,
        default_taskiq_result,
    )

    await result_backend.delete_by_date(_FROM_DATE, to_date)

    remaining = await _stored_task_ids(postgresql_dsn, postgres_table)
    assert remaining == {ids[name] for name in kept}
