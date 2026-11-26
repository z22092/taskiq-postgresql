"""
Task progress in the result row, against a real PostgreSQL, on each driver.

A row holds a result, a progress, or both, each in its own column. Only a row
with a result is a ready result; progress is JSON whatever serializer the
backend is given.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest
from taskiq import TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress, TaskState
from taskiq.serializers.pickle import PickleSerializer

from taskiq_postgresql.exceptions import ResultIsMissingError
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from tests.helpers import drop_and_shutdown, fetch_on_server

pytestmark = pytest.mark.anyio

Backend = PostgresqlResultBackend[Any]
MakeBackend = Callable[..., Awaitable[Backend]]


@pytest.fixture
def task_id() -> str:
    """The id of a task: a UUID as text, which the default column takes."""
    return str(uuid.uuid4())


@pytest.fixture
def result() -> TaskiqResult[Any]:
    """The result of a task."""
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value="done",
        execution_time=0.1,
    )


@pytest.fixture
async def make_backend(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> AsyncGenerator[MakeBackend, None]:
    """
    Build and start result backends on the test's table, with options.

    The table is dropped, and every backend shut down, after the test.
    """
    started: list[Backend] = []

    async def make(**options: Any) -> Backend:
        backend: Backend = PostgresqlResultBackend(
            dsn=postgresql_dsn,
            table_name=postgres_table,
            driver=driver,
            run_migrations=True,
            **options,
        )
        await backend.startup()
        started.append(backend)
        return backend

    yield make
    if started:
        first, *others = started
        for backend in others:
            await backend.shutdown()
        await drop_and_shutdown(first, postgres_table)


def started(meta: Any = None) -> TaskProgress[Any]:
    """A report of a task that has started."""
    return TaskProgress(state=TaskState.STARTED, meta=meta)


async def columns_of(dsn: str, table: str, task_id: str) -> tuple[bool, bool]:
    """Tell whether the row of ``task_id`` holds a result, and whether progress."""
    rows = await fetch_on_server(
        dsn,
        f"SELECT result IS NOT NULL AS has_result, "  # noqa: S608
        f"progress IS NOT NULL AS has_progress FROM {table}"
        " WHERE task_id = $1",
        uuid.UUID(task_id),
    )
    ((has_result, has_progress),) = [tuple(row) for row in rows]
    return has_result, has_progress


async def count_rows(dsn: str, table: str) -> int:
    """Count the rows of ``table``."""
    (row,) = await fetch_on_server(dsn, f"SELECT count(*) FROM {table}")  # noqa: S608
    return row[0]


async def test_there_is_no_progress_before_the_first_report(
    make_backend: MakeBackend,
    task_id: str,
) -> None:
    backend = await make_backend()

    assert await backend.get_progress(task_id) is None


async def test_progress_is_read_back_and_the_last_report_wins(
    make_backend: MakeBackend,
    task_id: str,
) -> None:
    backend = await make_backend()

    await backend.set_progress(task_id, started({"done": 1}))
    first = await backend.get_progress(task_id)
    await backend.set_progress(
        task_id,
        TaskProgress(state="ALMOST", meta={"done": 2, "of": [1, 2, 3]}),
    )
    second = await backend.get_progress(task_id)

    assert first is not None
    assert (first.state, first.meta) == (TaskState.STARTED, {"done": 1})
    assert second is not None
    assert (second.state, second.meta) == ("ALMOST", {"done": 2, "of": [1, 2, 3]})


async def test_progress_comes_back_from_the_driver_as_bytes_that_parse(
    make_backend: MakeBackend,
    task_id: str,
) -> None:
    # BYTEA is not returned as bytes by every driver; the store makes it so.
    backend = await make_backend()
    await backend.set_progress(task_id, started({"a": 1}))

    stored = await backend._store.get_progress(task_id)

    assert type(stored) is bytes
    assert stored == b'{"state":"STARTED","meta":{"a":1}}'


async def test_a_row_with_only_progress_is_not_a_ready_result(
    make_backend: MakeBackend,
    task_id: str,
) -> None:
    backend = await make_backend()

    await backend.set_progress(task_id, started())

    assert await backend.is_result_ready(task_id) is False
    with pytest.raises(ResultIsMissingError, match=task_id):
        await backend.get_result(task_id)
    progress = await backend.get_progress(task_id)
    assert progress is not None
    assert progress.state == TaskState.STARTED


async def test_reading_a_progress_only_row_with_keep_results_off_deletes_nothing(
    make_backend: MakeBackend,
    task_id: str,
) -> None:
    backend = await make_backend(keep_results=False)
    await backend.set_progress(task_id, started({"a": 1}))

    with pytest.raises(ResultIsMissingError):
        await backend.get_result(task_id)

    # The progress is still there, and a later result completes the row.
    progress = await backend.get_progress(task_id)
    assert progress is not None
    assert progress.meta == {"a": 1}


async def test_a_result_set_after_progress_keeps_the_progress(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    task_id: str,
    result: TaskiqResult[Any],
) -> None:
    backend = await make_backend()

    await backend.set_progress(task_id, started({"a": 1}))
    await backend.set_result(task_id, result)

    assert await columns_of(postgresql_dsn, postgres_table, task_id) == (True, True)
    assert await backend.is_result_ready(task_id) is True
    assert (await backend.get_result(task_id)).return_value == "done"
    progress = await backend.get_progress(task_id)
    assert progress is not None
    assert progress.meta == {"a": 1}


async def test_progress_set_after_a_result_keeps_the_result_ready(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    task_id: str,
    result: TaskiqResult[Any],
) -> None:
    backend = await make_backend()

    await backend.set_result(task_id, result)
    await backend.set_progress(task_id, started())

    assert await columns_of(postgresql_dsn, postgres_table, task_id) == (True, True)
    assert await backend.is_result_ready(task_id) is True
    assert (await backend.get_result(task_id)).return_value == "done"


async def test_a_result_without_progress_has_none(
    make_backend: MakeBackend,
    task_id: str,
    result: TaskiqResult[Any],
) -> None:
    backend = await make_backend()

    await backend.set_result(task_id, result)

    assert await backend.get_progress(task_id) is None


async def test_reading_a_result_with_keep_results_off_deletes_the_row_and_its_progress(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    task_id: str,
    result: TaskiqResult[Any],
) -> None:
    backend = await make_backend(keep_results=False)
    await backend.set_progress(task_id, started())
    await backend.set_result(task_id, result)

    assert (await backend.get_result(task_id)).return_value == "done"

    assert await count_rows(postgresql_dsn, postgres_table) == 0
    assert await backend.get_progress(task_id) is None


async def test_progress_is_json_on_disk_whatever_the_serializer(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    task_id: str,
) -> None:
    backend = await make_backend(serializer=PickleSerializer())

    await backend.set_progress(task_id, started({"a": 1}))

    query = (
        "SELECT convert_from(progress, 'UTF8')::jsonb ->> 'state' AS state,"  # noqa: S608
        " convert_from(progress, 'UTF8')::jsonb -> 'meta' ->> 'a' AS a"
        f" FROM {postgres_table}"
    )
    (row,) = await fetch_on_server(postgresql_dsn, query)
    assert (row["state"], row["a"]) == ("STARTED", "1")


async def test_meta_that_is_not_json_raises_and_writes_no_row(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    task_id: str,
) -> None:
    backend = await make_backend()

    with pytest.raises(ValueError, match="serialize"):
        await backend.set_progress(task_id, started({"x": object()}))

    assert await count_rows(postgresql_dsn, postgres_table) == 0


async def test_concurrent_reports_and_results_leave_both_columns(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    result: TaskiqResult[Any],
) -> None:
    backend = await make_backend()
    ids = [str(uuid.uuid4()) for _ in range(20)]

    await asyncio.gather(
        *(backend.set_progress(task_id, started({"id": task_id})) for task_id in ids),
        *(backend.set_result(task_id, result) for task_id in ids),
    )

    assert await count_rows(postgresql_dsn, postgres_table) == len(ids)
    for task_id in ids:
        assert await columns_of(postgresql_dsn, postgres_table, task_id) == (
            True,
            True,
        )
        progress = await backend.get_progress(task_id)
        assert progress is not None
        assert progress.meta == {"id": task_id}


async def test_progress_is_not_written_when_the_backend_does_not_store_it(
    make_backend: MakeBackend,
    postgresql_dsn: str,
    postgres_table: str,
    task_id: str,
) -> None:
    backend = await make_backend(store_progress=False)

    await backend.set_progress(task_id, started({"a": 1}))

    assert await count_rows(postgresql_dsn, postgres_table) == 0
    assert await backend.get_progress(task_id) is None


async def test_a_backend_that_does_not_store_progress_still_reads_it(
    make_backend: MakeBackend,
    task_id: str,
) -> None:
    writer = await make_backend()
    await writer.set_progress(task_id, started({"a": 1}))
    reader = await make_backend(store_progress=False)

    progress = await reader.get_progress(task_id)

    assert progress is not None
    assert progress.meta == {"a": 1}
