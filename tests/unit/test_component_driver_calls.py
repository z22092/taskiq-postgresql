"""
Pin the calls each component makes on its driver, without a database.

Each component gets a ``RecordingDriver`` in place of the one it built, and the
tests assert, exactly, the driver method, arguments and order of every
operation. A change in how the components reach the database must keep these
calls identical, so the assertions are never loosened to follow it.
"""

import asyncio
import json
import logging
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime, timezone
from functools import partial
from types import TracebackType
from typing import Any, Optional
from unittest import mock

import pytest
from taskiq import BrokerMessage, ScheduledTask, TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress, TaskState
from taskiq.serializers.pickle import PickleSerializer
from typing_extensions import Self, override

from taskiq_postgresql.abc.driver import ListenDriver, QueryDriver
from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.core.sql.catalog import Column, notify_statement
from taskiq_postgresql.drivers._asyncpg import AsyncpgDriver
from taskiq_postgresql.exceptions import ResultIsMissingError
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

DSN = "postgresql://u@localhost/db"

# The return value of ``a_result``, which the result tests read back.
RETURN_VALUE = 5

Call = tuple[str, tuple[Any, ...], dict[str, Any]]

# Tells a ``returning`` the component left out from one it passed as None.
_OMITTED: Any = object()


class RecordingDriver(QueryDriver):
    """A ``QueryDriver`` that records each call and answers with fixed values."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.calls: list[Call] = []
        self.on_startup_seen: list[dict[str, Any]] = []
        self.insert_returns: Any = 7
        self.fetch_row_returns: Optional[dict[str, Any]] = None
        self.fetch_row_raises: Optional[BaseException] = None
        self.select_returns: list[dict[str, Any]] = []
        self.exists_returns = True
        self.execute_raises: Optional[BaseException] = None
        self.on_startup_raises: Optional[BaseException] = None

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))

    @override
    async def __aenter__(self) -> Self:
        self._record("__aenter__")
        return self

    @override
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._record("__aexit__", exc_type, exc_value, traceback)

    @override
    async def insert(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        returning: Any = _OMITTED,
    ) -> Any:
        passed = () if returning is _OMITTED else (returning,)
        self._record("insert", columns, values, *passed)
        return self.insert_returns

    @override
    async def insert_or_update(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        on_conflict_columns: Sequence[Column],
        on_conflict_update_columns: Sequence[Column],
        returning: Any = _OMITTED,
    ) -> Any:
        passed = () if returning is _OMITTED else (returning,)
        self._record(
            "insert_or_update",
            columns,
            values,
            on_conflict_columns,
            on_conflict_update_columns,
            *passed,
        )

    @override
    async def delete(self, column: Column, value: Any) -> Any:
        self._record("delete", column, value)

    @override
    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        self._record("fetch_row", query, *values)
        if self.fetch_row_raises is not None:
            raise self.fetch_row_raises
        return self.fetch_row_returns

    @override
    async def delete_by_date(
        self,
        from_date: Any,
        to_date: Any = None,
    ) -> Any:
        self._record("delete_by_date", from_date, to_date)

    @override
    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
        not_null: Sequence[Column] = (),
    ) -> list[dict[str, Any]]:
        # The keyword is recorded only when it is given, so a read that has no
        # null check is pinned as it always was.
        given = {"not_null": not_null} if not_null else {}
        if where_columns is None and where_values is None:
            self._record("select", columns, **given)
        else:
            self._record("select", columns, where_columns, where_values, **given)
        return self.select_returns

    @override
    async def exists(self, id: Any, not_null: Sequence[Column] = ()) -> bool:
        self._record("exists", id, **({"not_null": not_null} if not_null else {}))
        return self.exists_returns

    @override
    async def on_startup(self) -> None:
        # What the component must have set by now, read when it is called.
        self._record("on_startup")
        self.on_startup_seen.append(
            {
                "connection_string": self.connection_string,
                "component_kind": self.component_kind,
                "query_timeout": self.query_timeout,
            },
        )
        if self.on_startup_raises is not None:
            raise self.on_startup_raises

    @override
    async def on_shutdown(self) -> None:
        self._record("on_shutdown")

    @override
    async def execute(self, query: str, *values: Any) -> Any:
        self._record("execute", query, *values)
        if self.execute_raises is not None:
            raise self.execute_raises


class FakeListenDriver(ListenDriver):
    """A ``ListenDriver`` that records its start and stop in a shared log."""

    def __init__(self, events: list[str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events = events

    @override
    async def on_startup(self) -> None:
        self.events.append("listen.on_startup")

    @override
    async def on_shutdown(self) -> None:
        self.events.append("listen.on_shutdown")

    @override
    def __aiter__(self) -> AsyncIterator[str]:
        raise NotImplementedError


def recording_like(original: QueryDriver) -> RecordingDriver:
    """Build a ``RecordingDriver`` declared exactly as ``original`` is."""
    driver = RecordingDriver(
        connection_string=DSN,
        table_name=original.table,
        columns=original.columns,
        primary_key=original.primary_key,
        created_at=original.created_at,
        index_columns=original.index_columns,
        added_columns=original.added_columns,
        added_indexes=original.added_indexes,
    )
    # A driver of a release before steps has none, and declares what it adds.
    driver.steps = getattr(original, "steps", ())
    return driver


def make_broker() -> tuple[PostgresqlBroker, RecordingDriver, list[str]]:
    """Build a broker over a ``RecordingDriver`` and a fake LISTEN driver."""
    with warnings.catch_warnings():
        # A mixed-case channel warns; the fold is what the NOTIFY test pins.
        warnings.simplefilter("ignore")
        broker = PostgresqlBroker(DSN, channel_name="MixedChannel")
    driver = recording_like(broker.driver)
    events: list[str] = []
    broker.driver = driver
    broker.listen_driver = FakeListenDriver(
        events,
        connection_string=DSN,
        channel_name="MixedChannel",
    )
    return broker, driver, events


def make_result_backend(
    keep_results: bool = True,
    **options: Any,
) -> tuple[PostgresqlResultBackend[Any], RecordingDriver]:
    """Build a result backend over a ``RecordingDriver``."""
    backend: PostgresqlResultBackend[Any] = PostgresqlResultBackend(
        DSN,
        keep_results=keep_results,
        **options,
    )
    driver = recording_like(backend.driver)
    backend.driver = driver
    return backend, driver


def make_scheduler_source() -> tuple[PostgresqlSchedulerSource, RecordingDriver]:
    """Build a scheduler source over a ``RecordingDriver``."""
    source = PostgresqlSchedulerSource(DSN)
    driver = recording_like(source.driver)
    source.driver = driver
    return source, driver


def broker_message(labels: Optional[dict[str, str]] = None) -> BrokerMessage:
    """Return a message to kick."""
    return BrokerMessage(
        task_id="task-1",
        task_name="tests:work",
        message=b"payload",
        labels=labels or {},
    )


def a_result() -> TaskiqResult[int]:
    """Return a result of a task."""
    return TaskiqResult(
        is_err=False,
        log="a log",
        return_value=RETURN_VALUE,
        execution_time=0.5,
    )


def a_schedule(**fields: Any) -> ScheduledTask:
    """Return a schedule."""
    return ScheduledTask(
        task_name="tests:work",
        labels={},
        schedule_id="schedule-1",
        args=[],
        kwargs={},
        **fields,
    )


Operation = Callable[[], Awaitable[Any]]


def a_broker_operation() -> tuple[Any, Operation, str]:
    """Return a fresh broker, one operation on its driver and that call's name."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        broker = PostgresqlBroker(DSN, channel_name="MixedChannel")
    return broker, partial(broker.kick, broker_message({"delay": "2.5"})), "insert"


def a_result_backend_operation() -> tuple[Any, Operation, str]:
    """Return a fresh result backend, one operation and that call's name."""
    backend: PostgresqlResultBackend[Any] = PostgresqlResultBackend(DSN)
    return backend, partial(backend.is_result_ready, "task-1"), "exists"


def a_scheduler_source_operation() -> tuple[Any, Operation, str]:
    """Return a fresh scheduler source, one operation and that call's name."""
    source = PostgresqlSchedulerSource(DSN)
    return source, partial(source.delete_schedule, "schedule-1"), "delete"


OPERATIONS = pytest.mark.parametrize(
    "operation_of",
    [a_broker_operation, a_result_backend_operation, a_scheduler_source_operation],
    ids=["broker", "result-backend", "scheduler-source"],
)


@pytest.mark.anyio
@OPERATIONS
async def test_when_the_driver_is_patched__then_the_operation_goes_to_the_patch(
    operation_of: Callable[[], tuple[Any, Operation, str]],
) -> None:
    component, operation, method = operation_of()
    original = component.driver
    recording = recording_like(original)

    with mock.patch.object(component, "driver", recording):
        await operation()

    assert [call[0] for call in recording.calls] == [method]
    assert component.driver is original


@pytest.mark.anyio
@OPERATIONS
async def test_when_the_driver_is_deleted_and_assigned__then_the_operation_goes_to_it(
    operation_of: Callable[[], tuple[Any, Operation, str]],
) -> None:
    component, operation, method = operation_of()
    recording = recording_like(component.driver)

    del component.driver
    component.driver = recording
    await operation()

    assert [call[0] for call in recording.calls] == [method]
    assert component.driver is recording


@pytest.mark.anyio
@pytest.mark.parametrize(
    "make",
    [make_broker, make_result_backend, make_scheduler_source],
    ids=["broker", "result-backend", "scheduler-source"],
)
async def test_when_on_startup_raises__then_startup_raises_it_and_nothing_closes(
    make: Callable[[], tuple[Any, ...]],
) -> None:
    component, driver, *_ = make()
    error = RuntimeError("no startup")
    driver.on_startup_raises = error

    with pytest.raises(RuntimeError) as raised:
        await component.startup()

    assert raised.value is error
    assert [call[0] for call in driver.calls] == ["on_startup"]


"""The calls of the broker."""


@pytest.mark.anyio
async def test_broker_when_kicked_without_delay__then_it_inserts_and_notifies() -> None:
    broker, driver, _ = make_broker()
    message = broker_message({"a": "b"})
    columns = broker.columns

    await broker.kick(message)

    assert driver.calls == [
        (
            "insert",
            (
                [
                    columns.task_id,
                    columns.task_name,
                    columns.message,
                    columns.labels,
                    columns.channel,
                    columns.not_before,
                ],
                [
                    "task-1",
                    "tests:work",
                    b"payload",
                    {"a": "b"},
                    "MixedChannel",
                    0.0,
                ],
                [columns.primary_key],
            ),
            {},
        ),
        (
            "execute",
            (notify_statement().text, "mixedchannel", "7"),
            {},
        ),
    ]


@pytest.mark.anyio
async def test_broker_when_kicked_with_a_delay__then_it_only_inserts() -> None:
    broker, driver, _ = make_broker()
    columns = broker.columns

    await broker.kick(broker_message({"delay": "2.5"}))

    assert [call[0] for call in driver.calls] == ["insert"]
    _, (insert_columns, values, returning), kwargs = driver.calls[0]
    assert insert_columns[-1] is columns.not_before
    assert values == [
        "task-1",
        "tests:work",
        b"payload",
        {"delay": "2.5"},
        "MixedChannel",
        2.5,
    ]
    assert returning == [columns.primary_key]
    assert kwargs == {}


@pytest.mark.anyio
async def test_broker_when_the_notify_fails__then_it_warns_and_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker, driver, _ = make_broker()
    driver.execute_raises = RuntimeError("no notify")

    with caplog.at_level(logging.WARNING, logger="taskiq.asyncpg_broker"):
        await broker.kick(broker_message())

    assert [call[0] for call in driver.calls] == ["insert", "execute"]
    assert [record.getMessage() for record in caplog.records] == [
        (
            "NOTIFY of message 7 on channel MixedChannel failed "
            "(RuntimeError('no notify')): it will be delivered by the next sweep"
        ),
    ]
    assert [record.levelno for record in caplog.records] == [logging.WARNING]


@pytest.mark.anyio
async def test_broker_when_the_insert_fails__then_the_error_propagates() -> None:
    broker, driver, _ = make_broker()

    async def failing_insert(*args: Any, **kwargs: Any) -> Any:
        driver.calls.append(("insert", args, kwargs))
        raise RuntimeError("insert failed")

    driver.insert = failing_insert  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="insert failed"):
        await broker.kick(broker_message())

    assert [call[0] for call in driver.calls] == ["insert"]


@pytest.mark.anyio
async def test_broker_when_a_due_message_is_claimed__then_it_returns_its_bytes() -> (
    None
):
    broker, driver, _ = make_broker()
    driver.fetch_row_returns = {"message": b"claimed"}

    claimed = await broker._claim_due()

    assert claimed is not None
    assert claimed.data == b"claimed"
    assert driver.calls == [
        ("fetch_row", (broker._claim_due_query, "MixedChannel"), {}),
    ]


@pytest.mark.anyio
async def test_broker_when_a_message_is_claimed_by_id__then_it_returns_its_bytes() -> (
    None
):
    broker, driver, _ = make_broker()
    driver.fetch_row_returns = {"message": b"claimed"}

    claimed = await broker._claim_notified(42)

    assert claimed is not None
    assert claimed.data == b"claimed"
    assert driver.calls == [
        ("fetch_row", (broker._claim_by_id_query, "MixedChannel", 42), {}),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("by_id", [False, True], ids=["due", "by-id"])
async def test_broker_when_no_row_matches__then_the_claim_is_none(
    by_id: bool,
) -> None:
    broker, driver, _ = make_broker()
    driver.fetch_row_returns = None

    claimed = await (broker._claim_notified(3) if by_id else broker._claim_due())

    assert claimed is None
    assert [call[0] for call in driver.calls] == ["fetch_row"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("by_id", "what"),
    [(False, "a due message"), (True, "message 3")],
    ids=["due", "by-id"],
)
async def test_broker_when_the_claim_raises__then_it_logs_and_returns_none(
    by_id: bool,
    what: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker, driver, _ = make_broker()
    driver.fetch_row_raises = RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger="taskiq.asyncpg_broker"):
        claimed = await (broker._claim_notified(3) if by_id else broker._claim_due())

    assert claimed is None
    assert [record.getMessage() for record in caplog.records] == [
        f"Error claiming {what} on channel MixedChannel",
    ]
    assert caplog.records[0].exc_info is not None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("by_id", "what"),
    [(False, "a due message"), (True, "message 3")],
    ids=["due", "by-id"],
)
async def test_broker_when_the_claim_times_out__then_it_logs_an_error_and_returns_none(
    by_id: bool,
    what: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker, driver, _ = make_broker()
    driver.fetch_row_raises = asyncio.TimeoutError()

    with caplog.at_level(logging.ERROR, logger="taskiq.asyncpg_broker"):
        claimed = await (broker._claim_notified(3) if by_id else broker._claim_due())

    assert claimed is None
    assert [record.getMessage() for record in caplog.records] == [
        (
            f"Claiming {what} on channel MixedChannel got no answer within "
            f"{broker.query_timeout} s"
        ),
    ]
    assert caplog.records[0].exc_info is None


@pytest.mark.anyio
async def test_broker_when_it_starts__then_the_driver_is_set_up_first() -> None:
    broker, driver, events = make_broker()
    # The listen driver shares the log, so the order across drivers shows.
    original_on_startup = driver.on_startup

    async def logged_on_startup() -> None:
        events.append("driver.on_startup")
        await original_on_startup()

    driver.on_startup = logged_on_startup  # type: ignore[method-assign]

    await broker.startup()

    assert driver.on_startup_seen == [
        {
            "connection_string": DSN,
            "component_kind": "broker",
            "query_timeout": broker.query_timeout,
        },
    ]
    assert [call[0] for call in driver.calls] == ["on_startup"]
    assert events == ["driver.on_startup", "listen.on_startup"]


@pytest.mark.anyio
async def test_broker_when_it_shuts_down__then_both_drivers_are_closed() -> None:
    broker, driver, events = make_broker()

    await broker.shutdown()

    assert driver.calls == [("on_shutdown", (), {})]
    assert events == ["listen.on_shutdown"]


"""The calls of the result backend."""

# What taskiq's ProgressTracker sends, as JSON.
STARTED_JSON = b'{"state":"STARTED","meta":{"a":1}}'


def a_progress(meta: Any = None) -> TaskProgress[Any]:
    """Return a progress report of a started task."""
    return TaskProgress(state=TaskState.STARTED, meta=meta)


class FailingSerializer(PickleSerializer):
    """A serializer that raises when it is used: progress never uses it."""

    @override
    def dumpb(self, value: Any) -> bytes:
        raise AssertionError("progress must not go through the serializer")

    @override
    def loadb(self, value: bytes) -> Any:
        raise AssertionError("progress must not go through the serializer")


@pytest.mark.anyio
async def test_results_when_a_result_is_set__then_it_is_upserted_by_task_id() -> None:
    backend, driver = make_result_backend()
    result = a_result()
    columns = backend.columns

    await backend.set_result("task-1", result)

    assert driver.calls == [
        (
            "insert_or_update",
            (
                [columns.primary_key, columns.result],
                ["task-1", backend.serializer.dumpb(result)],
                [columns.primary_key],
                [columns.result],
            ),
            {},
        ),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("ready", [True, False])
async def test_results_when_asked_if_a_result_is_ready__then_it_asks_exists(
    ready: bool,
) -> None:
    backend, driver = make_result_backend()
    driver.exists_returns = ready

    assert await backend.is_result_ready("task-1") is ready

    # A row that holds only progress is not a ready result.
    assert driver.calls == [
        ("exists", ("task-1",), {"not_null": [backend.columns.result]}),
    ]


@pytest.mark.anyio
async def test_results_when_results_are_kept__then_get_result_only_selects() -> None:
    backend, driver = make_result_backend(keep_results=True)
    driver.select_returns = [{"result": backend.serializer.dumpb(a_result())}]
    columns = backend.columns

    result = await backend.get_result("task-1")

    assert result.return_value == RETURN_VALUE
    assert result.log is None
    assert driver.calls == [
        (
            "select",
            ([columns.result], [columns.primary_key], ["task-1"]),
            {"not_null": [columns.result]},
        ),
    ]


@pytest.mark.anyio
async def test_results_when_not_kept__then_get_result_selects_then_deletes() -> None:
    backend, driver = make_result_backend(keep_results=False)
    driver.select_returns = [{"result": backend.serializer.dumpb(a_result())}]
    columns = backend.columns

    result = await backend.get_result("task-1", with_logs=True)

    assert result.return_value == RETURN_VALUE
    assert result.log == "a log"
    assert driver.calls == [
        (
            "select",
            ([columns.result], [columns.primary_key], ["task-1"]),
            {"not_null": [columns.result]},
        ),
        ("delete", (columns.primary_key, "task-1"), {}),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("keep_results", [True, False])
async def test_results_when_there_is_no_row__then_get_result_raises_missing(
    keep_results: bool,
) -> None:
    backend, driver = make_result_backend(keep_results=keep_results)
    driver.select_returns = []

    with pytest.raises(ResultIsMissingError) as raised:
        await backend.get_result("task-1")

    assert str(raised.value) == (
        "Cannot find record with task_id = task-1 in PostgreSQL"
    )
    assert [call[0] for call in driver.calls] == ["select"]


class OldSignatureDriver(RecordingDriver):
    """A third-party driver written before ``not_null`` existed."""

    @override
    async def select(  # ty: ignore[invalid-method-override]
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        return await super().select(columns, where_columns, where_values)

    @override
    async def exists(self, id: Any) -> bool:  # ty: ignore[invalid-method-override]
        return await super().exists(id)


@pytest.mark.anyio
async def test_results_when_a_driver_does_not_take_not_null__then_reads_fail() -> None:
    # The breaking change of this release for a query driver written by hand:
    # select and exists must accept the keyword, or a result cannot be read.
    backend, _ = make_result_backend()
    old = OldSignatureDriver(
        connection_string=DSN,
        table_name=backend.driver.table,
        columns=backend.driver.columns,
        primary_key=backend.driver.primary_key,
    )
    backend.driver = old

    with pytest.raises(TypeError, match="not_null"):
        await backend.is_result_ready("task-1")
    with pytest.raises(TypeError, match="not_null"):
        await backend.get_result("task-1")


@pytest.mark.anyio
async def test_results_when_a_row_has_only_progress__then_a_read_deletes_nothing() -> (
    None
):
    # The select leaves a row out when its result is NULL, so none comes back;
    # the delete of keep_results=False must not run for it.
    backend, driver = make_result_backend(keep_results=False)
    driver.select_returns = []

    with pytest.raises(ResultIsMissingError):
        await backend.get_result("task-1")

    assert [call[0] for call in driver.calls] == ["select"]


@pytest.mark.anyio
async def test_results_when_deleting_from_a_date__then_to_date_is_passed_as_none() -> (
    None
):
    backend, driver = make_result_backend()
    from_date = datetime(2026, 1, 1, tzinfo=timezone.utc)

    await backend.delete_by_date(from_date)

    assert driver.calls == [("delete_by_date", (from_date, None), {})]


@pytest.mark.anyio
async def test_results_when_deleting_between_dates__then_both_are_passed() -> None:
    backend, driver = make_result_backend()
    from_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2026, 2, 1, tzinfo=timezone.utc)

    await backend.delete_by_date(from_date, to_date)

    assert driver.calls == [("delete_by_date", (from_date, to_date), {})]


@pytest.mark.anyio
async def test_progress_when_it_is_set__then_only_its_column_is_upserted() -> None:
    backend, driver = make_result_backend()
    columns = backend.columns

    await backend.set_progress("task-1", a_progress({"a": 1}))

    assert driver.calls == [
        (
            "insert_or_update",
            (
                [columns.primary_key, columns.progress],
                ["task-1", STARTED_JSON],
                [columns.primary_key],
                [columns.progress],
            ),
            {},
        ),
    ]


@pytest.mark.anyio
async def test_progress_when_it_is_set__then_the_serializer_is_not_used() -> None:
    backend, driver = make_result_backend(serializer=FailingSerializer())
    driver.select_returns = [{"progress": STARTED_JSON}]

    await backend.set_progress("task-1", a_progress({"a": 1}))
    found = await backend.get_progress("task-1")

    assert driver.calls[0][1][1] == ["task-1", STARTED_JSON]
    assert found is not None
    assert found.meta == {"a": 1}


@pytest.mark.anyio
async def test_progress_when_it_is_not_stored__then_set_progress_calls_nothing() -> (
    None
):
    backend, driver = make_result_backend(store_progress=False)

    await backend.set_progress("task-1", a_progress())

    assert backend.store_progress is False
    assert driver.calls == []


@pytest.mark.anyio
async def test_progress_when_it_is_not_stored__then_get_progress_still_reads() -> None:
    backend, driver = make_result_backend(store_progress=False)
    driver.select_returns = [{"progress": STARTED_JSON}]
    columns = backend.columns

    found = await backend.get_progress("task-1")

    assert found == a_progress({"a": 1})
    assert driver.calls == [
        ("select", ([columns.progress], [columns.primary_key], ["task-1"]), {}),
    ]


def test_progress_is_stored_unless_the_backend_is_told_not_to() -> None:
    backend, _ = make_result_backend()

    assert backend.store_progress is True


@pytest.mark.anyio
async def test_progress_when_it_is_read__then_it_is_the_one_that_was_stored() -> None:
    backend, driver = make_result_backend()
    driver.select_returns = [{"progress": STARTED_JSON}]
    columns = backend.columns

    found = await backend.get_progress("task-1")

    assert found is not None
    assert found.state == TaskState.STARTED
    assert found.meta == {"a": 1}
    assert driver.calls == [
        ("select", ([columns.progress], [columns.primary_key], ["task-1"]), {}),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("returned", [[], [{"progress": None}]])
async def test_progress_when_there_is_none__then_get_progress_returns_none(
    returned: list[dict[str, Any]],
) -> None:
    backend, driver = make_result_backend()
    driver.select_returns = returned

    assert await backend.get_progress("task-1") is None


@pytest.mark.anyio
async def test_progress_when_meta_is_not_json__then_it_raises_and_writes_nothing() -> (
    None
):
    backend, driver = make_result_backend()

    with pytest.raises(ValueError, match="Unable to serialize"):
        await backend.set_progress("task-1", a_progress({"x": object()}))

    assert driver.calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("stored", [b"not json", PickleSerializer().dumpb({"a": 1})])
async def test_progress_when_the_column_is_not_json__then_it_raises(
    stored: bytes,
) -> None:
    # No pickle fallback: what a database holds is never unpickled.
    backend, driver = make_result_backend()
    driver.select_returns = [{"progress": stored}]

    with pytest.raises(ValueError, match=r"Invalid JSON|valid"):
        await backend.get_progress("task-1")


@pytest.mark.anyio
async def test_progress_when_a_driver_gives_a_buffer__then_it_is_read_as_bytes() -> (
    None
):
    backend, driver = make_result_backend()
    driver.select_returns = [{"progress": memoryview(STARTED_JSON)}]

    found = await backend.get_progress("task-1")

    assert found == a_progress({"a": 1})


@pytest.mark.anyio
async def test_progress_state_is_a_plain_string_that_equals_the_enum() -> None:
    backend, driver = make_result_backend()
    driver.select_returns = [{"progress": STARTED_JSON}]

    found = await backend.get_progress("task-1")

    assert found is not None
    assert type(found.state) is str
    assert found.state == TaskState.STARTED


@pytest.mark.anyio
async def test_results_when_it_starts__then_the_driver_is_set_up_first() -> None:
    backend, driver = make_result_backend()

    await backend.startup()

    assert driver.on_startup_seen == [
        {
            "connection_string": DSN,
            "component_kind": "result_backend",
            "query_timeout": None,
        },
    ]
    assert driver.calls == [("on_startup", (), {})]


@pytest.mark.anyio
async def test_results_when_it_shuts_down__then_the_driver_is_closed() -> None:
    backend, driver = make_result_backend()

    await backend.shutdown()

    assert driver.calls == [("on_shutdown", (), {})]


@pytest.mark.anyio
async def test_results_when_a_driver_overrides_on_startup__then_it_runs_once() -> None:
    seen: list[str] = []

    class OverridingDriver(AsyncpgDriver):
        @override
        async def on_startup(self) -> None:
            seen.append(self.component_kind or "")

    backend: PostgresqlResultBackend[Any] = PostgresqlResultBackend(DSN)
    original = backend.driver
    backend.driver = OverridingDriver(
        connection_string=DSN,
        table_name=original.table,
        columns=original.columns,
        primary_key=original.primary_key,
        created_at=original.created_at,
        index_columns=original.index_columns,
    )

    await backend.startup()

    assert seen == ["result_backend"]


"""The calls of the scheduler source."""


@pytest.mark.anyio
async def test_schedules_when_schedules_are_read__then_it_selects_id_and_schedule() -> (
    None
):
    source, driver = make_scheduler_source()
    stored = a_schedule(cron="* * * * *")
    driver.select_returns = [
        {"id": "schedule-1", "schedule": stored.model_dump_json()},
    ]
    columns = source.columns

    schedules = await source.get_schedules()

    assert [schedule.schedule_id for schedule in schedules] == ["schedule-1"]
    assert driver.calls == [
        ("select", ([columns.primary_key, columns.schedule],), {}),
    ]


@pytest.mark.anyio
async def test_schedules_when_one_is_added__then_it_inserts_without_returning() -> None:
    source, driver = make_scheduler_source()
    schedule = a_schedule(cron="* * * * *")
    columns = source.columns

    await source.add_schedule(schedule)

    assert driver.calls == [
        (
            "insert",
            (
                [columns.primary_key, columns.task_name, columns.schedule],
                [
                    "schedule-1",
                    "tests:work",
                    schedule.model_dump_json(),
                ],
            ),
            {},
        ),
    ]
    assert json.loads(driver.calls[0][1][1][2])["schedule_id"] == "schedule-1"


@pytest.mark.anyio
async def test_schedules_when_a_schedule_is_deleted__then_it_deletes_by_id() -> None:
    source, driver = make_scheduler_source()

    await source.delete_schedule("schedule-1")

    assert driver.calls == [
        ("delete", (source.columns.primary_key, "schedule-1"), {}),
    ]


@pytest.mark.anyio
async def test_schedules_when_a_timed_one_was_sent__then_post_send_deletes_it() -> None:
    source, driver = make_scheduler_source()
    schedule = a_schedule(time=datetime(2030, 1, 1, tzinfo=timezone.utc))

    await source.post_send(schedule)

    assert driver.calls == [
        ("delete", (source.columns.primary_key, "schedule-1"), {}),
    ]


@pytest.mark.anyio
async def test_schedules_when_a_cron_one_was_sent__then_post_send_calls_nothing() -> (
    None
):
    source, driver = make_scheduler_source()

    await source.post_send(a_schedule(cron="* * * * *"))

    assert driver.calls == []


@pytest.mark.anyio
async def test_schedules_when_it_starts__then_the_driver_is_set_up_first() -> None:
    source, driver = make_scheduler_source()

    await source.startup()

    assert driver.on_startup_seen == [
        {
            "connection_string": DSN,
            "component_kind": "scheduler_source",
            "query_timeout": None,
        },
    ]
    assert driver.calls == [("on_startup", (), {})]


@pytest.mark.anyio
async def test_schedules_when_it_shuts_down__then_the_driver_is_closed() -> None:
    source, driver = make_scheduler_source()

    await source.shutdown()

    assert driver.calls == [("on_shutdown", (), {})]
