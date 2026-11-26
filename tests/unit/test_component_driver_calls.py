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
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Optional

import pytest
from taskiq import BrokerMessage, ScheduledTask, TaskiqResult
from typing_extensions import Self

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

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))

    async def __aenter__(self) -> Self:
        self._record("__aenter__")
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._record("__aexit__", exc_type, exc_value, traceback)

    async def insert(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        returning: Any = _OMITTED,
    ) -> Any:
        """Record the call and return the fixed id."""
        passed = () if returning is _OMITTED else (returning,)
        self._record("insert", columns, values, *passed)
        return self.insert_returns

    async def insert_or_update(
        self,
        columns: Sequence[Column],
        values: Sequence[Any],
        on_conflict_columns: Sequence[Column],
        on_conflict_update_columns: Sequence[Column],
        returning: Any = _OMITTED,
    ) -> Any:
        """Record the call."""
        passed = () if returning is _OMITTED else (returning,)
        self._record(
            "insert_or_update",
            columns,
            values,
            on_conflict_columns,
            on_conflict_update_columns,
            *passed,
        )

    async def delete(self, column: Column, value: Any) -> Any:
        """Record the call."""
        self._record("delete", column, value)

    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        """Record the call; return or raise what the test set."""
        self._record("fetch_row", query, *values)
        if self.fetch_row_raises is not None:
            raise self.fetch_row_raises
        return self.fetch_row_returns

    async def delete_by_date(
        self,
        from_date: Any,
        to_date: Any = None,
    ) -> Any:
        """Record the call."""
        self._record("delete_by_date", from_date, to_date)

    async def select(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
        where_values: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Record the call; return the rows the test set."""
        if where_columns is None and where_values is None:
            self._record("select", columns)
        else:
            self._record("select", columns, where_columns, where_values)
        return self.select_returns

    async def exists(self, id: Any) -> bool:
        """Record the call; return the fixed answer."""
        self._record("exists", id)
        return self.exists_returns

    async def on_startup(self) -> None:
        """Record the call and what the component had set by then."""
        # What the component must have set by now, read when it is called.
        self._record("on_startup")
        self.on_startup_seen.append(
            {
                "connection_string": self.connection_string,
                "component_kind": self.component_kind,
                "query_timeout": self.query_timeout,
            },
        )

    async def on_shutdown(self) -> None:
        """Record the call."""
        self._record("on_shutdown")

    async def execute(self, query: str, *values: Any) -> Any:
        """Record the call; raise what the test set."""
        self._record("execute", query, *values)
        if self.execute_raises is not None:
            raise self.execute_raises


class FakeListenDriver(ListenDriver):
    """A ``ListenDriver`` that records its start and stop in a shared log."""

    def __init__(self, events: list[str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events = events

    async def on_startup(self) -> None:
        """Log the start in the shared log."""
        self.events.append("listen.on_startup")

    async def on_shutdown(self) -> None:
        """Log the stop in the shared log."""
        self.events.append("listen.on_shutdown")

    def __aiter__(self) -> AsyncIterator[str]:
        raise NotImplementedError


def recording_like(original: QueryDriver) -> RecordingDriver:
    """Build a ``RecordingDriver`` declared exactly as ``original`` is."""
    return RecordingDriver(
        connection_string=DSN,
        table_name=original.table,
        columns=original.columns,
        primary_key=original.primary_key,
        created_at=original.created_at,
        index_columns=original.index_columns,
        added_columns=original.added_columns,
        added_indexes=original.added_indexes,
    )


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
) -> tuple[PostgresqlResultBackend[Any], RecordingDriver]:
    """Build a result backend over a ``RecordingDriver``."""
    backend: PostgresqlResultBackend[Any] = PostgresqlResultBackend(
        DSN,
        keep_results=keep_results,
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


class TestBroker:
    """The calls of the broker."""

    @pytest.mark.anyio
    async def test_when_kicked_without_delay__then_it_inserts_and_notifies(
        self,
    ) -> None:
        """When kicked without delay, then it inserts and notifies."""
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
    async def test_when_kicked_with_a_delay__then_it_inserts_and_does_not_notify(
        self,
    ) -> None:
        """When kicked with a delay, then it inserts and does not notify."""
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
    async def test_when_the_notify_fails__then_it_warns_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the notify fails, then it warns and does not raise."""
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
    async def test_when_the_insert_fails__then_the_error_propagates(self) -> None:
        """When the insert fails, then the error propagates."""
        broker, driver, _ = make_broker()

        async def failing_insert(*args: Any, **kwargs: Any) -> Any:
            """Record the call."""
            driver.calls.append(("insert", args, kwargs))
            raise RuntimeError("insert failed")

        driver.insert = failing_insert  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="insert failed"):
            await broker.kick(broker_message())

        assert [call[0] for call in driver.calls] == ["insert"]

    @pytest.mark.anyio
    async def test_when_a_due_message_is_claimed__then_it_returns_its_bytes(
        self,
    ) -> None:
        """When a due message is claimed, then it returns its bytes."""
        broker, driver, _ = make_broker()
        driver.fetch_row_returns = {"message": b"claimed"}

        claimed = await broker._claim_due()

        assert claimed is not None
        assert claimed.data == b"claimed"
        assert driver.calls == [
            ("fetch_row", (broker._claim_due_query, "MixedChannel"), {}),
        ]

    @pytest.mark.anyio
    async def test_when_a_message_is_claimed_by_id__then_it_returns_its_bytes(
        self,
    ) -> None:
        """When a message is claimed by id, then it returns its bytes."""
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
    async def test_when_no_row_matches__then_the_claim_is_none(
        self,
        by_id: bool,
    ) -> None:
        """When no row matches, then the claim is none."""
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
    async def test_when_the_claim_raises__then_it_logs_and_returns_none(
        self,
        by_id: bool,
        what: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the claim raises, then it logs and returns none."""
        broker, driver, _ = make_broker()
        driver.fetch_row_raises = RuntimeError("boom")

        with caplog.at_level(logging.ERROR, logger="taskiq.asyncpg_broker"):
            claimed = await (
                broker._claim_notified(3) if by_id else broker._claim_due()
            )

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
    async def test_when_the_claim_times_out__then_it_logs_an_error_and_returns_none(
        self,
        by_id: bool,
        what: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the claim times out, then it logs an error and returns none."""
        broker, driver, _ = make_broker()
        driver.fetch_row_raises = asyncio.TimeoutError()

        with caplog.at_level(logging.ERROR, logger="taskiq.asyncpg_broker"):
            claimed = await (
                broker._claim_notified(3) if by_id else broker._claim_due()
            )

        assert claimed is None
        assert [record.getMessage() for record in caplog.records] == [
            (
                f"Claiming {what} on channel MixedChannel got no answer within "
                f"{broker.query_timeout} s"
            ),
        ]
        assert caplog.records[0].exc_info is None

    @pytest.mark.anyio
    async def test_when_it_starts__then_the_driver_is_set_up_before_on_startup(
        self,
    ) -> None:
        """When it starts, then the driver is set up before on startup."""
        broker, driver, events = make_broker()
        # The listen driver shares the log, so the order across drivers shows.
        original_on_startup = driver.on_startup

        async def logged_on_startup() -> None:
            """Record the call."""
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
    async def test_when_it_shuts_down__then_both_drivers_are_closed(self) -> None:
        """When it shuts down, then both drivers are closed."""
        broker, driver, events = make_broker()

        await broker.shutdown()

        assert driver.calls == [("on_shutdown", (), {})]
        assert events == ["listen.on_shutdown"]


class TestResultBackend:
    """The calls of the result backend."""

    @pytest.mark.anyio
    async def test_when_a_result_is_set__then_it_is_upserted_by_task_id(
        self,
    ) -> None:
        """When a result is set, then it is upserted by task id."""
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
    async def test_when_asked_if_a_result_is_ready__then_it_asks_exists(
        self,
        ready: bool,
    ) -> None:
        """When asked if a result is ready, then it asks exists."""
        backend, driver = make_result_backend()
        driver.exists_returns = ready

        assert await backend.is_result_ready("task-1") is ready

        assert driver.calls == [("exists", ("task-1",), {})]

    @pytest.mark.anyio
    async def test_when_results_are_kept__then_get_result_only_selects(
        self,
    ) -> None:
        """When results are kept, then get result only selects."""
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
                {},
            ),
        ]

    @pytest.mark.anyio
    async def test_when_results_are_not_kept__then_get_result_selects_then_deletes(
        self,
    ) -> None:
        """When results are not kept, then get result selects then deletes."""
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
                {},
            ),
            ("delete", (columns.primary_key, "task-1"), {}),
        ]

    @pytest.mark.anyio
    @pytest.mark.parametrize("keep_results", [True, False])
    async def test_when_there_is_no_row__then_get_result_raises_missing(
        self,
        keep_results: bool,
    ) -> None:
        """When there is no row, then get result raises missing."""
        backend, driver = make_result_backend(keep_results=keep_results)
        driver.select_returns = []

        with pytest.raises(ResultIsMissingError) as raised:
            await backend.get_result("task-1")

        assert str(raised.value) == (
            "Cannot find record with task_id = task-1 in PostgreSQL"
        )
        assert [call[0] for call in driver.calls] == ["select"]

    @pytest.mark.anyio
    async def test_when_deleting_from_a_date__then_to_date_is_passed_as_none(
        self,
    ) -> None:
        """When deleting from a date, then to date is passed as none."""
        backend, driver = make_result_backend()
        from_date = datetime(2026, 1, 1, tzinfo=timezone.utc)

        await backend.delete_by_date(from_date)

        assert driver.calls == [("delete_by_date", (from_date, None), {})]

    @pytest.mark.anyio
    async def test_when_deleting_between_dates__then_both_are_passed(self) -> None:
        """When deleting between dates, then both are passed."""
        backend, driver = make_result_backend()
        from_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        to_date = datetime(2026, 2, 1, tzinfo=timezone.utc)

        await backend.delete_by_date(from_date, to_date)

        assert driver.calls == [("delete_by_date", (from_date, to_date), {})]

    @pytest.mark.anyio
    async def test_when_it_starts__then_the_driver_is_set_up_before_on_startup(
        self,
    ) -> None:
        """When it starts, then the driver is set up before on startup."""
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
    async def test_when_it_shuts_down__then_the_driver_is_closed(self) -> None:
        """When it shuts down, then the driver is closed."""
        backend, driver = make_result_backend()

        await backend.shutdown()

        assert driver.calls == [("on_shutdown", (), {})]

    @pytest.mark.anyio
    async def test_when_a_distribution_driver_overrides_on_startup__then_it_runs_once(
        self,
    ) -> None:
        """A distribution driver that overrides on_startup is called once."""
        seen: list[str] = []

        class OverridingDriver(AsyncpgDriver):
            async def on_startup(self) -> None:
                """Record the call and what the component had set by then."""
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


class TestSchedulerSource:
    """The calls of the scheduler source."""

    @pytest.mark.anyio
    async def test_when_schedules_are_read__then_it_selects_id_and_schedule(
        self,
    ) -> None:
        """When schedules are read, then it selects id and schedule."""
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
    async def test_when_a_schedule_is_added__then_it_inserts_without_returning(
        self,
    ) -> None:
        """When a schedule is added, then it inserts without returning."""
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
    async def test_when_a_schedule_is_deleted__then_it_deletes_by_id(self) -> None:
        """When a schedule is deleted, then it deletes by id."""
        source, driver = make_scheduler_source()

        await source.delete_schedule("schedule-1")

        assert driver.calls == [
            ("delete", (source.columns.primary_key, "schedule-1"), {}),
        ]

    @pytest.mark.anyio
    async def test_when_a_timed_schedule_was_sent__then_post_send_deletes_it(
        self,
    ) -> None:
        """When a timed schedule was sent, then post send deletes it."""
        source, driver = make_scheduler_source()
        schedule = a_schedule(time=datetime(2030, 1, 1, tzinfo=timezone.utc))

        await source.post_send(schedule)

        assert driver.calls == [
            ("delete", (source.columns.primary_key, "schedule-1"), {}),
        ]

    @pytest.mark.anyio
    async def test_when_a_cron_schedule_was_sent__then_post_send_calls_nothing(
        self,
    ) -> None:
        """When a cron schedule was sent, then post send calls nothing."""
        source, driver = make_scheduler_source()

        await source.post_send(a_schedule(cron="* * * * *"))

        assert driver.calls == []

    @pytest.mark.anyio
    async def test_when_it_starts__then_the_driver_is_set_up_before_on_startup(
        self,
    ) -> None:
        """When it starts, then the driver is set up before on startup."""
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
    async def test_when_it_shuts_down__then_the_driver_is_closed(self) -> None:
        """When it shuts down, then the driver is closed."""
        source, driver = make_scheduler_source()

        await source.shutdown()

        assert driver.calls == [("on_shutdown", (), {})]
