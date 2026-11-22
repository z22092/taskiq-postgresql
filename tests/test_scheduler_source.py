import json
import logging
import uuid
import zoneinfo
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from taskiq import AsyncTaskiqDecoratedTask as TaskiqTask
from taskiq import ScheduledTask

from taskiq_postgresql.exceptions import DatabaseConnectionError
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import drop_and_shutdown

pytestmark = pytest.mark.anyio

# The IANA database has several hundred names; fewer means tzdata is missing.
MIN_KNOWN_ZONES = 100


@pytest.fixture
async def mock_broker() -> Mock:
    """Create a mock broker for testing startup schedules."""
    broker = Mock()
    mock_task = Mock(spec=TaskiqTask)
    mock_task.labels = {"test": "label"}
    broker.find_task.return_value = mock_task
    return broker


@pytest.fixture
def sample_scheduled_task() -> ScheduledTask:
    """Create a sample scheduled task for testing."""
    return ScheduledTask(
        task_name="test_task",
        labels={"env": "test"},
        schedule_id=uuid.uuid4().hex,
        args=["arg1", "arg2"],
        kwargs={"key": "value"},
        cron="0 */6 * * *",
        cron_offset="UTC",
    )


@pytest.fixture
def time_based_scheduled_task() -> ScheduledTask:
    """Create a time-based scheduled task for testing post_send."""
    return ScheduledTask(
        task_name="time_task",
        labels={"type": "time"},
        schedule_id=uuid.uuid4().hex,
        args=[],
        kwargs={},
        time=datetime.now(timezone.utc),
    )


async def test_failure_connection_database(missing_database_dsn: str) -> None:
    """Test exception raising when connecting to invalid database."""
    with pytest.raises(expected_exception=DatabaseConnectionError):
        scheduler = PostgresqlSchedulerSource(
            dsn=missing_database_dsn,
            table_name="test_table",
            run_migrations=True,
        )
        await scheduler.startup()


async def test_scheduler_initialization() -> None:
    """Test scheduler source initialization with various parameters."""
    # Test with string DSN
    scheduler = PostgresqlSchedulerSource(
        dsn="postgresql://postgres@localhost:5432/test",
        table_name="custom_table",
        driver="asyncpg",
    )
    assert scheduler.table_name == "custom_table"
    assert scheduler.dsn == "postgresql://postgres@localhost:5432/test"

    # Test with callable DSN
    def get_dsn() -> str:
        return "postgresql://postgres@localhost:5432/test"

    scheduler_callable = PostgresqlSchedulerSource(
        dsn=get_dsn,
        table_name="custom_table",
    )
    assert scheduler_callable.dsn == "postgresql://postgres@localhost:5432/test"


async def test_startup_creates_table(
    scheduler_source: PostgresqlSchedulerSource,
) -> None:
    """Test that startup creates the scheduler table."""
    # Verify table exists and is empty
    table_exists = await scheduler_source.driver.execute(
        f"SELECT * FROM {scheduler_source.table_name}",  # noqa: S608
    )
    assert table_exists == []


async def test_add_and_get_schedules(
    scheduler_source: PostgresqlSchedulerSource,
    sample_scheduled_task: ScheduledTask,
) -> None:
    """Test adding and retrieving schedules."""
    # Initially no schedules
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 0

    # Add a schedule
    await scheduler_source.add_schedule(sample_scheduled_task)

    # Retrieve schedules
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 1

    retrieved_schedule = schedules[0]
    assert retrieved_schedule.task_name == sample_scheduled_task.task_name
    assert retrieved_schedule.schedule_id == sample_scheduled_task.schedule_id
    assert retrieved_schedule.cron == sample_scheduled_task.cron
    assert retrieved_schedule.args == sample_scheduled_task.args
    assert retrieved_schedule.kwargs == sample_scheduled_task.kwargs
    assert retrieved_schedule.labels == sample_scheduled_task.labels


async def test_add_multiple_schedules(
    scheduler_source: PostgresqlSchedulerSource,
) -> None:
    """Test adding multiple schedules for the same task."""
    task_name = "multi_schedule_task"

    # Create multiple schedules for the same task
    schedule1 = ScheduledTask(
        task_name=task_name,
        labels={"env": "test"},
        schedule_id=uuid.uuid4().hex,
        cron="0 */6 * * *",
        args=[],
        kwargs={},
    )

    schedule2 = ScheduledTask(
        task_name=task_name,
        labels={"env": "test"},
        schedule_id=uuid.uuid4().hex,
        cron="0 */12 * * *",
        args=[],
        kwargs={},
    )

    await scheduler_source.add_schedule(schedule1)
    await scheduler_source.add_schedule(schedule2)

    schedules = await scheduler_source.get_schedules()
    expected_schedules = 2
    assert len(schedules) == expected_schedules

    # Verify both schedules are retrieved
    schedule_ids = {schedule.schedule_id for schedule in schedules}
    assert schedule1.schedule_id in schedule_ids
    assert schedule2.schedule_id in schedule_ids


async def test_delete_schedule(
    scheduler_source: PostgresqlSchedulerSource,
    sample_scheduled_task: ScheduledTask,
) -> None:
    """Test deleting a schedule."""
    # Add a schedule
    await scheduler_source.add_schedule(sample_scheduled_task)

    # Verify it exists
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 1

    # Delete the schedule
    await scheduler_source.delete_schedule(sample_scheduled_task.schedule_id)

    # Verify it's gone
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 0


async def test_delete_nonexistent_schedule(
    scheduler_source: PostgresqlSchedulerSource,
) -> None:
    """Test deleting a schedule that doesn't exist (should not raise error)."""
    fake_schedule_id = uuid.uuid4().hex
    # This should not raise an exception
    await scheduler_source.delete_schedule(fake_schedule_id)


async def test_post_send_deletes_time_based_task(
    scheduler_source: PostgresqlSchedulerSource,
    time_based_scheduled_task: ScheduledTask,
    sample_scheduled_task: ScheduledTask,
) -> None:
    """Test that post_send deletes time-based tasks but keeps cron tasks."""
    # Add both time-based and cron-based tasks
    await scheduler_source.add_schedule(time_based_scheduled_task)
    await scheduler_source.add_schedule(sample_scheduled_task)

    # Verify both exist
    schedules = await scheduler_source.get_schedules()
    expected_schedules = 2
    assert len(schedules) == expected_schedules

    # Call post_send on time-based task
    await scheduler_source.post_send(time_based_scheduled_task)

    # Verify only cron task remains
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 1
    assert schedules[0].schedule_id == sample_scheduled_task.schedule_id

    # Call post_send on cron task (should not delete it)
    await scheduler_source.post_send(sample_scheduled_task)

    # Verify cron task still exists
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 1
    assert schedules[0].schedule_id == sample_scheduled_task.schedule_id


async def test_startup_schedule_processing(
    postgresql_dsn: str,
    postgres_table: str,
    mock_broker: Mock,
) -> None:
    """Test startup schedule processing functionality."""
    startup_schedule = {
        "test_task": [
            {
                "cron": "0 */6 * * *",
                "cron_offset": "UTC",
                "args": ["startup_arg"],
                "kwargs": {"startup": True},
            },
            {
                "time": datetime.now(timezone.utc).isoformat(),
                "args": ["time_arg"],
            },
        ],
        "missing_task": [
            {"cron": "0 */12 * * *"},
        ],
        "invalid_schedule": [
            {"invalid": "schedule"},  # Missing cron/time
        ],
    }

    # Mock broker to return task for "test_task" but None for "missing_task"
    def mock_find_task(task_name: str) -> TaskiqTask | None:
        if task_name == "test_task":
            task = Mock(spec=TaskiqTask)
            task.labels = {"from": "broker"}
            return task
        return None

    mock_broker.find_task.side_effect = mock_find_task

    scheduler = PostgresqlSchedulerSource(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        startup_schedule=startup_schedule,
        broker=mock_broker,
        run_migrations=True,
    )

    await scheduler.startup()

    try:
        # Verify schedules were created
        schedules = await scheduler.get_schedules()
        expected_schedules = 2
        # Should have 2 schedules (both valid schedules for "test_task")
        # "missing_task" and "invalid_schedule" should be skipped
        assert len(schedules) == expected_schedules

        # Verify all schedules are for "test_task"
        for schedule in schedules:
            assert schedule.task_name == "test_task"
            assert schedule.labels == {"from": "broker"}

        # Verify one has cron and one has time
        cron_schedules = [s for s in schedules if s.cron is not None]
        time_schedules = [s for s in schedules if s.time is not None]
        expected_cron_schedules = 1
        expected_time_schedules = 1
        assert len(cron_schedules) == expected_cron_schedules
        assert len(time_schedules) == expected_time_schedules

        assert cron_schedules[0].cron == "0 */6 * * *"
        assert cron_schedules[0].args == ["startup_arg"]
        assert cron_schedules[0].kwargs == {"startup": True}

    finally:
        await drop_and_shutdown(scheduler, postgres_table)


async def test_startup_schedule_no_duplicates(
    postgresql_dsn: str,
    postgres_table: str,
    mock_broker: Mock,
) -> None:
    """Test that startup schedules don't create duplicates on multiple startups."""
    startup_schedule = {
        "test_task": [
            {"cron": "0 */6 * * *"},
        ],
    }

    mock_task = Mock(spec=TaskiqTask)
    mock_task.labels = {"test": "label"}
    mock_broker.find_task.return_value = mock_task

    scheduler = PostgresqlSchedulerSource(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        startup_schedule=startup_schedule,
        broker=mock_broker,
        run_migrations=True,
    )

    # First startup
    await scheduler.startup()
    schedules = await scheduler.get_schedules()
    assert len(schedules) == 1

    # Restart (shutdown and startup again)
    await scheduler.shutdown()
    await scheduler.startup()

    try:
        # Should still have only one schedule (no duplicates)
        schedules = await scheduler.get_schedules()
        assert len(schedules) == 1

    finally:
        await drop_and_shutdown(scheduler, postgres_table)


async def test_startup_without_broker_or_startup_schedule(
    scheduler_source: PostgresqlSchedulerSource,
) -> None:
    """Test startup when no broker or startup_schedule is provided."""
    # This should work without any issues
    # The scheduler_source fixture already calls startup, so if we get here, it worked
    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 0


async def test_schedule_serialization(
    scheduler_source: PostgresqlSchedulerSource,
) -> None:
    """Test that complex schedule data is properly serialized and deserialized."""
    complex_schedule = ScheduledTask(
        task_name="complex_task",
        labels={"env": "test", "priority": "high", "numbers": [1, 2, 3]},
        schedule_id=uuid.uuid4().hex,
        args=["string", 42, True, {"nested": "dict"}],
        kwargs={
            "complex_key": {"nested": {"deeply": "nested"}},
            "list_value": [1, "two", {"three": 3}],
            "none_value": None,
        },
        cron="0 */6 * * *",
        cron_offset="UTC",
    )

    await scheduler_source.add_schedule(complex_schedule)

    schedules = await scheduler_source.get_schedules()
    assert len(schedules) == 1

    retrieved = schedules[0]
    assert retrieved.task_name == complex_schedule.task_name
    assert retrieved.labels == complex_schedule.labels
    assert retrieved.args == complex_schedule.args
    assert retrieved.kwargs == complex_schedule.kwargs
    assert retrieved.cron == complex_schedule.cron
    assert retrieved.cron_offset == complex_schedule.cron_offset


async def test_connection_failures(connection_failure_dsn: str) -> None:
    """Test various connection failure scenarios."""
    scheduler = PostgresqlSchedulerSource(
        dsn=connection_failure_dsn,
        table_name="test_table",
        run_migrations=True,
    )

    with pytest.raises(DatabaseConnectionError):
        await scheduler.startup()


async def insert_raw_schedule(
    scheduler: PostgresqlSchedulerSource,
    schedule_id: str,
    schedule: dict[str, Any],
) -> None:
    """Store ``schedule`` as JSON exactly as given, bypassing validation."""
    await scheduler.driver.insert(
        [
            scheduler.columns.primary_key,
            scheduler.columns.task_name,
            scheduler.columns.schedule,
        ],
        [schedule_id, schedule["task_name"], json.dumps(schedule)],
    )


async def test_when_a_row_has_a_numeric_cron_offset__then_it_loads_as_seconds(
    scheduler_source: PostgresqlSchedulerSource,
    sample_scheduled_task: ScheduledTask,
) -> None:
    """Rows written under pydantic v1 hold a timedelta offset as seconds."""
    legacy = sample_scheduled_task.model_dump(mode="json")
    legacy["cron_offset"] = 3600.0
    await insert_raw_schedule(
        scheduler_source,
        sample_scheduled_task.schedule_id,
        legacy,
    )

    schedules = await scheduler_source.get_schedules()

    assert len(schedules) == 1
    assert schedules[0].schedule_id == sample_scheduled_task.schedule_id
    assert schedules[0].cron_offset == timedelta(hours=1)


async def test_when_a_row_does_not_validate__then_only_that_row_is_skipped(
    scheduler_source: PostgresqlSchedulerSource,
    sample_scheduled_task: ScheduledTask,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One bad row must not hide every other schedule."""
    broken_id = uuid.uuid4().hex
    # No cron, time or interval: no taskiq version accepts it.
    broken = {
        "task_name": "broken_task",
        "labels": {},
        "args": [],
        "kwargs": {},
        "schedule_id": broken_id,
    }
    await insert_raw_schedule(scheduler_source, broken_id, broken)
    await scheduler_source.add_schedule(sample_scheduled_task)

    with caplog.at_level(logging.WARNING):
        schedules = await scheduler_source.get_schedules()

    assert [schedule.schedule_id for schedule in schedules] == [
        sample_scheduled_task.schedule_id,
    ]
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]
    assert any(str(uuid.UUID(broken_id)) in message for message in warnings), warnings


async def insert_raw_json_row(
    scheduler: PostgresqlSchedulerSource,
    schedule_id: str,
    document: Any,
) -> None:
    """Store any JSON document, object or not, as the row's schedule."""
    text = json.dumps(document).replace("'", "''")
    await scheduler.driver.execute(
        f"INSERT INTO {scheduler.table_name} (id, task_name, schedule) "  # noqa: S608
        f"VALUES ('{schedule_id}', 'broken_task', '{text}'::jsonb)",
    )


ERROR_TYPES = ("TypeError", "ValueError", "OverflowError", "ValidationError")


@pytest.mark.parametrize(
    "kind",
    ["empty_list", "number", "huge_cron_offset", "huge_duration_cron_offset"],
)
async def test_when_a_row_cannot_become_a_schedule__then_only_that_row_is_skipped(
    scheduler_source: PostgresqlSchedulerSource,
    sample_scheduled_task: ScheduledTask,
    caplog: pytest.LogCaptureFixture,
    kind: str,
) -> None:
    """Rows that are not objects, or overflow timedelta, are skipped too."""
    broken_id = uuid.uuid4().hex
    marker = uuid.uuid4().hex
    documents: dict[str, Any] = {
        "empty_list": [],
        "number": 42,
        "huge_cron_offset": {
            **sample_scheduled_task.model_dump(mode="json"),
            "schedule_id": broken_id,
            "cron_offset": 1e20,
            "kwargs": {"marker": marker},
        },
        "huge_duration_cron_offset": {
            **sample_scheduled_task.model_dump(mode="json"),
            "schedule_id": broken_id,
            "cron_offset": "-P999999999DT1S",
            "kwargs": {"marker": marker},
        },
    }
    await insert_raw_json_row(scheduler_source, broken_id, documents[kind])
    valid = sample_scheduled_task.model_copy(update={"schedule_id": uuid.uuid4().hex})
    await scheduler_source.add_schedule(valid)

    with caplog.at_level(logging.DEBUG):
        schedules = await scheduler_source.get_schedules()

    assert [schedule.schedule_id for schedule in schedules] == [valid.schedule_id]
    messages = [record.getMessage() for record in caplog.records]
    skipped = [message for message in messages if str(uuid.UUID(broken_id)) in message]
    assert skipped, messages
    assert any(name in message for message in skipped for name in ERROR_TYPES)
    assert not any(marker in message for message in messages), messages


async def test_when_a_row_is_not_json__then_it_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A schedule that does not decode as JSON is skipped like any other."""
    scheduler = PostgresqlSchedulerSource("postgresql://postgres@localhost:5432/x")
    row_id = uuid.uuid4()

    async def select(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return [{"id": row_id, "schedule": "{not json"}]

    monkeypatch.setattr(scheduler.driver, "select", select)

    with caplog.at_level(logging.WARNING):
        assert await scheduler.get_schedules() == []

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        str(row_id) in message and "JSONDecodeError" in message for message in messages
    ), messages


async def test_when_a_row_cannot_become_a_schedule__then_startup_schedule_runs(
    scheduler_source: PostgresqlSchedulerSource,
    postgresql_dsn: str,
    driver: str,
    mock_broker: Mock,
) -> None:
    """startup() reads the stored schedules first; a bad row must not stop it."""
    await insert_raw_json_row(scheduler_source, uuid.uuid4().hex, 42)
    scheduler = PostgresqlSchedulerSource(
        dsn=postgresql_dsn,
        table_name=scheduler_source.table_name,
        driver=driver,
        startup_schedule={"test_task": [{"cron": "0 */6 * * *"}]},
        broker=mock_broker,
    )

    await scheduler.startup()
    try:
        schedules = await scheduler.get_schedules()
    finally:
        await scheduler.shutdown()

    assert [schedule.task_name for schedule in schedules] == ["test_task"]


@pytest.mark.parametrize(
    "offset",
    [
        timedelta(hours=1),
        timedelta(hours=-3),
        timedelta(days=1, minutes=30),
        timedelta(seconds=90.5),
        "Europe/Berlin",
        "America/Sao_Paulo",
    ],
)
async def test_when_cron_offset_is_stored__then_it_loads_with_its_type(
    scheduler_source: PostgresqlSchedulerSource,
    sample_scheduled_task: ScheduledTask,
    offset: timedelta | str,
) -> None:
    """A stored ISO 8601 duration must not come back as a time zone name."""
    await scheduler_source.add_schedule(
        sample_scheduled_task.model_copy(update={"cron_offset": offset}),
    )

    schedules = await scheduler_source.get_schedules()

    assert len(schedules) == 1
    assert type(schedules[0].cron_offset) is type(offset)
    assert schedules[0].cron_offset == offset


def test_when_cron_offset_is_a_known_zone__then_it_is_not_a_duration() -> None:
    """No IANA zone name may be mistaken for a duration."""
    from taskiq_postgresql.scheduler_source import _read_offset  # noqa: PLC0415

    names = zoneinfo.available_timezones()
    # Without tzdata the set is empty and the loop below would pass vacuously.
    assert len(names) > MIN_KNOWN_ZONES, "tzdata is missing: the sweep is vacuous"

    assert [name for name in sorted(names) if _read_offset(name) != name] == []
