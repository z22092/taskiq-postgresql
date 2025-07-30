import uuid
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from taskiq import AsyncTaskiqDecoratedTask as TaskiqTask
from taskiq import ScheduledTask

from taskiq_postgresql.exceptions import DatabaseConnectionError
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

pytestmark = pytest.mark.anyio


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


async def test_failure_connection_database() -> None:
    """Test exception raising when connecting to invalid database."""
    with pytest.raises(expected_exception=DatabaseConnectionError):
        scheduler = PostgresqlSchedulerSource(
            dsn="postgresql://postgres:postgres@localhost:5432/nonexistent_db",
            table_name="test_table",
        )
        await scheduler.startup()


async def test_scheduler_initialization() -> None:
    """Test scheduler source initialization with various parameters."""
    # Test with string DSN
    scheduler = PostgresqlSchedulerSource(
        dsn="postgresql://postgres:postgres@localhost:5432/test",
        table_name="custom_table",
        driver="asyncpg",
    )
    assert scheduler.table_name == "custom_table"
    assert scheduler.dsn == "postgresql://postgres:postgres@localhost:5432/test"

    # Test with callable DSN
    def get_dsn() -> str:
        return "postgresql://postgres:postgres@localhost:5432/test"

    scheduler_callable = PostgresqlSchedulerSource(
        dsn=get_dsn,
        table_name="custom_table",
    )
    assert (
        scheduler_callable.dsn == "postgresql://postgres:postgres@localhost:5432/test"
    )


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
        # Cleanup
        async with scheduler.driver, scheduler.driver.connection() as connection:
            await connection.execute(f"DROP TABLE {postgres_table}")

        await scheduler.shutdown()


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
        # Cleanup
        # Cleanup
        async with scheduler.driver, scheduler.driver.connection() as connection:
            await connection.execute(f"DROP TABLE {postgres_table}")
        await scheduler.shutdown()


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


@pytest.mark.parametrize(
    "invalid_dsn",
    [
        "postgresql://postgres:wrong@localhost:5432/postgres",
        "postgresql://postgres:postgres@nonexistent:5432/postgres",
        "invalid://connection/string",
    ],
)
async def test_connection_failures(invalid_dsn: str) -> None:
    """Test various connection failure scenarios."""
    scheduler = PostgresqlSchedulerSource(
        dsn=invalid_dsn,
        table_name="test_table",
    )

    with pytest.raises(DatabaseConnectionError):
        await scheduler.startup()
