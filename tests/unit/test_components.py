"""
Unit tests for what the components hand to their driver.

Construction opens no connection, so none of these tests needs a database.
"""

from typing import Any, Optional, Union

import pytest

from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.broker import PostgresqlBroker, _parse_message_id
from taskiq_postgresql.exceptions import DatabaseConnectionError
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from taskiq_postgresql.utils import get_db_driver

DSN = "postgresql://postgres@localhost:5432/postgres"
TIMESTAMP = "TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()"

Component = Union[
    type[PostgresqlBroker],
    type[PostgresqlResultBackend[Any]],
    type[PostgresqlSchedulerSource],
]


# The tables each component creates with run_migrations=True. Existing
# deployments already hold these tables, so a change here is a schema change.
SCHEMAS = [
    pytest.param(
        PostgresqlBroker,
        "CREATE TABLE IF NOT EXISTS taskiq_messages ("
        "id SERIAL NOT NULL PRIMARY KEY, "
        "task_id Uuid NOT NULL, "
        "task_name VARCHAR NOT NULL, "
        "message BYTEA NOT NULL, "
        "labels JSONB NOT NULL, "
        f"created_at {TIMESTAMP})",
        "CREATE INDEX IF NOT EXISTS taskiq_messages_id_idx "
        "ON taskiq_messages USING HASH (id);",
        id="broker",
    ),
    pytest.param(
        PostgresqlResultBackend,
        "CREATE TABLE IF NOT EXISTS taskiq_results ("
        "task_id Uuid NOT NULL PRIMARY KEY, "
        "result BYTEA NOT NULL, "
        f"created_at {TIMESTAMP})",
        "CREATE INDEX IF NOT EXISTS taskiq_results_task_id_idx "
        "ON taskiq_results USING HASH (task_id);",
        id="result_backend",
    ),
    pytest.param(
        PostgresqlSchedulerSource,
        "CREATE TABLE IF NOT EXISTS taskiq_schedulers ("
        "id UUID NOT NULL PRIMARY KEY, "
        "task_name VARCHAR(100) NOT NULL, "
        "schedule JSONB NOT NULL, "
        f"updated_at {TIMESTAMP}, "
        f"created_at {TIMESTAMP})",
        "CREATE INDEX IF NOT EXISTS taskiq_schedulers_id_idx "
        "ON taskiq_schedulers USING HASH (id);\n"
        "CREATE INDEX IF NOT EXISTS taskiq_schedulers_task_name_idx "
        "ON taskiq_schedulers USING HASH (task_name);",
        id="scheduler_source",
    ),
]


@pytest.mark.parametrize(("component", "table", "indexes"), SCHEMAS)
def test_default_schema(
    component: Component,
    table: str,
    indexes: str,
    driver: str,
) -> None:
    built_driver = component(DSN, driver=driver).driver
    index_columns = built_driver.index_columns
    assert index_columns is not None

    assert built_driver.create_table_query.make_query() == table
    assert built_driver.create_index_query.make_query(index_columns) == indexes


def never_connected(driver: str) -> Any:
    """Return a query driver of ``driver`` that has not opened its pool."""
    return get_db_driver(driver)(
        DSN,
        "messages",
        columns=[Column("name", "VARCHAR")],
        primary_key=PrimaryKeyColumn(),
    )


@pytest.mark.anyio
async def test_when_driver_never_connected__then_shutdown_succeeds(
    driver: str,
) -> None:
    instance = never_connected(driver)

    await instance.on_shutdown()

    assert instance.pool is None


@pytest.mark.anyio
async def test_when_no_component_owns_the_driver__then_startup_checks_nothing(
    driver: str,
) -> None:
    # Only a component can tell the driver what kind of table it holds, so
    # a driver built alone and not migrating sends no query at startup.
    instance = never_connected(driver)

    await instance.on_startup()

    assert instance.pool is None


@pytest.mark.anyio
async def test_when_scheduler_source_never_queried__then_shutdown_succeeds(
    driver: str,
) -> None:
    # startup() reads the catalog, which opens the driver's pool: only a
    # source that never started has no pool to close.
    source = PostgresqlSchedulerSource(DSN, driver=driver)

    await source.shutdown()


@pytest.mark.anyio
async def test_when_connection_is_used_outside_the_driver_context__then_it_fails(
    driver: str,
) -> None:
    instance = never_connected(driver)

    with pytest.raises(DatabaseConnectionError):
        async with instance.connection():
            pass


@pytest.mark.parametrize(
    ("payload", "message_id"),
    [
        ("1", 1),
        ("2147483647", 2147483647),
        ("0", None),
        ("2147483648", None),
        ("-1", None),
        ("not-a-number", None),
    ],
)
def test_the_broker_hands_a_claim_only_ids_its_table_can_hold(
    payload: str,
    message_id: Optional[int],
) -> None:
    # The broker's id column is a SERIAL: 1 to 2147483647.
    assert _parse_message_id(payload) == message_id
