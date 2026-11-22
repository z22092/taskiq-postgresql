"""
Unit tests for how each driver shapes bind parameters before a query.

Building a driver opens no connection, so these tests need no database. The
shaping helpers are private (name-mangled), and they are reached directly
because the SQL they feed is only observable against a live server.
"""

import uuid
from typing import Any, Callable

import pytest

from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.utils import get_db_driver

DSN = "postgresql://postgres@localhost:5432/postgres"
LABELS = Column("labels", "JSONB")
TASK_ID = Column("task_id", "UUID")
NAME = Column("name", "VARCHAR")

# The attribute each driver's private shaping helper is reachable under.
SHAPERS = {
    "asyncpg": "_AsyncpgDriver__parser_query",
    "psqlpy": "_PsqlpyDriver__parser_query",
    "psycopg": "_PsycopgDriver__parser_params",
}


def shaper(driver: str) -> Callable[[Any, Any], list[Any]]:
    """Return the bound parameter-shaping helper of a fresh ``driver``."""
    instance = get_db_driver(driver)(
        DSN,
        "messages",
        columns=[TASK_ID, NAME, LABELS],
        primary_key=PrimaryKeyColumn(),
    )
    return getattr(instance, SHAPERS[driver])


@pytest.mark.parametrize("name", ["asyncpg", "psycopg"])
def test_text_protocol_drivers_send_json_and_uuid_as_text(name: str) -> None:
    pytest.importorskip(name)
    task_id = uuid.uuid4()

    shaped = shaper(name)([LABELS, TASK_ID, NAME], [{"a": 1}, task_id, "x"])

    assert shaped == ['{"a":1}', task_id.hex, "x"]


@pytest.mark.parametrize("name", ["asyncpg", "psycopg"])
def test_text_protocol_drivers_pass_values_through_without_columns(
    name: str,
) -> None:
    pytest.importorskip(name)

    assert shaper(name)(None, None) == []
    assert shaper(name)(None, ["x"]) == ["x"]


def test_psqlpy_wraps_json_and_parses_uuid() -> None:
    pytest.importorskip("psqlpy")
    from psqlpy.extra_types import JSONB  # noqa: PLC0415

    task_id = uuid.uuid4()

    shaped = shaper("psqlpy")([LABELS, TASK_ID], ['{"a": 1}', str(task_id)])

    assert isinstance(shaped[0], JSONB)
    assert shaped[1] == task_id


def test_psqlpy_without_values_sends_no_parameters() -> None:
    pytest.importorskip("psqlpy")

    assert shaper("psqlpy")(None, None) == []


def test_column_and_value_counts_must_match(driver: str) -> None:
    with pytest.raises(ValueError, match="zip"):
        shaper(driver)([NAME, NAME], ["only one"])


def test_create_table_puts_the_key_first_and_created_at_last(driver: str) -> None:
    instance = get_db_driver(driver)(
        DSN,
        "messages",
        columns=[TASK_ID, NAME],
        primary_key=PrimaryKeyColumn(type_="SERIAL"),
    )

    assert instance.create_table_query.make_query() == (
        "CREATE TABLE IF NOT EXISTS messages ("
        "id SERIAL NOT NULL PRIMARY KEY, "
        "task_id UUID NOT NULL, "
        "name VARCHAR NOT NULL, "
        "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW())"
    )
