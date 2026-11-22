"""
Unit tests for how the drivers shape bind parameters before a query.

asyncpg and psycopg share ``encode_params``; psqlpy has its own helper. Both
are plain functions, so these tests need no database.
"""

import uuid

import pytest

from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.drivers._params import encode_params

LABELS = Column("labels", "JSONB")
TASK_ID = Column("task_id", "UUID")
NAME = Column("name", "VARCHAR")


def test_json_and_uuid_are_sent_as_text() -> None:
    task_id = uuid.uuid4()

    shaped = encode_params([LABELS, TASK_ID, NAME], [{"a": 1}, task_id, "x"])

    assert shaped == ['{"a":1}', task_id.hex, "x"]


def test_values_pass_through_without_columns() -> None:
    assert encode_params(None, None) == []
    assert encode_params(None, ["x"]) == ["x"]


def test_column_and_value_counts_must_match() -> None:
    with pytest.raises(ValueError, match="zip"):
        encode_params([NAME, NAME], ["only one"])


def test_psqlpy_wraps_json_and_parses_uuid() -> None:
    pytest.importorskip("psqlpy")
    from psqlpy.extra_types import JSONB  # noqa: PLC0415

    from taskiq_postgresql.drivers._psqlpy import (  # noqa: PLC0415
        encode_psqlpy_params,
    )

    task_id = uuid.uuid4()

    shaped = encode_psqlpy_params([LABELS, TASK_ID], ['{"a": 1}', str(task_id)])

    assert isinstance(shaped[0], JSONB)
    assert shaped[1] == task_id


def test_psqlpy_without_values_sends_no_parameters() -> None:
    pytest.importorskip("psqlpy")
    from taskiq_postgresql.drivers._psqlpy import (  # noqa: PLC0415
        encode_psqlpy_params,
    )

    assert encode_psqlpy_params([], None) == []


def test_psqlpy_column_and_value_counts_must_match() -> None:
    pytest.importorskip("psqlpy")
    from taskiq_postgresql.drivers._psqlpy import (  # noqa: PLC0415
        encode_psqlpy_params,
    )

    with pytest.raises(ValueError, match="zip"):
        encode_psqlpy_params([NAME, NAME], ["only one"])
