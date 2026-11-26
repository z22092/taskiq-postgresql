"""
The Alembic comparator finds one description per folded table name.

Two metadata collections that describe ``Jobs`` and ``jobs`` describe one
relation, which would take two migration lock keys: it is refused, naming both
spellings, as ``add_taskiq_tables`` refuses it.
"""

from __future__ import annotations

import warnings
from types import SimpleNamespace
from typing import Any

import pytest

from taskiq_postgresql.contrib.alembic import _described_tables
from taskiq_postgresql.contrib.sqlalchemy import taskiq_metadata
from taskiq_postgresql.migrations import table_schema

pytestmark = pytest.mark.alembic


def metadata_of(name: str) -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return taskiq_metadata(table_schema("broker", table_name=name))


def context_of(*collections: Any) -> Any:
    """An autogenerate context that filters nothing out."""
    return SimpleNamespace(
        metadata=list(collections),
        run_name_filters=lambda *_: True,
        run_object_filters=lambda *_: True,
    )


def test_two_spellings_in_two_metadata_collections_are_refused() -> None:
    context = context_of(metadata_of("Jobs"), metadata_of("jobs"))

    with pytest.raises(ValueError, match=r"'Jobs'.*'jobs'"):
        _described_tables(context)


def test_one_spelling_in_two_collections_is_described_once() -> None:
    context = context_of(metadata_of("jobs"), metadata_of("jobs"))

    (schema,) = _described_tables(context)

    assert schema.name == "jobs"
