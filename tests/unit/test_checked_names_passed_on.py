"""
The checked names travel on instead of being parsed again.

One catalog state reader, one spelling rule for every place that collects table
descriptions, and one channel check in the base ``ListenDriver``.
"""

from __future__ import annotations

import json
import warnings
from collections.abc import AsyncIterator
from typing import Any

import pytest

from taskiq_postgresql import migrations
from taskiq_postgresql.abc.driver import ListenDriver
from taskiq_postgresql.contrib.sqlalchemy import add_taskiq_tables
from taskiq_postgresql.core.sql import catalog
from taskiq_postgresql.core.sql.catalog import Column
from taskiq_postgresql.core.sql.identifier import Identifier, IdentifierError
from taskiq_postgresql.core.sql.statement import ParamType
from taskiq_postgresql.migrations import table_schema

_EMPTY_CATALOG = json.dumps(
    {
        "tables": [
            {
                "name": "jobs",
                "relkind": None,
                "columns": None,
                "not_null": None,
                "indexes": [],
            },
        ],
    },
)


def _quiet_schema(kind: Any, name: str) -> migrations.TableSchema:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return table_schema(kind, table_name=name)


# --- catalog_state -----------------------------------------------------------


def test_catalog_state_reads_the_value_the_catalog_query_returns() -> None:
    state = migrations.catalog_state(_EMPTY_CATALOG)

    assert set(state.tables) == {"jobs"}
    assert state.tables["jobs"].relkind is None


def test_catalog_state_refuses_a_missing_row() -> None:
    with pytest.raises(ValueError, match="no row"):
        migrations.catalog_state(None)


def test_catalog_state_refuses_a_value_that_is_not_text() -> None:
    with pytest.raises(ValueError, match="dict, not the JSON text"):
        migrations.catalog_state({"tables": []})


@pytest.mark.anyio
async def test_read_catalog_reads_what_catalog_state_reads() -> None:
    schema = _quiet_schema("broker", "jobs")
    seen: list[str] = []

    async def fetch_value(query: str) -> Any:
        seen.append(query)
        return _EMPTY_CATALOG

    state = await migrations.read_catalog([schema], fetch_value)

    assert seen == [migrations.catalog_query([schema])]
    assert state == migrations.catalog_state(_EMPTY_CATALOG)


# --- one spelling rule -------------------------------------------------------


def test_keep_table_schema_refuses_one_table_described_with_different_columns() -> None:
    broker = _quiet_schema("broker", "jobs")
    results = _quiet_schema("result_backend", "jobs")
    kept: dict[str, migrations.TableSchema] = {}

    assert migrations.keep_table_schema(kept, broker) is True
    assert migrations.keep_table_schema(kept, broker) is False
    with pytest.raises(ValueError, match="described twice, with different columns"):
        migrations.keep_table_schema(kept, results)


def test_add_taskiq_tables_refuses_one_table_described_with_different_columns() -> None:
    from sqlalchemy import MetaData  # noqa: PLC0415

    broker = _quiet_schema("broker", "jobs")
    results = _quiet_schema("result_backend", "jobs")

    metadata = MetaData()
    add_taskiq_tables(metadata, broker)

    with pytest.raises(ValueError, match="described twice, with different columns"):
        add_taskiq_tables(metadata, results)


# --- the base ListenDriver ---------------------------------------------------


class _Listen(ListenDriver):
    async def on_startup(self) -> None: ...

    async def on_shutdown(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[str]:
        raise NotImplementedError


def test_the_base_listen_driver_refuses_a_channel_outside_the_rule() -> None:
    with pytest.raises(IdentifierError, match="reserved word"):
        _Listen("postgresql://unused", "select")


def test_the_base_listen_driver_keeps_the_folded_channel() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        driver = _Listen("postgresql://unused", "Jobs_Mixed")

    assert driver.channel == Identifier.parse(
        "Jobs_Mixed",
        what="channel name",
        warn=False,
    )
    assert driver.channel_name == "jobs_mixed"


# --- ExistsQuery -------------------------------------------------------------


def test_exists_query_text_does_not_type_the_key() -> None:
    # No ParamType covers NUMERIC: a third-party driver with such a key column
    # still gets the text.
    key = Column("id", "NUMERIC")

    text = catalog.ExistsQuery("jobs").make_query(key)

    assert text == "SELECT 1 FROM jobs WHERE id = $1"


def test_exists_query_statement_types_the_key_and_the_constant_column() -> None:
    key = Column("id", "SERIAL")

    statement = catalog.ExistsQuery("jobs").statement(key)

    assert statement.text == "SELECT 1 FROM jobs WHERE id = $1"
    assert statement.params == (ParamType.INT4,)
    assert statement.columns == (("?column?", ParamType.INT4),)
