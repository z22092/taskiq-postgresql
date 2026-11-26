"""
The catalog state reader and the one-description-per-table rule of ``migrations``.

``catalog_state`` reads the value the catalog query returns, for ``read_catalog``
and the CLI alike; ``keep_table_schema`` is the rule every place that collects
table descriptions shares.
"""

from __future__ import annotations

import json
import warnings
from typing import Any

import pytest

from taskiq_postgresql import migrations
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


def test_keep_table_schema_refuses_one_table_described_with_different_columns() -> None:
    broker = _quiet_schema("broker", "jobs")
    results = _quiet_schema("result_backend", "jobs")
    kept: dict[str, migrations.TableSchema] = {}

    migrations.keep_table_schema(kept, broker)
    assert kept == {"jobs": broker}
    migrations.keep_table_schema(kept, broker)
    assert kept == {"jobs": broker}
    with pytest.raises(ValueError, match="described twice, with different columns"):
        migrations.keep_table_schema(kept, results)
