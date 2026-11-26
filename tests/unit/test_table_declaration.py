"""
The declaration of a table, and the one function that describes it.

A ``TableDeclaration`` holds what a component hands its driver, as given; the
description of a table comes from it for a component, a driver or a kind alike.
No test here needs a database: building a driver opens no connection.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.core.sql.catalog import AddedIndex, CreatedAtColumn
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.migrations import _schema, declared_schema, table_schema
from tests.helpers import build_component

KINDS = ("broker", "result_backend", "scheduler_source")
SHAPES = [
    *(("broker", field) for field in ("VarChar", "Text", "Uuid")),
    *(("result_backend", field) for field in ("VarChar", "Text", "Uuid")),
    ("scheduler_source", None),
]
SHAPE_IDS = [f"{kind}-{field}" if field else kind for kind, field in SHAPES]
DSN = "postgresql://postgres@localhost:5432/postgres"


class _Driver(QueryDriver):
    """A query driver with no I/O, to be built from what a component passes."""

    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def insert(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def insert_or_update(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def fetch_row(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def delete_by_date(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def select(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def exists(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def on_startup(self) -> None:
        raise NotImplementedError

    async def on_shutdown(self) -> None:
        raise NotImplementedError


@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
def test_a_declaration_read_from_a_driver_describes_the_kind(
    kind: str,
    field: Optional[str],
) -> None:
    driver = build_component(kind, table_name="jobs", field=field).driver
    driver.component_kind = kind  # type: ignore[assignment]

    assert declared_schema(kind, TableDeclaration.of_driver(driver)) == (
        table_schema(kind, table_name="jobs", field_for_task_id=field)
    )


@pytest.mark.parametrize("kind", KINDS)
def test_of_driver_returns_the_objects_the_driver_keeps(kind: str) -> None:
    driver = build_component(kind).driver

    declaration = TableDeclaration.of_driver(driver)

    assert declaration.table is driver.table
    assert declaration.primary_key is driver.primary_key
    assert declaration.columns is driver.columns
    assert declaration.created_at is driver.created_at
    assert declaration.index_columns is driver.index_columns
    assert declaration.added_columns is driver.added_columns
    assert declaration.added_indexes is driver.added_indexes


def test_of_driver_keeps_no_index_columns_as_none() -> None:
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    driver = _Driver(DSN, "notes", columns=[Column("body", "TEXT")], primary_key=key)

    declaration = TableDeclaration.of_driver(driver)

    assert driver.index_columns is None
    assert declaration.index_columns is None
    assert declaration.added_columns is driver.added_columns


def test_the_table_columns_run_from_the_key_to_created_at() -> None:
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    body = Column("body", "TEXT")
    created_at = CreatedAtColumn()
    driver = _Driver(
        DSN,
        "notes",
        columns=[body],
        primary_key=key,
        created_at=created_at,
    )

    declaration = TableDeclaration.of_driver(driver)

    assert tuple(declaration.table_columns) == (key, body, created_at)


def test_a_declaration_is_kept_as_given_and_not_checked() -> None:
    # The name check stays with the driver's constructor.
    table: Any = "not checked here"
    columns = [Column("a", "TEXT")]
    index = AddedIndex(name="x_idx", columns=columns)
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    created_at = CreatedAtColumn()

    declaration = TableDeclaration(
        table=table,
        primary_key=key,
        columns=columns,
        created_at=created_at,
        index_columns=None,
        added_columns=columns,
        added_indexes=[index],
    )

    assert declaration.table is table
    assert declaration.columns is columns
    assert declaration.added_columns is columns
    assert declaration.index_columns is None
    assert declaration.added_indexes == [index]


@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
def test_a_driver_is_described_by_its_own_builders(
    kind: str,
    field: Optional[str],
) -> None:
    driver = build_component(kind, table_name="jobs", field=field).driver
    driver.component_kind = kind  # type: ignore[assignment]

    expected = _schema(
        kind,  # type: ignore[arg-type]
        driver.create_table_query,
        driver.create_index_query,
        driver.index_columns or (),
        add_columns=driver.add_columns_query,
        added_columns=driver.added_columns,
        added_indexes=driver.added_indexes,
    )

    assert table_schema(driver) == expected
