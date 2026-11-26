"""
The declaration of a table, and the one function that describes it.

A ``TableDeclaration`` holds what a component hands its driver, as given; the
description of a table comes from it for a component, a driver or a kind alike.
No test here needs a database: building a driver opens no connection.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Optional

import pytest
from typing_extensions import override

from taskiq_postgresql import migrations
from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.core.sql.catalog import AddedIndex, CreatedAtColumn
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.migrations import declared_schema, table_schema
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

    @override
    async def __aenter__(self) -> Any:
        return self

    @override
    async def __aexit__(self, *exc_info: object) -> None:
        return None

    @override
    async def insert(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def insert_or_update(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def fetch_row(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def delete_by_date(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def select(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def exists(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def on_startup(self) -> None:
        raise NotImplementedError

    @override
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
    assert declaration.steps is driver.steps
    # What the steps add, flattened, is what the driver keeps as its own.
    assert declaration.added_columns == tuple(driver.added_columns)
    assert declaration.added_indexes == tuple(driver.added_indexes)


def test_of_driver_keeps_no_index_columns_as_none() -> None:
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    driver = _Driver(DSN, "notes", columns=[Column("body", "TEXT")], primary_key=key)

    declaration = TableDeclaration.of_driver(driver)

    assert driver.index_columns is None
    assert declaration.index_columns is None
    assert declaration.steps == ()
    assert declaration.added_columns == ()


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
    from taskiq_postgresql.core.sql.catalog import DeclaredStep  # noqa: PLC0415

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
        steps=[DeclaredStep(added_columns=columns, added_indexes=[index])],
    )

    assert declaration.table is table
    assert declaration.columns is columns
    assert declaration.steps[0].added_columns is columns
    assert declaration.index_columns is None
    assert declaration.steps[0].added_indexes == [index]


@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
def test_a_driver_is_described_by_its_own_builders(
    kind: str,
    field: Optional[str],
) -> None:
    driver = build_component(kind, table_name="jobs", field=field).driver
    driver.component_kind = kind  # type: ignore[assignment]

    expected = migrations._schema(
        kind,  # type: ignore[arg-type]
        driver.create_table_query,
        driver.create_index_query,
        driver.index_columns or (),
        steps=driver.steps,
    )

    assert table_schema(driver) == expected


# --- the steps are an ordered list on the declaration --------------------------

RESULTS_STEP_2 = (
    "ALTER TABLE taskiq_results\n  ADD COLUMN IF NOT EXISTS progress BYTEA",
    "ALTER TABLE taskiq_results ALTER COLUMN result DROP NOT NULL",
)


@pytest.mark.parametrize("field", ["VarChar", "Text", "Uuid"])
def test_the_results_table_has_a_second_step_that_adds_progress(field: str) -> None:
    schema = table_schema("result_backend", field_for_task_id=field)

    step_1, step_2 = schema.steps
    assert (step_1.number, step_2.number) == (1, 2)
    assert step_2.statements == RESULTS_STEP_2
    assert step_2.objects == migrations.StepObjects(
        creates_table=False,
        columns=("progress",),
        indexes=(),
        nullable_columns=("result",),
    )
    # Step 1 is the table a released version creates, with result NOT NULL.
    assert step_1.objects.nullable_columns == ()
    assert "result BYTEA NOT NULL" in step_1.statements[0]


def test_the_results_final_description_has_result_nullable_and_progress_last() -> None:
    schema = table_schema("result_backend")

    assert [column.name for column in schema.columns] == [
        "task_id",
        "result",
        "created_at",
        "progress",
    ]
    by_name = {column.name: column for column in schema.columns}
    assert by_name["result"].nullable
    assert by_name["result"].type_name == "bytea"
    progress = by_name["progress"]
    assert (progress.sql_type, progress.type_name) == ("BYTEA", "bytea")
    assert progress.nullable
    assert progress.default is None
    assert not by_name["task_id"].nullable
    assert not by_name["created_at"].nullable
    assert schema.indexes == schema.steps[0].objects.indexes


def test_the_broker_and_the_scheduler_keep_the_steps_they_had() -> None:
    broker = table_schema("broker")
    scheduler = table_schema("scheduler_source")

    assert [step.number for step in broker.steps] == [1, 2]
    assert broker.steps[1].objects.columns == ("channel", "not_before")
    assert all(step.objects.nullable_columns == () for step in broker.steps)
    assert [step.number for step in scheduler.steps] == [1]


@pytest.mark.parametrize(("kind", "field"), SHAPES, ids=SHAPE_IDS)
def test_a_component_declares_the_steps_its_kind_describes(
    kind: str,
    field: Optional[str],
) -> None:
    component = build_component(kind, table_name="jobs", field=field)

    assert migrations.table_schema(component) == table_schema(
        kind,
        table_name="jobs",
        field_for_task_id=field,
    )
    assert component.driver.steps == TableDeclaration.of_driver(component.driver).steps


def test_the_steps_of_a_declaration_flatten_into_what_they_add() -> None:
    from taskiq_postgresql.core.sql.catalog import DeclaredStep  # noqa: PLC0415

    first = Column("a", "TEXT", nullable=True)
    second = Column("b", "TEXT", nullable=True)
    first_index = AddedIndex(name="notes_a_idx", columns=[first])
    second_index = AddedIndex(name="notes_b_idx", columns=[second])
    key = PrimaryKeyColumn(name="id", type_="SERIAL")

    declaration = TableDeclaration(
        table=_Driver(DSN, "notes", columns=[], primary_key=key).table,
        primary_key=key,
        columns=[],
        created_at=CreatedAtColumn(),
        index_columns=None,
        steps=[
            DeclaredStep(added_columns=[first], added_indexes=[first_index]),
            DeclaredStep(added_columns=[second], added_indexes=[second_index]),
        ],
    )

    assert declaration.added_columns == (first, second)
    assert declaration.added_indexes == (first_index, second_index)


def test_a_driver_built_with_added_columns_describes_them_as_one_step() -> None:
    from taskiq_postgresql.core.sql.catalog import DeclaredStep  # noqa: PLC0415

    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    added = Column("extra", "TEXT", nullable=True)
    index = AddedIndex(name="notes_extra_idx", columns=[added])
    driver = _Driver(
        DSN,
        "notes",
        columns=[Column("body", "TEXT")],
        primary_key=key,
        added_columns=[added],
        added_indexes=[index],
    )

    declaration = TableDeclaration.of_driver(driver)

    assert driver.steps == ()
    assert declaration.steps == (DeclaredStep([added], [index]),)
    assert declaration.added_columns == (added,)
    assert declaration.added_indexes == (index,)


def test_a_driver_that_sets_its_steps_keeps_them_as_they_are() -> None:
    from taskiq_postgresql.core.sql.catalog import DeclaredStep  # noqa: PLC0415

    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    added = Column("extra", "TEXT", nullable=True)
    steps = [DeclaredStep(added_columns=[added], nullable_columns=("body",))]
    driver = _Driver(
        DSN,
        "notes",
        columns=[Column("body", "TEXT")],
        primary_key=key,
        added_columns=[added],
    )
    driver.steps = steps

    assert TableDeclaration.of_driver(driver).steps is steps


def test_a_driver_with_nothing_added_declares_no_step() -> None:
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    driver = _Driver(DSN, "notes", columns=[Column("body", "TEXT")], primary_key=key)

    assert TableDeclaration.of_driver(driver).steps == ()
    described = declared_schema("result_backend", TableDeclaration.of_driver(driver))
    assert [step.number for step in described.steps] == [1]


def test_the_step_numbers_follow_the_order_of_the_list() -> None:
    from taskiq_postgresql.core.sql.catalog import DeclaredStep  # noqa: PLC0415

    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    first = Column("a", "TEXT", nullable=True)
    second = Column("b", "TEXT", nullable=True)
    driver = _Driver(DSN, "notes", columns=[Column("body", "TEXT")], primary_key=key)
    declaration = dataclasses.replace(
        TableDeclaration.of_driver(driver),
        steps=[
            DeclaredStep(added_columns=[first]),
            DeclaredStep(added_columns=[second], nullable_columns=("a", "body")),
        ],
    )

    schema = declared_schema("result_backend", declaration)

    assert [(step.number, step.objects.columns) for step in schema.steps] == [
        (1, ("id", "body", "created_at")),
        (2, ("a",)),
        (3, ("b",)),
    ]
    assert schema.steps[2].objects.nullable_columns == ("a", "body")
    # The column table_columns holds is NOT NULL when created and nullable in
    # the final description, once the step that relaxes it has run.
    assert {column.name: column.nullable for column in schema.columns} == {
        "id": False,
        "body": True,
        "created_at": False,
        "a": True,
        "b": True,
    }


def test_a_step_cannot_relax_a_column_the_table_does_not_have() -> None:
    from taskiq_postgresql.core.sql.catalog import DeclaredStep  # noqa: PLC0415

    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    driver = _Driver(DSN, "notes", columns=[Column("body", "TEXT")], primary_key=key)
    declaration = dataclasses.replace(
        TableDeclaration.of_driver(driver),
        steps=[DeclaredStep(nullable_columns=("nowhere",))],
    )

    with pytest.raises(ValueError, match="no such column"):
        declared_schema("result_backend", declaration)


def test_the_statements_of_a_step_are_the_script_run_migrations_executes() -> None:
    from taskiq_postgresql.abc.query import DeclaredStep as Reexported  # noqa: PLC0415
    from taskiq_postgresql.core.sql.catalog import (  # noqa: PLC0415
        DeclaredStep,
        declared_step_script,
        declared_step_statements,
    )

    driver = build_component("result_backend", table_name="jobs").driver
    (step,) = driver.steps
    # The third-party drivers that import the builders from abc.query keep them.
    assert Reexported is DeclaredStep

    statements = declared_step_statements("jobs", step)

    assert statements == tuple(
        statement.replace("taskiq_results", "jobs") for statement in RESULTS_STEP_2
    )
    assert declared_step_script("jobs", step) == "\n".join(
        f"{statement};" for statement in statements
    )
    # A step that declares nothing has no statement.
    assert declared_step_script("jobs", DeclaredStep()) == ""
