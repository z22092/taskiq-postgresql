"""
A test-only table whose second step relaxes a ``NOT NULL`` column.

No released step does that yet, so the tests that need one describe their own
table: step 1 creates it with ``note TEXT NOT NULL``, step 2 runs
``ALTER COLUMN note DROP NOT NULL``, and the final description has ``note``
nullable. The names this change adds are imported inside the function, so
that on a tree without it each test that calls it fails on its own and the
rest of the suite still collects and runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskiq_postgresql.migrations import TableSchema

TABLE = "relax_demo"
RELAXED = "note"
# What format_type() reports for each column of the table.
CATALOG_COLUMNS = {"id": "integer", "note": "text"}


def relaxing_schema(table_name: str = TABLE) -> TableSchema:
    """
    Describe a table with a second step that makes ``note`` nullable.

    Args:
        table_name: The table name.

    Returns:
        The description, of the ``result_backend`` kind.
    """
    from taskiq_postgresql.abc.query import DropNotNullQuery  # noqa: PLC0415
    from taskiq_postgresql.migrations import (  # noqa: PLC0415
        ColumnSchema,
        MigrationStep,
        StepObjects,
        TableSchema,
    )

    return TableSchema(
        kind="result_backend",
        name=table_name,
        columns=(
            ColumnSchema("id", "SERIAL", "serial", None, False, None, True),
            ColumnSchema("note", "TEXT", "text", None, True, None, False),
        ),
        indexes=(),
        steps=(
            MigrationStep(
                number=1,
                statements=(
                    (
                        f"CREATE TABLE IF NOT EXISTS {table_name} "
                        "(id SERIAL NOT NULL PRIMARY KEY, note TEXT NOT NULL)"
                    ),
                ),
                objects=StepObjects(
                    creates_table=True,
                    columns=("id", "note"),
                    indexes=(),
                ),
            ),
            MigrationStep(
                number=2,
                statements=(
                    DropNotNullQuery(table_name).make_query(RELAXED).removesuffix(";"),
                ),
                objects=StepObjects(
                    creates_table=False,
                    columns=(),
                    indexes=(),
                    nullable_columns=(RELAXED,),
                ),
            ),
        ),
    )
