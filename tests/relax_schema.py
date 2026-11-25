"""
Test-only tables whose steps relax a ``NOT NULL`` column.

No released step does that yet, so the tests that need one describe their own
table. ``relaxing_schema``: step 1 creates it with ``note TEXT NOT NULL``,
step 2 runs ``ALTER COLUMN note DROP NOT NULL``, and the final description has
``note`` nullable. ``late_relaxing_schema``: step 1 creates the table without
``note``, step 2 adds it ``NOT NULL`` and step 3 relaxes it.
"""

from __future__ import annotations

from taskiq_postgresql.abc.query import DropNotNullQuery
from taskiq_postgresql.migrations import (
    ColumnSchema,
    MigrationStep,
    StepObjects,
    TableSchema,
)

TABLE = "relax_demo"
RELAXED = "note"
# What format_type() reports for each column of the table.
CATALOG_COLUMNS = {"id": "integer", "note": "text"}

_ID = ColumnSchema("id", "SERIAL", "serial", None, False, None, True)
_NOTE = ColumnSchema("note", "TEXT", "text", None, True, None, False)


def _relax(table_name: str, number: int) -> MigrationStep:
    return MigrationStep(
        number=number,
        statements=(
            DropNotNullQuery(table_name).make_query(RELAXED).removesuffix(";"),
        ),
        objects=StepObjects(
            creates_table=False,
            columns=(),
            indexes=(),
            nullable_columns=(RELAXED,),
        ),
    )


def relaxing_schema(table_name: str = TABLE) -> TableSchema:
    """
    Describe a table with a second step that makes ``note`` nullable.

    Args:
        table_name: The table name.

    Returns:
        The description, of the ``result_backend`` kind.
    """
    return TableSchema(
        kind="result_backend",
        name=table_name,
        columns=(_ID, _NOTE),
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
            _relax(table_name, 2),
        ),
    )


def late_relaxing_schema(table_name: str = TABLE) -> TableSchema:
    """
    Describe a table whose third step relaxes the column its second step adds.

    Args:
        table_name: The table name.

    Returns:
        The description, of the ``result_backend`` kind.
    """
    return TableSchema(
        kind="result_backend",
        name=table_name,
        columns=(_ID, _NOTE),
        indexes=(),
        steps=(
            MigrationStep(
                number=1,
                statements=(
                    (
                        f"CREATE TABLE IF NOT EXISTS {table_name} "
                        "(id SERIAL NOT NULL PRIMARY KEY)"
                    ),
                ),
                objects=StepObjects(creates_table=True, columns=("id",), indexes=()),
            ),
            MigrationStep(
                number=2,
                statements=(
                    (
                        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
                        "note TEXT NOT NULL DEFAULT ''"
                    ),
                ),
                objects=StepObjects(
                    creates_table=False,
                    columns=("note",),
                    indexes=(),
                ),
            ),
            _relax(table_name, 3),
        ),
    )
