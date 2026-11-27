"""The DDL the components run: tables, indexes, added columns and the probe."""

from typing import NamedTuple, Optional, Sequence, Union

from taskiq_postgresql.core.sql.catalog.base import QueryBase
from taskiq_postgresql.core.sql.catalog.columns import Column
from taskiq_postgresql.core.sql.identifier import (
    Identifier,
    as_identifier,
    check_table_names,
    derived_names,
)
from taskiq_postgresql.core.sql.statement import ParamType, Script, Statement


class CreateTableQuery(QueryBase):
    """Query to create a table."""

    def __init__(
        self,
        table_name: Union[str, Identifier],
        columns: Sequence[Column],
    ) -> None:
        """Initialize the query."""
        super().__init__(table_name)
        self.columns = columns

    def make_query(self) -> str:
        """Return the query as a string."""
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"({', '.join(column.make_query() for column in self.columns)})"
        )

    def script(self) -> Script:
        """Return the query as a script: it takes no parameters."""
        return Script(self.make_query())


def index_name(table_name: str, column: Column) -> str:
    """Return the name of the index ``CreateIndexQuery`` creates on ``column``."""
    return f"{table_name}_{column.name}_idx"


class CreateIndexQuery(QueryBase):
    """Query to create an index."""

    def make_query(self, columns: Sequence[Column]) -> str:
        """Return the query as a string."""
        return "\n".join(
            "CREATE INDEX IF NOT EXISTS "
            f"{index_name(self.table_name, column)} "
            f"ON {self.table_name} USING HASH ({column.name});"
            for column in columns
        )

    def script(self, columns: Sequence[Column]) -> Script:
        """Return the query as a script: it takes no parameters."""
        return Script(self.make_query(columns))


class AddedIndex(NamedTuple):
    """A btree index that ``AddColumnsQuery`` creates after adding its columns."""

    name: str
    columns: Sequence[Column]


def index_names(
    table_name: str,
    index_columns: Optional[Sequence[Column]],
    added_indexes: Sequence[AddedIndex],
) -> list[str]:
    """
    Return the names of a table's indexes, as the package writes them.

    First the hash index of each of ``index_columns`` (``CreateIndexQuery``),
    then each of ``added_indexes`` (``AddColumnsQuery``), with the case of
    ``table_name``.
    """
    return [
        *(index_name(table_name, column) for column in index_columns or ()),
        *(index.name for index in added_indexes),
    ]


def table_object_names(
    table: Identifier,
    *,
    columns: Sequence[Column],
    index_columns: Optional[Sequence[Column]],
    added_indexes: Sequence[AddedIndex],
) -> tuple[tuple[str, str], ...]:
    """
    Return the names PostgreSQL gives the objects of a declared table.

    This is the one place that reads a table's declaration (its columns and
    indexes) into the inputs of ``derived_names``; ``check_table_objects`` and
    the tests go through it.

    Args:
        table: The checked table name.
        columns: Every column ``CREATE TABLE`` declares.
        index_columns: The columns ``CreateIndexQuery`` indexes.
        added_indexes: The indexes ``AddColumnsQuery`` adds.

    Returns:
        ``(kind, name)`` pairs, as ``derived_names`` returns them.
    """
    return derived_names(
        table,
        index_names=index_names(table.configured, index_columns, added_indexes),
        serial_column=next(
            (column.name for column in columns if column.is_serial),
            None,
        ),
    )


def check_table_objects(
    table: Identifier,
    *,
    columns: Sequence[Column],
    index_columns: Optional[Sequence[Column]],
    added_indexes: Sequence[AddedIndex],
) -> None:
    """
    Refuse a table name whose derived names collide.

    A driver runs it when it is built and the migration description runs it
    again when it is built, from the same lists, so a table that is described
    but never run through a driver gets the same refusal.
    ``CREATE INDEX IF NOT EXISTS`` skips an index whose cut name already
    exists, so a collision would lose an index without an error.

    Args:
        table: The checked table name.
        columns: Every column ``CREATE TABLE`` declares.
        index_columns: The columns ``CreateIndexQuery`` indexes.
        added_indexes: The indexes ``AddColumnsQuery`` adds.

    Raises:
        IdentifierError: Two of the names the table's objects get are equal.
    """
    check_table_names(
        table,
        table_object_names(
            table,
            columns=columns,
            index_columns=index_columns,
            added_indexes=added_indexes,
        ),
    )


class AddColumnsQuery(QueryBase):
    """Query adding columns, then indexes, to a table that may already exist."""

    def make_query(
        self,
        columns: Sequence[Column],
        indexes: Sequence[AddedIndex] = (),
    ) -> str:
        """Return the query as a string: one statement each, without parameters."""
        statements = []
        if columns:
            add_columns = ",\n".join(
                f"  ADD COLUMN IF NOT EXISTS {column.make_query()}"
                for column in columns
            )
            statements.append(f"ALTER TABLE {self.table_name}\n{add_columns};")
        for index in indexes:
            indexed = ", ".join(column.name for column in index.columns)
            statements.append(
                f"CREATE INDEX IF NOT EXISTS {index.name}\n"
                f"  ON {self.table_name} ({indexed});",
            )
        return "\n".join(statements)

    def script(
        self,
        columns: Sequence[Column],
        indexes: Sequence[AddedIndex] = (),
    ) -> Script:
        """Return the statements as a script: they take no parameters."""
        return Script(self.make_query(columns, indexes))


class DropNotNullQuery(QueryBase):
    """Query making an existing column accept ``NULL``; a no-op when it already does."""

    def make_query(self, column: str) -> str:
        """Return the query as a string: one statement, without parameters."""
        return f"ALTER TABLE {self.table_name} ALTER COLUMN {column} DROP NOT NULL;"

    def script(self, column: str) -> Script:
        """Return the statement as a script: it takes no parameters."""
        return Script(self.make_query(column))


class DeclaredStep(NamedTuple):
    """
    One migration step after ``CREATE TABLE``, as a component declares it.

    The steps of a table run in the order they are declared, as steps 2, 3 and
    so on (step 1 is ``CREATE TABLE`` and its hash indexes), and a released
    step never changes: a later release declares one more.

    Attributes:
        added_columns: Columns the step adds with ``ADD COLUMN IF NOT EXISTS``.
        added_indexes: Indexes the step creates, after its columns.
        nullable_columns: Names of columns the step makes accept ``NULL``
            (``DropNotNullQuery``), after it added what it adds.
    """

    added_columns: Sequence[Column] = ()
    added_indexes: Sequence[AddedIndex] = ()
    nullable_columns: Sequence[str] = ()


def declared_step_statements(
    table: Union[str, Identifier],
    step: DeclaredStep,
) -> tuple[str, ...]:
    """
    Return the statements of a step, one by one, without the trailing ``;``.

    The one place that turns a declared step into SQL: ``run_migrations=True``
    runs them (``declared_step_script``) and the migration description holds
    them, so the two cannot differ.

    Args:
        table: The table, as configured or already checked.
        step: The step.

    Returns:
        The ``ALTER TABLE ... ADD COLUMN`` statement when the step adds
        columns, then one ``CREATE INDEX`` for each added index, then one
        ``ALTER COLUMN ... DROP NOT NULL`` for each nullable column.
    """
    add = AddColumnsQuery(table)
    drop = DropNotNullQuery(table)
    statements = []
    if step.added_columns:
        statements.append(add.make_query(step.added_columns).removesuffix(";"))
    statements.extend(
        add.make_query((), [index]).removesuffix(";") for index in step.added_indexes
    )
    statements.extend(
        drop.make_query(column).removesuffix(";") for column in step.nullable_columns
    )
    return tuple(statements)


def declared_step_script(table: Union[str, Identifier], step: DeclaredStep) -> str:
    """
    Return the statements of a step as one script, each ending with ``;``.

    Args:
        table: The table, as configured or already checked.
        step: The step.

    Returns:
        The statements of ``declared_step_statements``, one per line, to run in
        one call without parameters.
    """
    return "\n".join(
        f"{statement};" for statement in declared_step_statements(table, step)
    )


class RelationsExistQuery(QueryBase):
    """
    Query telling whether a table and every index it gets all exist.

    Given the columns and indexes ``AddColumnsQuery`` adds, it also tells
    whether the table has them. A query driver no component owns
    (``QueryDriver.component_kind`` is ``None``) cannot describe its table for
    ``catalog_query``, so with ``run_migrations`` it runs this probe instead,
    as every driver did before that comparison, and skips the migration lock
    and the DDL when it answers true.
    """

    def relation_names(
        self,
        index_columns: Optional[Sequence[Column]],
        *,
        added_indexes: Sequence[AddedIndex] = (),
    ) -> list[str]:
        """Return the table name, then the name of each index on it."""
        return [
            self.table_name,
            *index_names(self.table_name, index_columns, added_indexes),
        ]

    def params(
        self,
        index_columns: Optional[Sequence[Column]],
        *,
        added_columns: Sequence[Column] = (),
        added_indexes: Sequence[AddedIndex] = (),
    ) -> list[str]:
        """Return the arguments of the query: relation names, then column names."""
        return [
            *self.relation_names(index_columns, added_indexes=added_indexes),
            *(column.name for column in added_columns),
        ]

    def make_query(
        self,
        index_columns: Optional[Sequence[Column]],
        *,
        added_columns: Sequence[Column] = (),
        added_indexes: Sequence[AddedIndex] = (),
    ) -> str:
        """Return the query as a string, one parameter per item of ``params``."""
        relation_count = len(
            self.relation_names(index_columns, added_indexes=added_indexes),
        )
        checks = [
            f"to_regclass(${position}) IS NOT NULL"
            for position in range(1, relation_count + 1)
        ]
        checks.extend(
            "EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1)"  # noqa: S608
            f" AND attname::text = ${position} AND NOT attisdropped)"
            for position in range(
                relation_count + 1,
                relation_count + 1 + len(added_columns),
            )
        )
        return "SELECT " + " AND ".join(checks)

    def statement(
        self,
        index_columns: Optional[Sequence[Column]],
        *,
        added_columns: Sequence[Column] = (),
        added_indexes: Sequence[AddedIndex] = (),
    ) -> Statement:
        """Return the query typed: one text parameter per item of ``params``."""
        params = self.params(
            index_columns,
            added_columns=added_columns,
            added_indexes=added_indexes,
        )
        return Statement(
            self.make_query(
                index_columns,
                added_columns=added_columns,
                added_indexes=added_indexes,
            ),
            (ParamType.TEXT,) * len(params),
            (("?column?", ParamType.BOOL),),
        )


def drop_table_script(table: Union[str, Identifier]) -> Script:
    """Return the script that drops ``table`` if it exists."""
    table_name = as_identifier(table, what="table name").configured
    return Script(f"DROP TABLE IF EXISTS {table_name}")
