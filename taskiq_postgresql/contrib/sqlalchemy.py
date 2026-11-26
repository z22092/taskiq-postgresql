"""
The tables of taskiq-postgresql as SQLAlchemy ``Table`` objects.

Install it with ``pip install 'taskiq-postgresql[sqlalchemy]'``. The tables
come from the description in ``taskiq_postgresql.migrations``, the one
``run_migrations=True`` creates them from, so ``MetaData.create_all`` creates
the same columns, types, defaults, primary key and indexes, under the same
names, and Alembic's autogenerate finds nothing to change on a database that
``run_migrations=True`` set up.

The metadata also keeps the table descriptions themselves, under
``metadata.info[METADATA_INFO_KEY]``: that is where
``taskiq_postgresql.contrib.alembic`` finds the migration steps.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Optional

try:
    from sqlalchemy import (
        Column,
        DateTime,
        Index,
        Integer,
        MetaData,
        PrimaryKeyConstraint,
        String,
        Table,
        Text,
        Uuid,
        text,
    )
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable
except ImportError as error:
    raise ImportError(
        "sqlalchemy is not installed. Please install it with "
        "`pip install taskiq-postgresql[sqlalchemy]`.",
    ) from error

from taskiq_postgresql.core.sql.identifier import object_name, relation_name
from taskiq_postgresql.migrations import (
    ColumnSchema,
    TableSchema,
    TypeName,
    collect_schemas,
    keep_table_schema,
)

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine

__all__ = ["METADATA_INFO_KEY", "add_taskiq_tables", "taskiq_metadata"]

METADATA_INFO_KEY: Final = "taskiq_postgresql"
"""
The ``MetaData.info`` key of the table descriptions, a tuple of
``TableSchema``: Alembic documents ``info`` as the place where autogenerate
hooks find what they compare.
"""

_TYPES: Final[Mapping[TypeName, Callable[[ColumnSchema], TypeEngine[Any]]]] = {
    # SQLAlchemy writes SERIAL for an integer primary key that autoincrements.
    "serial": lambda _: Integer(),
    "uuid": lambda _: Uuid(),
    "varchar": lambda column: String(column.length),
    "text": lambda _: Text(),
    "bytea": lambda _: postgresql.BYTEA(),
    "jsonb": lambda _: postgresql.JSONB(),
    "timestamptz": lambda _: DateTime(timezone=True),
}


def add_taskiq_tables(
    metadata: MetaData,
    *targets: object,
    app_dir: Optional[str] = None,
) -> list[Table]:
    """
    Add the tables of taskiq-postgresql components to a SQLAlchemy metadata.

    Each table gets the columns, types, server defaults, primary key and
    indexes ``run_migrations=True`` creates, under the names PostgreSQL gives
    them, so ``metadata.create_all`` builds the same catalog. A table the
    metadata already holds under the same name is reused when SQLAlchemy would
    create it with the same DDL. The descriptions go to
    ``metadata.info[METADATA_INFO_KEY]``, each table once.

    Args:
        metadata: The metadata to add the tables to, for example
            ``Base.metadata`` of a declarative base.
        *targets: What ``collect_schemas`` takes (components, brokers,
            ``TaskiqScheduler`` objects, ``"module:object"`` paths), or
            ``TableSchema`` descriptions such as ``table_schema("broker")``,
            which need neither the application nor a DSN.
        app_dir: A directory to add to ``sys.path`` before importing a path.

    Returns:
        The tables, in target order, each once.

    Raises:
        TypeError: if no target is given.
        ValueError: if the metadata holds a different table under one of the
            names, a table name is not a plain SQL identifier, two targets
            spell one table name differently ('Jobs' and 'jobs') or describe
            it with different columns, or a target cannot be described (see
            ``collect_schemas``).
        LookupError: if a target holds no component of this package.
        ImportError: if a path's module cannot be imported.
        AttributeError: if a path's module has no such object.
    """
    tables: list[Table] = []
    described: dict[str, TableSchema] = {}
    for kept in metadata.info.get(METADATA_INFO_KEY, ()):
        keep_table_schema(described, kept)
    for schema in _schemas(targets, app_dir):
        keep_table_schema(described, schema)
        table = _add(metadata, schema)
        if all(table is not kept for kept in tables):
            tables.append(table)
    metadata.info[METADATA_INFO_KEY] = tuple(described.values())
    return tables


def taskiq_metadata(*targets: object, app_dir: Optional[str] = None) -> MetaData:
    """
    Return a new metadata that holds only the tables of the targets.

    Args:
        *targets: As for ``add_taskiq_tables``.
        app_dir: As for ``add_taskiq_tables``.

    Returns:
        The metadata.

    Raises:
        TypeError: as ``add_taskiq_tables`` does.
        ValueError: as ``add_taskiq_tables`` does.
        LookupError: as ``add_taskiq_tables`` does.
        ImportError: as ``add_taskiq_tables`` does.
        AttributeError: as ``add_taskiq_tables`` does.
    """
    metadata = MetaData()
    add_taskiq_tables(metadata, *targets, app_dir=app_dir)
    return metadata


def _schemas(targets: Sequence[object], app_dir: Optional[str]) -> list[TableSchema]:
    """Describe the targets' tables; shared with the Alembic integration."""
    if not targets:
        raise TypeError(
            "name at least one target: a component, a broker, a scheduler, a "
            "'module:object' path or a TableSchema",
        )
    return collect_schemas(*targets, app_dir=app_dir)


def _add(metadata: MetaData, schema: TableSchema) -> Table:
    # Built apart first: Table() refuses a name the metadata already holds.
    candidate = _table(schema, MetaData(schema=metadata.schema))
    existing = metadata.tables.get(candidate.key)
    if existing is None:
        return _table(schema, metadata)
    if _ddl(existing) != _ddl(candidate):
        raise ValueError(
            f"the metadata already holds a table {existing.name!r} that is not "
            "the taskiq-postgresql table of that name",
        )
    return existing


def _table(schema: TableSchema, metadata: MetaData) -> Table:
    # The name PostgreSQL keeps: SQLAlchemy quotes a name with a capital
    # letter, so it gets the folded one.
    name = schema.table.name
    return Table(
        name,
        metadata,
        *(_column(column) for column in schema.columns),
        # The name PostgreSQL gives the constraint run_migrations creates; an
        # explicit name also keeps the metadata's naming convention off it.
        PrimaryKeyConstraint(
            schema.primary_key.name,
            name=object_name(name, None, "pkey"),
        ),
        # A derived index name is up to 71 bytes, which PostgreSQL cuts to 63;
        # SQLAlchemy refuses the longer one.
        *(
            Index(
                relation_name(index.name), *index.columns, postgresql_using=index.method
            )
            for index in schema.indexes
        ),
    )


def _column(column: ColumnSchema) -> Column[Any]:
    return Column(
        column.name,
        _column_type(column),
        nullable=column.nullable,
        server_default=None if column.default is None else text(column.default),
        autoincrement=column.type_name == "serial",
    )


def _column_type(column: ColumnSchema) -> TypeEngine[Any]:
    # A function of its own for the return type: each factory of _TYPES gives
    # its own TypeEngine subclass, and Column() cannot pick one type parameter
    # from their union.
    make_type = _TYPES.get(column.type_name)
    if make_type is None:
        raise ValueError(
            f"column {column.name!r} has the type {column.type_name!r}, which "
            "taskiq_postgresql.contrib.sqlalchemy has no SQLAlchemy type for",
        )
    return make_type(column)


def _ddl(table: Table) -> tuple[str, ...]:
    dialect = postgresql.dialect()
    return (
        str(CreateTable(table).compile(dialect=dialect)),
        *sorted(
            str(CreateIndex(index).compile(dialect=dialect)) for index in table.indexes
        ),
    )
