"""
Describe the tables this package creates, and render the SQL that creates them.

``run_migrations=True``, ``taskiq postgresql generate`` and integrations with
other migration tools share this description. It is built from the columns
the components' drivers use, so the SQL a tool runs is the SQL a worker runs.

A table is described twice over: structurally (columns, types, indexes), for
tools that model tables themselves, and as an ordered list of migration steps
holding the exact SQL statements, for tools that run SQL. Neither needs the
other parsed.

Each step also declares the objects it guarantees, so a database can be
compared with the description: ``catalog_query`` reads what exists, in one
read-only query (``read_catalog`` runs it through a driver), and
``plan_migration`` tells which steps are still pending and what is wrong in a
way no step fixes.

Importing this module imports no driver library.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cached_property
from importlib import import_module
from importlib.metadata import PackageNotFoundError, entry_points, version
from inspect import isfunction, signature
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    Optional,
    TypeVar,
    Union,
    get_args,
)

from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.core.sql.catalog import (
    AddColumnsQuery,
    AddedIndex,
    CatalogTable,
    ClaimColumns,
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    PrimaryKeyColumn,
    catalog_read,
    check_lock_timeout,
    check_table_objects,
    in_transaction,
    index_name,
    index_names,
    lock_timeout_scripts,
    migration_lock_script,
)
from taskiq_postgresql.core.sql.identifier import Identifier, as_identifier, fold
from taskiq_postgresql.core.stores.declaration import TableDeclaration

if TYPE_CHECKING:
    from taskiq_postgresql.broker import PostgresqlBroker
    from taskiq_postgresql.result_backend import PostgresqlResultBackend
    from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

    Component = Union[
        PostgresqlBroker,
        PostgresqlResultBackend[Any],
        PostgresqlSchedulerSource,
    ]

__all__ = [
    "COMPONENT_KINDS",
    "DEFAULT_LOCK_TIMEOUT",
    "MIGRATION_FORMATS_GROUP",
    "SYSTEM_PATH",
    "TASK_ID_TYPES",
    "AmbiguousMigrationFormatError",
    "CatalogState",
    "ColumnSchema",
    "ComponentKind",
    "FormatOptions",
    "IndexMethod",
    "IndexSchema",
    "IndexState",
    "MigrationFormat",
    "MigrationFormatOptionError",
    "MigrationPlan",
    "MigrationStep",
    "PendingStep",
    "ProblemCode",
    "RenderedStep",
    "SchemaProblem",
    "Severity",
    "StepObjects",
    "TableSchema",
    "TableState",
    "TaskIdType",
    "TypeName",
    "UnknownMigrationFormatError",
    "available_formats",
    "catalog_query",
    "catalog_state",
    "catalog_tables",
    "check_comment",
    "check_lock_timeout",
    "check_plain_identifier",
    "collect_components",
    "collect_schemas",
    "keep_table_schema",
    "load_format",
    "migration_statements",
    "package_version",
    "parse_target",
    "plan_migration",
    "read_catalog",
    "render_sql",
    "render_steps",
    "scoped_lock_timeout",
    "sql_format",
    "step_statements",
    "table_schema",
]

ComponentKind = Literal["broker", "result_backend", "scheduler_source"]
"""The three components that own a table."""


class _SystemPath:
    """The type of ``SYSTEM_PATH``."""

    def __repr__(self) -> str:
        return "SYSTEM_PATH"


SYSTEM_PATH: Final = _SystemPath()
"""
Pass as ``app_dir`` to import a path with the interpreter's own ``sys.path``.

Nothing is added to it, not even the working directory, which the default
puts first while a module is imported. A caller whose process has already set
its import path up, such as a Django project, passes it so that a module of
the same name in whatever directory the command runs from is not loaded.
"""

COMPONENT_KINDS: Final[tuple[ComponentKind, ...]] = get_args(ComponentKind)

TaskIdType = Literal["VarChar", "Text", "Uuid"]
"""The values ``field_for_task_id`` accepts."""

TASK_ID_TYPES: Final[tuple[TaskIdType, ...]] = get_args(TaskIdType)

TypeName = Literal["serial", "uuid", "varchar", "text", "bytea", "jsonb", "timestamptz"]
"""A column type, named once whatever spelling the SQL uses."""

IndexMethod = Literal["hash", "btree"]
"""The access method of an index."""

Severity = Literal["required", "advisory"]
"""
How much a gap matters: without a ``required`` object (a table or a column,
or a column that accepts ``NULL``) the components fail; without an
``advisory`` one (an index) they only slow down.
"""

ProblemCode = Literal[
    "name_taken",
    "missing_column",
    "column_type",
    "invalid_index",
    "index_on_other_table",
    "index_shape",
]
"""What is wrong with an object in a way no migration step fixes."""

MIGRATION_FORMATS_GROUP: Final = "taskiq_postgresql.migration_formats"
"""The entry-point group that ``taskiq postgresql generate --format`` reads."""

DEFAULT_LOCK_TIMEOUT: Final = 10
"""Seconds a script's statement waits for a lock before the script fails."""

_DISTRIBUTION: Final = "taskiq-postgresql"

# Keyed by the upper-cased SQL spelling; PostgreSQL type names ignore case.
_TYPE_NAMES: Final[Mapping[str, TypeName]] = {
    "SERIAL": "serial",
    "UUID": "uuid",
    "VARCHAR": "varchar",
    "TEXT": "text",
    "BYTEA": "bytea",
    "JSONB": "jsonb",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "TIMESTAMPTZ": "timestamptz",
}
_SIZED_TYPE: Final = re.compile(r"(?P<name>VARCHAR)\((?P<length>[1-9][0-9]*)\)")
# PEP 263: on one of the first two lines of a Python file, a comment that
# matches this declares the encoding Python decodes the whole file with.
# Matched without regard to case, wider than Python's own pattern.
_ENCODING_DECLARATION: Final = re.compile(r"coding[:=]\s*[-\w.]+", re.IGNORECASE)
_T = TypeVar("_T")


def check_plain_identifier(name: str) -> str:
    """
    Refuse a table or index name that a migration file cannot write unquoted.

    The SQL names tables and indexes without quotes, so a name made of other
    characters could name two tables, or end a statement. The rule is the one
    components apply to ``table_name`` (ADR 0008), except that mixed case is
    neither folded nor warned about here.

    Args:
        name: The name.

    Returns:
        ``name``, unchanged.

    Raises:
        ValueError: if ``name`` is not letters, digits, ``_`` and ``$``,
            starting with a letter or ``_``, is a reserved word or is over 63
            bytes.
    """
    Identifier.parse(name, what="name", warn=False)
    return name


def _check_nullable_name(number: int, table: str, name: str) -> None:
    """Refuse a nullable column name a generated file could not hold."""
    try:
        check_plain_identifier(name)
    except ValueError as error:
        raise ValueError(
            f"step {number} of {table!r} makes {name!r} nullable: {error}",
        ) from None


def check_comment(text: str) -> str:
    """
    Refuse text that a comment line of a migration file cannot hold.

    A line break would end the comment and leave the rest of it as code, a
    NUL reaches neither PostgreSQL nor Python, and an encoding declaration
    (PEP 263, such as ``coding: utf-7``) on one of the first two lines of a
    Python file changes how Python reads the whole file.

    Args:
        text: The comment, without its comment marker.

    Returns:
        ``text``, unchanged.

    Raises:
        ValueError: if ``text`` holds a line break, a NUL or an encoding
            declaration.
    """
    if any(character in text for character in "\0\r\n"):
        raise ValueError(
            f"comment {text!r} cannot be written in a migration file: it holds "
            "a line break or a NUL",
        )
    declaration = _ENCODING_DECLARATION.search(text)
    if declaration is not None:
        raise ValueError(
            f"comment {text!r} cannot be written in a migration file: "
            f"{declaration[0]!r} is an encoding declaration, which Python reads "
            "on the first two lines of a file",
        )
    return text


def parse_target(spec: str) -> tuple[str, str]:
    """
    Split a ``"module:object"`` path, without importing anything.

    Args:
        spec: The path, such as ``"app:broker"``.

    Returns:
        The module name and the object name.

    Raises:
        ValueError: if ``spec`` is not ``"module:object"``.
    """
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute or ":" in attribute:
        raise ValueError(f"{spec!r} is not a 'module:object' path")
    return module_name, attribute


def package_version() -> str:
    """
    Return the installed version of taskiq-postgresql.

    Returns:
        The version, or ``unknown`` in a source tree that was never installed,
        which has no package metadata.
    """
    try:
        return version(_DISTRIBUTION)
    except PackageNotFoundError:
        return "unknown"


@dataclass(frozen=True)
class ColumnSchema:
    """
    One column of a table, as the package creates it.

    Attributes:
        name: Column name.
        sql_type: The type exactly as ``CREATE TABLE`` writes it, for example
            ``VARCHAR(100)`` or ``Uuid``.
        type_name: The type, named once whatever its spelling: what an
            integration maps to its own type.
        length: The length of a sized type (``VARCHAR(100)`` has 100), or
            ``None``.
        nullable: Whether the column accepts ``NULL``.
        default: The server default as SQL, for example ``NOW()``, or ``None``.
        primary_key: Whether the column is the primary key.
    """

    name: str
    sql_type: str
    type_name: TypeName
    length: Optional[int]
    nullable: bool
    default: Optional[str]
    primary_key: bool


@dataclass(frozen=True)
class IndexSchema:
    """
    One index of a table, besides the primary key's own.

    Attributes:
        name: Index name.
        columns: The indexed columns, in index order.
        method: The access method.
    """

    name: str
    columns: tuple[str, ...]
    method: IndexMethod


@dataclass(frozen=True)
class StepObjects:
    """
    The objects a migration step guarantees once it has run.

    Attributes:
        creates_table: Whether the step creates the table. Such a step cannot
            add a column to a table that already exists.
        columns: The columns the step creates or adds, by name.
        indexes: The indexes the step creates.
        nullable_columns: The columns the step makes nullable
            (``ALTER COLUMN ... DROP NOT NULL``). Each is a column of the
            table's description that is nullable there, and a step with a
            lower number creates or adds it; ``TableSchema`` checks both.
    """

    creates_table: bool
    columns: tuple[str, ...]
    indexes: tuple[IndexSchema, ...]
    nullable_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class MigrationStep:
    """
    One idempotent step of a table's migration: it adds, or relaxes ``NOT NULL``.

    Steps run in ``number`` order and each one is idempotent, so the whole
    list brings a table of any earlier release up to date. A step adds tables,
    columns and indexes, or makes a column accept ``NULL``; it never removes
    or renames anything. Idempotent is not lock-free: on a table that already
    exists, ``ALTER TABLE`` and ``CREATE INDEX`` lock it before they find that
    nothing is missing. A released step never changes: a later release adds a
    step instead.

    Attributes:
        number: Position of the step, from 1.
        statements: The SQL statements, without the trailing semicolon, in
            the order they run. They are the statements ``run_migrations=True``
            executes, byte for byte.
        objects: What exists once the statements have run.
    """

    number: int
    statements: tuple[str, ...]
    objects: StepObjects


@dataclass(frozen=True)
class TableSchema:
    """
    A table this package creates: its final shape and the steps that build it.

    Attributes:
        kind: The component that owns the table.
        name: Table name, as configured on the component.
        columns: Every column, in table order, after all steps.
        indexes: Every index after all steps, except the primary key's own
            index, which ``CREATE TABLE`` creates and PostgreSQL names
            (``taskiq_messages_pkey`` for ``taskiq_messages``).
        steps: The migration steps, in the order they run.

    Raises:
        ValueError: if a step's ``nullable_columns`` names a column that is not
            in ``columns``, one that is not nullable there, or one that no
            step with a lower number creates or adds: the final description
            would contradict the step, or the statement would fail on a table
            built from scratch.
    """

    kind: ComponentKind
    name: str
    columns: tuple[ColumnSchema, ...]
    indexes: tuple[IndexSchema, ...]
    steps: tuple[MigrationStep, ...]

    def __post_init__(self) -> None:
        # A step needs the final description to be checked against, so the
        # check lives here and not in MigrationStep.
        final = {column.name: column for column in self.columns}
        added_at: dict[str, int] = {}
        for step in self.steps:
            for name in step.objects.columns:
                added_at.setdefault(name, step.number)
        for step in self.steps:
            for name in step.objects.nullable_columns:
                # The name is written into a docstring and a comment line of
                # a generated file, and the class is public.
                _check_nullable_name(step.number, self.name, name)
                column = final.get(name)
                if column is None:
                    raise ValueError(
                        f"step {step.number} of {self.name!r} makes {name!r} "
                        "nullable, and the table has no such column",
                    )
                if not column.nullable:
                    raise ValueError(
                        f"step {step.number} of {self.name!r} makes {name!r} "
                        "nullable, but the final description has it NOT NULL",
                    )
                if added_at.get(name, step.number) >= step.number:
                    raise ValueError(
                        f"step {step.number} of {self.name!r} makes {name!r} "
                        "nullable, and no earlier step adds it",
                    )

    @cached_property
    def table(self) -> Identifier:
        """
        Return the table name as a checked ``Identifier``.

        Not checked when the description is built, because the class is
        public; checked, without a warning, the first time a tool reads it
        here. A name the rule refuses is not kept: it raises again.

        Raises:
            IdentifierError: if the name is not one the rule accepts.
        """
        return Identifier.parse(self.name, what="table name", warn=False)

    @property
    def primary_key(self) -> ColumnSchema:
        """
        Return the primary-key column.

        Raises:
            LookupError: if no column is the primary key.
        """
        for column in self.columns:
            if column.primary_key:
                return column
        raise LookupError(f"table {self.name!r} has no primary key")


def table_schema(
    component: Union[Component, QueryDriver, ComponentKind],
    *,
    table_name: Optional[str] = None,
    field_for_task_id: Optional[TaskIdType] = None,
) -> TableSchema:
    """
    Describe the table of a component, of its query driver, or of a kind.

    Given a component, the description comes from the columns its driver
    uses. Given that query driver, it comes from the driver's declaration,
    under the ``component_kind`` the component set on it, so a driver can
    describe its own table. Given a kind, it needs no instance, no DSN and no
    driver: the component's defaults apply unless ``table_name`` or
    ``field_for_task_id`` say otherwise.

    Args:
        component: A ``PostgresqlBroker``, ``PostgresqlResultBackend`` or
            ``PostgresqlSchedulerSource``, the ``QueryDriver`` of one, or one
            of ``COMPONENT_KINDS``.
        table_name: With a kind, the table name; the component's default
            otherwise.
        field_for_task_id: With the ``broker`` or ``result_backend`` kind, the
            type of the ``task_id`` column; ``"Uuid"`` by default.

    Returns:
        The table's description.

    Raises:
        TypeError: if ``component`` is neither a component, a query driver
            nor a kind, if a query driver has no ``component_kind``, or if
            ``table_name`` or ``field_for_task_id`` come with a component or
            a driver.
        ValueError: if the kind is unknown, ``field_for_task_id`` is not one
            of ``TASK_ID_TYPES`` or is given for the scheduler source, the
            table name cannot be written in a script, or a column type has no
            ``TypeName``.
    """
    if isinstance(component, str):
        return _schema_of_kind(component, table_name, field_for_task_id)
    if table_name is not None or field_for_task_id is not None:
        raise TypeError(
            "table_name and field_for_task_id describe a component kind; "
            "a component already has its own",
        )
    if isinstance(component, QueryDriver):
        if component.component_kind is None:
            raise TypeError(
                "this query driver has no component_kind, so it cannot tell "
                "which table it holds: describe the component that owns it",
            )
        return declared_schema(
            component.component_kind,
            TableDeclaration.of_driver(component),
        )
    # The kind first, so that what is not a component is refused here.
    kind = _kind_of(component)
    from taskiq_postgresql.core.stores.schema import schema_store  # noqa: PLC0415

    return declared_schema(kind, schema_store(component).declaration)


def collect_components(
    *targets: object,
    app_dir: Union[str, _SystemPath, None] = None,
) -> list[Component]:
    """
    Find the components behind brokers, schedulers and import paths.

    A broker brings itself when it is a ``PostgresqlBroker`` and its result
    backend when that is a ``PostgresqlResultBackend``; a ``TaskiqScheduler``
    brings its broker the same way, then its ``PostgresqlSchedulerSource``
    sources. A component that shares a table with an earlier one is left out.
    Nothing is started.

    Args:
        *targets: Components, brokers, schedulers, or ``"module:object"``
            paths that name one, resolved like ``taskiq worker`` resolves its
            broker: the object is imported and, if it is a function, called.
        app_dir: A directory to add to ``sys.path`` before importing a path,
            or ``SYSTEM_PATH`` to add nothing. Left out, the working
            directory is on ``sys.path`` while a path is imported.

    Returns:
        The components, in target order.

    Raises:
        ValueError: if a path is not ``"module:object"``, or two components
            use one table name with different columns.
        LookupError: if a target holds no component of this package.
        ImportError: if a path's module cannot be imported.
        AttributeError: if a path's module has no such object.
    """
    return _one_per_table(
        (component, table_schema(component))
        for target in targets
        for component in _components_of(target, app_dir)
    )


def collect_schemas(
    *targets: object,
    app_dir: Union[str, _SystemPath, None] = None,
) -> list[TableSchema]:
    """
    Describe the tables behind brokers, schedulers, import paths and descriptions.

    Args:
        *targets: As for ``collect_components``, or ``TableSchema``
            descriptions such as ``table_schema("broker")``, which are kept as
            they are and need neither the application nor a DSN.
        app_dir: As for ``collect_components``.

    Returns:
        One description per table, in target order: a table that two targets
        describe with the same columns comes once.

    Raises:
        ValueError: as ``collect_components`` does, also when a description
            and a component describe one table with different columns.
        LookupError: as ``collect_components`` does.
        ImportError: as ``collect_components`` does.
        AttributeError: as ``collect_components`` does.
    """
    return _one_per_table(
        (schema, schema)
        for target in targets
        for schema in (
            [target]
            if isinstance(target, TableSchema)
            else map(table_schema, _components_of(target, app_dir))
        )
    )


def render_sql(
    schemas: Iterable[TableSchema],
    *,
    transaction: bool = True,
    lock: bool = True,
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
) -> str:
    """
    Render the SQL script that creates the tables, or completes them.

    The script runs every step of every table, with ``IF NOT EXISTS``, so it
    is idempotent: it completes a database an earlier release migrated, and a
    second run changes nothing. It still locks the tables when nothing is
    missing, because ``ALTER TABLE`` and ``CREATE INDEX`` lock a table before
    they find that the column or index exists. On a live database
    ``ALTER TABLE`` waits for every open transaction that used the table,
    and every later query on the table waits behind it; ``lock_timeout``
    bounds that wait. The output is the same for the same tables, options and
    package version.

    Args:
        schemas: The tables, in the order their statements run.
        transaction: Wrap the script in ``BEGIN`` and ``COMMIT``. Pass
            ``False`` to a tool that runs each migration in its own
            transaction, such as Alembic, Django's ``RunSQL`` or Flyway.
        lock: Take, per table, the advisory lock ``run_migrations=True``
            takes, so the script and starting workers create the tables one
            after the other. The lock lasts until the transaction ends.
        lock_timeout: Seconds a statement waits for a lock, the advisory lock
            included, before it fails and the transaction rolls back. The
            first statement sets it with ``SET LOCAL lock_timeout``, which
            lasts until the transaction ends (the caller's transaction with
            ``transaction=False``). ``0`` leaves the statement out, so the
            session's own ``lock_timeout`` applies.

    Returns:
        The script: a header comment, then each statement, starting on a new
        line and ending with a semicolon. A statement may span several lines.

    Raises:
        ValueError: if ``schemas`` is empty, a table name cannot be written
            in a script, or ``lock_timeout`` is not a whole number of seconds
            from 0 to 2147483.
    """
    tables = list(schemas)
    return _script_text(
        [f"-- tables: {', '.join(table.name for table in tables)}"],
        _lock_timeout_first(migration_statements(tables, lock=lock), lock_timeout),
        transaction=transaction,
    )


def migration_statements(
    schemas: Iterable[TableSchema],
    *,
    lock: bool = True,
    through: Optional[int] = None,
) -> list[str]:
    """
    Return the statements of the script, one by one, for a tool that runs one.

    A driver that prepares every statement it sends, such as asyncpg, refuses
    a script of several statements in one call. These are the statements of
    ``render_sql(schemas, transaction=False, lock=lock, lock_timeout=0)``, in
    the same order, without the header comment and the semicolons.

    Args:
        schemas: The tables, in the order their statements run.
        lock: Start with the advisory locks, as ``render_sql`` does.
        through: Stop after the step of this number, so the statements stay
            the same when a later release adds steps. Every table must have
            that step: a step it does not have yet would be picked up from
            the release that adds it.

    Returns:
        The statements, in the order they run.

    Raises:
        ValueError: if ``schemas`` is empty, a table name cannot be written in
            a script, or a table has no step ``through``.
    """
    tables = _checked_tables(schemas)
    return _script_statements(
        [(identifier, _steps_through(table, through)) for table, identifier in tables],
        lock=lock,
    )


def step_statements(
    schemas: Iterable[TableSchema],
    *,
    step: Optional[int] = None,
    through: Optional[int] = None,
    lock: bool = True,
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
) -> list[str]:
    """
    Return the statements that run steps in a transaction other statements share.

    For a tool that runs the steps inside a transaction of its own, such as an
    Alembic revision or a Django migration: the statements of
    ``migration_statements``, with their lock waits bounded by
    ``scoped_lock_timeout``, so the statements after them wait for locks as
    before.

    Args:
        schemas: The tables, in the order their statements run.
        step: Only this step, for each table that has it; a table without it
            adds nothing.
        through: Steps 1 to this one, as for ``migration_statements``.
        lock: Start with the advisory locks, as ``render_sql`` does.
        lock_timeout: Seconds a statement waits for a lock, the advisory lock
            included; ``0`` bounds nothing.

    Returns:
        The statements, in the order they run; none when ``step`` is given and
        no table has it.

    Raises:
        TypeError: if both ``step`` and ``through`` are given.
        ValueError: if ``lock_timeout`` is not a whole number of seconds from
            0 to 2147483, ``schemas`` is empty, a table name cannot be written
            in a script, or a table has no step ``through``.
    """
    if step is not None and through is not None:
        raise TypeError("give step or through, not both")
    check_lock_timeout(lock_timeout)
    tables = _checked_tables(schemas)
    if step is None:
        selected = [
            (identifier, _steps_through(table, through)) for table, identifier in tables
        ]
    else:
        selected = [
            (identifier, (found,))
            for table, identifier in tables
            for found in table.steps
            if found.number == step
        ]
        if not selected:
            return []
    return scoped_lock_timeout(_script_statements(selected, lock=lock), lock_timeout)


def scoped_lock_timeout(statements: Sequence[str], lock_timeout: int) -> list[str]:
    """
    Bound the lock waits of statements that share a transaction with others.

    The statements run with ``lock_timeout``; those after them, such as the
    rest of an Alembic revision, get back the value the transaction had.
    ``SET LOCAL lock_timeout TO DEFAULT`` would not do that: ``DEFAULT`` is
    "whatever value it would have had if no SET had been executed in the
    current session" (https://www.postgresql.org/docs/current/sql-set.html),
    so a value the session or the transaction set would be lost.

    Args:
        statements: The statements to bound.
        lock_timeout: Seconds each of them waits for a lock; ``0`` returns
            them as they are.

    Returns:
        A statement that keeps the transaction's ``lock_timeout``, ``SET LOCAL
        lock_timeout``, the statements, and one that gives the kept value
        back.

    Raises:
        ValueError: if ``lock_timeout`` is not a whole number of seconds from
            0 to 2147483.
    """
    check_lock_timeout(lock_timeout)
    if not lock_timeout:
        return list(statements)
    scripts = lock_timeout_scripts(lock_timeout)
    return [
        scripts.save.text,
        scripts.set_local.text,
        *statements,
        scripts.restore.text,
    ]


# --- comparing a database with the description -------------------------------


@dataclass(frozen=True)
class IndexState:
    """
    What the catalog holds under the name of one of a table's indexes.

    Attributes:
        name: The index name, as the description writes it.
        relkind: ``pg_class.relkind`` of the relation that has the name (``i``
            for an index), or ``None`` if no relation has it.
        on_table: Whether the relation is an index of the described table.
        valid: ``pg_index.indisvalid``: ``False`` after a failed
            ``CREATE INDEX CONCURRENTLY``, an index queries never use.
        method: The access method, such as ``hash`` or ``btree``.
        columns: The indexed columns, in index order; ``None`` stands for an
            expression.
        unique: Whether the index is unique.
        partial: Whether the index has a ``WHERE`` clause.
    """

    name: str
    relkind: Optional[str] = None
    on_table: bool = False
    valid: bool = False
    method: Optional[str] = None
    columns: tuple[Optional[str], ...] = ()
    unique: bool = False
    partial: bool = False

    @property
    def exists(self) -> bool:
        """Whether a relation has the name."""
        return self.relkind is not None


@dataclass(frozen=True)
class TableState:
    """
    What the catalog holds under a table's name, and under its index names.

    Attributes:
        name: The table name, as the description writes it.
        relkind: ``pg_class.relkind`` of the relation that has the name (``r``
            for a table, ``p`` for a partitioned one), or ``None`` if no
            relation has it.
        columns: Each column of that relation, by name, with its type as
            ``format_type`` writes it, for example ``character varying(100)``.
        indexes: One entry per index name the description has, by name.
        not_null: The columns of that relation that are ``NOT NULL``
            (``pg_attribute.attnotnull``).
    """

    name: str
    relkind: Optional[str] = None
    columns: Mapping[str, str] = field(default_factory=dict)
    indexes: Mapping[str, IndexState] = field(default_factory=dict)
    not_null: frozenset[str] = frozenset()


@dataclass(frozen=True)
class CatalogState:
    """
    What a database holds for the described tables.

    It is the result of ``catalog_query``, read by ``from_json``.

    Attributes:
        tables: One entry per described table, by the name the description
            writes.
    """

    tables: Mapping[str, TableState]

    @classmethod
    def from_json(cls, text: str) -> CatalogState:
        """
        Read the one value ``catalog_query`` returns.

        Args:
            text: The JSON text of the query's only column.

        Returns:
            The state.

        Raises:
            ValueError: if ``text`` is not what ``catalog_query`` returns.
        """
        try:
            document = json.loads(text)
            tables = {}
            for table in document["tables"]:
                indexes = {
                    index["name"]: IndexState(
                        name=index["name"],
                        relkind=index["relkind"],
                        on_table=bool(index["on_table"]),
                        valid=bool(index["valid"]),
                        method=index["method"],
                        columns=tuple(index["columns"] or ()),
                        unique=bool(index["unique"]),
                        partial=bool(index["partial"]),
                    )
                    for index in table["indexes"]
                }
                not_null = table["not_null"]
                # A string would read as a set of characters, a dict as its keys.
                if not_null is not None and not (
                    type(not_null) is list
                    and all(type(name) is str for name in not_null)
                ):
                    raise ValueError(
                        f"not_null must be null or a list of strings, got {not_null!r}",
                    )
                tables[table["name"]] = TableState(
                    name=table["name"],
                    relkind=table["relkind"],
                    columns=dict(table["columns"] or ()),
                    indexes=indexes,
                    not_null=frozenset(not_null or ()),
                )
        except (TypeError, KeyError, ValueError) as error:
            raise ValueError(
                f"not the result of catalog_query: {error!r}",
            ) from error
        return cls(tables=tables)


@dataclass(frozen=True)
class PendingStep:
    """
    A step that has not fully run on a table: an object it creates is absent.

    Or a column it makes nullable is still ``NOT NULL``. Running it again is
    safe: every statement only adds what is missing, or relaxes a ``NOT NULL``
    that is still there.

    Attributes:
        table: The table name.
        kind: The component that owns the table.
        step: The step.
        table_missing: Whether the table itself is absent.
        missing_columns: The step's columns the table lacks.
        missing_indexes: The step's indexes that no relation is named after.
        not_null_columns: The columns the step makes nullable that are still
            ``NOT NULL``, or that an earlier pending step of the table adds
            ``NOT NULL`` (they are then in its ``missing_columns``, and this
            step is pending in the same run). A column the table lacks and no
            pending step adds is not here: it is a ``missing_column`` problem
            when ``CREATE TABLE`` created it.
    """

    table: str
    kind: ComponentKind
    step: MigrationStep
    table_missing: bool
    missing_columns: tuple[str, ...]
    missing_indexes: tuple[str, ...]
    not_null_columns: tuple[str, ...] = ()

    @property
    def severity(self) -> Severity:
        """
        ``required`` when the table or a column is missing, or a column is ``NOT NULL``.

        Otherwise ``advisory``: only an index is missing. A column that
        should accept ``NULL`` is ``required``, because the components would
        violate ``NOT NULL`` without the step.
        """
        if self.table_missing or self.missing_columns or self.not_null_columns:
            return "required"
        return "advisory"

    @property
    def notes(self) -> tuple[str, ...]:
        """The locks the step takes on a table that already exists."""
        if self.table_missing:
            return ()
        notes = []
        adds_columns = self.step.objects.columns and not self.step.objects.creates_table
        if adds_columns or self.not_null_columns:
            notes.append(
                f"step {self.step.number} of {self.table}: ALTER TABLE takes an "
                f"ACCESS EXCLUSIVE lock on {self.table} for a moment; every "
                "query on the table waits for it",
            )
        if self.missing_indexes:
            notes.append(
                f"step {self.step.number} of {self.table}: CREATE INDEX takes a "
                f"SHARE lock on {self.table}; writes wait until the index is "
                "built",
            )
        return tuple(notes)

    def describe(self) -> str:
        """
        Say what the step still has to create.

        Returns:
            ``the table is missing``, or the missing columns and indexes, for
            example ``missing columns: channel, not_before; missing indexes:
            taskiq_messages_not_before_idx``, or ``columns still NOT NULL:
            result``.
        """
        if self.table_missing:
            return "the table is missing"
        found = []
        if self.missing_columns:
            found.append(f"missing columns: {', '.join(self.missing_columns)}")
        if self.not_null_columns:
            found.append(f"columns still NOT NULL: {', '.join(self.not_null_columns)}")
        if self.missing_indexes:
            found.append(f"missing indexes: {', '.join(self.missing_indexes)}")
        return "; ".join(found)

    def statements(self, *, lock: bool = True) -> list[str]:
        """
        Return the statements that run the step.

        Args:
            lock: Start with the advisory lock ``run_migrations=True`` takes.

        Returns:
            The statements, without the trailing semicolon.
        """
        table = as_identifier(self.table, what="table name")
        return _script_statements([(table, (self.step,))], lock=lock)


@dataclass(frozen=True)
class SchemaProblem:
    """
    Something wrong with a table that no step fixes: it needs a person.

    ``IF NOT EXISTS`` skips an index whose name is taken whatever the index
    looks like, and no step adds a column to a table it did not create.

    Attributes:
        table: The table name.
        code: What is wrong.
        severity: ``required`` when the components cannot work this way.
        detail: What was found, and what to do about it.
    """

    table: str
    code: ProblemCode
    severity: Severity
    detail: str


@dataclass(frozen=True)
class MigrationPlan:
    """
    How a database compares with the described tables.

    Attributes:
        pending: The steps to run, by table name, then by step number.
        problems: What no step fixes.
        notes: The locks the pending steps take on tables that exist.
    """

    pending: tuple[PendingStep, ...]
    problems: tuple[SchemaProblem, ...]
    notes: tuple[str, ...]

    @property
    def is_current(self) -> bool:
        """Whether no step is pending. Problems do not count."""
        return not self.pending

    @property
    def is_ready(self) -> bool:
        """Whether nothing ``required`` is missing or wrong."""
        return all(item.severity != "required" for item in self.pending) and all(
            problem.severity != "required" for problem in self.problems
        )

    def statements(self, *, lock: bool = True) -> list[str]:
        """
        Return the statements of the pending steps, for a tool that runs one.

        Args:
            lock: Start with the advisory locks, as ``migration_statements``
                does.

        Returns:
            The statements in the order they run; none when nothing is
            pending.
        """
        tables = [
            (as_identifier(table, what="table name"), steps)
            for table, steps in self._steps_by_table().items()
        ]
        return _script_statements(tables, lock=lock)

    def pending_schemas(self, schemas: Iterable[TableSchema]) -> list[TableSchema]:
        """
        Return the tables that have a pending step, each with only those steps.

        A migration format given them renders what the database still lacks,
        as ``render_sql`` of the plan does for the ``sql`` format.

        Args:
            schemas: The tables the plan was computed for.

        Returns:
            Those of ``schemas`` that have a pending step, in their order, each
            with ``steps`` cut down to its pending ones; none when nothing is
            pending.
        """
        steps = self._steps_by_table()
        return [
            replace(schema, steps=tuple(steps[schema.name]))
            for schema in schemas
            if schema.name in steps
        ]

    def render_sql(
        self,
        *,
        transaction: bool = True,
        lock: bool = True,
        lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
        comments: Sequence[str] = (),
    ) -> str:
        """
        Render the script that runs the pending steps.

        The script is only right for the database the plan was computed
        from, at that moment; every statement still only adds what is
        missing, or relaxes a ``NOT NULL`` that is still there.

        Args:
            transaction: Wrap the script in ``BEGIN`` and ``COMMIT``.
            lock: Take the advisory locks, as ``render_sql`` does.
            lock_timeout: Seconds a statement waits for a lock, as for
                ``render_sql``; ``0`` leaves the statement out.
            comments: Lines written as SQL comments after the version line,
                before the list of pending steps, such as the database the
                plan is for.

        Returns:
            The script, as ``render_sql`` lays it out, or an empty string when
            nothing is pending.

        Raises:
            ValueError: if ``lock_timeout`` is not a whole number of seconds
                from 0 to 2147483, or a comment is one ``check_comment``
                refuses.
        """
        statements = _lock_timeout_first(self.statements(lock=lock), lock_timeout)
        for comment in comments:
            check_comment(comment)
        if not self.pending:
            return ""
        pending = ", ".join(
            f"{item.table} step {item.step.number}" for item in self.pending
        )
        return _script_text(
            [*(f"-- {comment}" for comment in comments), f"-- pending: {pending}"],
            statements,
            transaction=transaction,
        )

    def _steps_by_table(self) -> dict[str, list[MigrationStep]]:
        grouped: dict[str, list[MigrationStep]] = {}
        for item in self.pending:
            grouped.setdefault(item.table, []).append(item.step)
        return grouped


def catalog_query(schemas: Iterable[TableSchema]) -> str:
    """
    Return the read-only query that reads what a database holds for the tables.

    The query has no parameters: the names are written in it as literals, so
    every driver runs it as it is. It returns one row with one text column,
    ``catalog``, which ``CatalogState.from_json`` reads; ``read_catalog`` runs
    the query and reads that value. Names resolve through
    the ``search_path``, as the components' own SQL does, and the query reads
    only the ``pg_catalog`` tables, which every role can read by default.

    Args:
        schemas: The tables.

    Returns:
        The query.

    Raises:
        ValueError: if ``schemas`` is empty or a table name cannot be written
            in SQL.
    """
    return catalog_read(catalog_tables(schemas)).text


def catalog_tables(schemas: Iterable[TableSchema]) -> list[CatalogTable]:
    """
    Return the tables of ``schemas`` as the catalog reads them.

    The one adapter from descriptions to what ``catalog_query`` and the
    catalog's other reads take: each table's checked name, and the names of
    the indexes it should have.

    Args:
        schemas: The tables.

    Returns:
        One ``(table, index names)`` pair per table.

    Raises:
        IdentifierError: if a table name cannot be written in SQL.
    """
    return [
        (schema.table, [index.name for index in schema.indexes]) for schema in schemas
    ]


def catalog_state(value: Any) -> CatalogState:
    """
    Read the value the catalog query's ``catalog`` column holds.

    For a caller that runs the query itself, such as one that reads more than
    the catalog in the same round trip.

    Args:
        value: The first column of the first row ``catalog_query`` returns,
            or ``None`` when it returned no row.

    Returns:
        What the database holds for the tables, for ``plan_migration``.

    Raises:
        ValueError: if the query came back without its row, or if its value is
            not what ``catalog_query`` returns.
    """
    if value is None:
        # The query always returns one row: whatever ran it lost the row.
        raise ValueError("catalog_query returned no row, and it always returns one")
    if not isinstance(value, str):
        raise ValueError(
            f"catalog_query returned {type(value).__name__}, not the JSON text "
            "of its catalog column",
        )
    return CatalogState.from_json(value)


async def read_catalog(
    schemas: Iterable[TableSchema],
    fetch_value: Callable[[str], Awaitable[Any]],
) -> CatalogState:
    """
    Run ``catalog_query`` for the tables and read what it returns.

    Args:
        schemas: The tables.
        fetch_value: Runs a query and returns the first column of its first
            row, or ``None`` when it returns no row; asyncpg's
            ``Connection.fetchval``, for example.

    Returns:
        What the database holds for the tables, for ``plan_migration``.

    Raises:
        ValueError: if ``schemas`` is empty or a table name cannot be written
            in SQL, if the query came back without its row, or if its value
            is not what ``catalog_query`` returns.
    """
    return catalog_state(await fetch_value(catalog_query(schemas)))


def plan_migration(
    schemas: Iterable[TableSchema],
    state: CatalogState,
) -> MigrationPlan:
    """
    Compare a database with the described tables.

    A step is pending when an object it creates is absent, or a column it
    makes nullable is still ``NOT NULL``: then running it again completes it.
    An object that exists in another shape than the description, or under a
    name another object took, is a problem instead, because ``IF NOT EXISTS``
    would skip it: nothing runs DDL for it.

    Args:
        schemas: The tables, as given to ``catalog_query``.
        state: What ``catalog_query`` read, through ``CatalogState.from_json``.

    Returns:
        The plan.

    Raises:
        LookupError: if ``state`` holds no entry for a table, because the
            query was built for other tables.
    """
    pending: list[PendingStep] = []
    problems: list[SchemaProblem] = []
    # By table name, so the steps take their locks in the order
    # migration_statements takes them.
    for schema in sorted(schemas, key=lambda table: table.name):
        table = state.tables.get(schema.name)
        if table is None:
            raise LookupError(
                f"the catalog state has no entry for the table {schema.name!r}; "
                "build catalog_query from the same table schemas",
            )
        table_pending, table_problems = _compare_table(schema, table)
        pending.extend(table_pending)
        problems.extend(table_problems)
    return MigrationPlan(
        pending=tuple(pending),
        problems=tuple(problems),
        notes=tuple(note for item in pending for note in item.notes),
    )


@dataclass(frozen=True)
class FormatOptions:
    """
    The options ``taskiq postgresql generate`` passes to a migration format.

    Attributes:
        transaction: ``False`` with ``--no-transaction``: the target tool runs
            the migration in its own transaction.
        lock: ``False`` with ``--no-lock``: leave out the advisory lock.
        lock_timeout: Seconds a statement waits for a lock before the
            migration fails, from ``--lock-timeout``; ``0`` sets none. See
            ``render_sql``.
        extra: Format-specific options, from repeated ``--option KEY=VALUE``.

    Raises:
        ValueError: if ``lock_timeout`` is not a whole number of seconds from
            0 to 2147483.
    """

    transaction: bool = True
    lock: bool = True
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT
    extra: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse a ``lock_timeout`` no format could write."""
        check_lock_timeout(self.lock_timeout)


MigrationFormat = Callable[[Sequence[TableSchema], FormatOptions], str]
"""A migration format: takes the tables and the options, returns the output."""


class UnknownMigrationFormatError(LookupError):
    """No installed package registers the migration format asked for."""

    def __init__(self, format_name: str, available: Iterable[str]) -> None:
        self.format_name = format_name
        self.available = tuple(available)
        super().__init__(
            f"unknown migration format {format_name!r}; available formats: "
            f"{', '.join(self.available) or 'none'}",
        )


class AmbiguousMigrationFormatError(LookupError):
    """Several installed packages, none of them this one, register the format."""

    def __init__(self, format_name: str, distributions: Iterable[str]) -> None:
        self.format_name = format_name
        self.distributions = tuple(distributions)
        super().__init__(
            f"migration format {format_name!r} is registered by several "
            f"installed packages: {', '.join(self.distributions)}; uninstall "
            "all but one of them",
        )


class MigrationFormatOptionError(ValueError):
    """A migration format got an ``--option`` it does not accept."""


@dataclass(frozen=True)
class RenderedStep:
    """
    One step of one table, as a migration file writes it.

    The table, the kind and the step number go into the step's comments, so
    the step refuses what a comment line could not hold.

    Attributes:
        table: The table name.
        kind: The component that owns the table.
        number: The step number.
        comments: What the step does and needs, one line each, to write as
            comments before its statements, without a comment marker.
        statements: The statements of ``step_statements`` for this step of
            this table: the bound on lock waits around the advisory lock and
            the step's own statements.
        nullable_columns: The columns the step makes nullable, which the text
            of a downgrade needs: it leaves them nullable.

    Raises:
        ValueError: if ``table`` is empty or holds a line break or a NUL,
            ``kind`` is not one of ``COMPONENT_KINDS``, ``number`` is not
            positive, or a comment holds a line break, a NUL or an encoding
            declaration.
        TypeError: if ``number`` is not an ``int``.
    """

    table: str
    kind: ComponentKind
    number: int
    comments: tuple[str, ...]
    statements: tuple[str, ...]
    nullable_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Refuse a table, kind or step number a comment line cannot hold."""
        Identifier.parse(self.table, what="table name", warn=False)
        _component_kind(self.kind)
        # A bool is an int, and a step number of True means nothing.
        if type(self.number) is not int:
            raise TypeError(
                f"the step number must be an int, got {type(self.number).__name__}",
            )
        if self.number < 1:
            raise ValueError(f"the step number must be positive, got {self.number}")
        # The lines go into a generated file, and the class is public: nothing
        # but the checked constructors may be trusted to have checked them.
        for comment in self.comments:
            check_comment(comment)
        for name in self.nullable_columns:
            _check_nullable_name(self.number, self.table, name)

    @classmethod
    def of(
        cls,
        table: str,
        kind: ComponentKind,
        step: MigrationStep,
        *,
        transaction_note: str,
        lock: bool = True,
        lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
        notes: Sequence[str] = (),
    ) -> RenderedStep:
        """
        Lay out one step of one table.

        Args:
            table: The table name.
            kind: The component that owns the table.
            step: The step.
            transaction_note: What the step needs of the tool's transaction,
                written after the line that says what the step runs.
            lock: Start with the advisory lock ``run_migrations=True`` takes.
            lock_timeout: Seconds a statement of the step waits for a lock;
                ``0`` bounds nothing.
            notes: The locks the step takes, such as ``PendingStep.notes``,
                one sentence each.

        Returns:
            The step.

        Raises:
            ValueError: if ``lock_timeout`` is not a whole number of seconds
                from 0 to 2147483, or as the class does.
            TypeError: as the class does.
        """
        statements = scoped_lock_timeout(
            _script_statements(
                [(as_identifier(table, what="table name"), (step,))],
                lock=lock,
            ),
            lock_timeout,
        )
        bound = (
            f"Lock waits end after {lock_timeout} s; the last statement restores "
            "the transaction's lock timeout."
        )
        title = f"step {step.number} of the {kind} table {table}"
        return cls(
            table=table,
            kind=kind,
            number=step.number,
            comments=(
                f"taskiq-postgresql {title}: the SQL run_migrations=True runs,",
                f"and each statement {statements_effect([step])}.",
                transaction_note,
                *(f"{note}." for note in notes),
                *([bound] if lock_timeout else []),
            ),
            statements=tuple(statements),
            nullable_columns=step.objects.nullable_columns,
        )


def statements_effect(steps: Iterable[MigrationStep]) -> str:
    """
    Say what the statements of ``steps`` do, for a generated file's comment.

    Every released step only adds, so the text for those is the one every
    release wrote; a step that makes columns nullable says so too.

    Args:
        steps: The steps the comment is about.

    Returns:
        A phrase that follows "each statement", such as ``only adds what is
        missing``.
    """
    nullable = [name for step in steps for name in step.objects.nullable_columns]
    if not nullable:
        return "only adds what is missing"
    return (
        f"only adds what is missing, or makes {', '.join(nullable)} nullable "
        "where it is still NOT NULL"
    )


def render_steps(
    schemas: Sequence[TableSchema],
    options: FormatOptions,
    *,
    format_name: str,
    accepted: Sequence[str],
    transaction_note: str,
    comments: Sequence[str] = (),
) -> list[RenderedStep]:
    """
    Check what a migration format was given, and lay out every step it writes.

    A format that writes a migration file of a tool's own, such as
    ``alembic`` or ``django``, starts here and keeps only its template and the
    way it writes a statement. Each step runs in the tool's transaction with
    the advisory lock when ``options.lock`` is set, and with its lock waits
    bounded by ``options.lock_timeout`` (see ``step_statements``).

    Args:
        schemas: The tables.
        options: The options of ``taskiq postgresql generate``; ``extra`` may
            only hold ``accepted`` keys.
        format_name: The format's name, for the error about an option.
        accepted: The options of ``extra`` the format takes.
        transaction_note: What every step needs of the tool's transaction,
            one line written with each step.
        comments: The lines the format writes as comments at the top of its
            file.

    Returns:
        Each step of each table, the tables by name, each table's steps in
        their order.

    Raises:
        MigrationFormatOptionError: if ``options.extra`` holds a key that is
            not in ``accepted``.
        ValueError: if a comment holds a line break, a NUL or an encoding
            declaration, a table name is not a plain SQL identifier, or as
            ``RenderedStep`` does.
        TypeError: as ``RenderedStep`` does.
    """
    for comment in comments:
        check_comment(comment)
    unknown = [key for key in options.extra if key not in accepted]
    if unknown:
        raise MigrationFormatOptionError(
            f"the {format_name} format takes {', '.join(accepted)}; unknown "
            f"option{'s' if len(unknown) > 1 else ''}: {', '.join(unknown)}",
        )
    for schema in schemas:
        check_plain_identifier(schema.name)
    return [
        RenderedStep.of(
            schema.name,
            schema.kind,
            step,
            transaction_note=transaction_note,
            lock=options.lock,
            lock_timeout=options.lock_timeout,
        )
        for schema in sorted(schemas, key=lambda table: table.name)
        for step in schema.steps
    ]


def sql_format(schemas: Sequence[TableSchema], options: FormatOptions) -> str:
    """
    Render the ``sql`` format: the script of ``render_sql``.

    Args:
        schemas: The tables.
        options: ``transaction``, ``lock`` and ``lock_timeout`` go to
            ``render_sql``.

    Returns:
        The SQL script.

    Raises:
        MigrationFormatOptionError: if ``options.extra`` holds anything; the
            ``sql`` format takes no option.
    """
    if options.extra:
        raise MigrationFormatOptionError(
            f"the sql format takes no --option; got {', '.join(sorted(options.extra))}",
        )
    return render_sql(
        schemas,
        transaction=options.transaction,
        lock=options.lock,
        lock_timeout=options.lock_timeout,
    )


def available_formats() -> list[str]:
    """
    List the installed migration formats, without loading any.

    Returns:
        The format names, sorted.
    """
    return sorted({entry_point.name for entry_point in _entry_points()})


def load_format(name: str) -> MigrationFormat:
    """
    Load a migration format registered under ``MIGRATION_FORMATS_GROUP``.

    A format this package registers, such as ``sql``, is always the one
    loaded, whatever else registers its name. Any other name must be
    registered by one installed package only.

    Args:
        name: The entry-point name, for example ``"sql"``.

    Returns:
        The format callable.

    Raises:
        UnknownMigrationFormatError: if no installed package registers ``name``;
            the error lists the available formats.
        AmbiguousMigrationFormatError: if several installed packages, none of
            them this one, register ``name``; the error names them.
        ImportError: if the format cannot be imported, usually because its
            optional dependency is missing; the message says what to install.
        TypeError: if the entry point does not name a callable.
    """
    found = [entry_point for entry_point in _entry_points() if entry_point.name == name]
    if not found:
        raise UnknownMigrationFormatError(name, available_formats())
    own = [
        entry_point
        for entry_point in found
        if _distribution_name(entry_point) == _DISTRIBUTION
    ]
    # Otherwise the order of sys.path would choose, and an installed package
    # could replace a format without anyone noticing.
    if not own and len(found) > 1:
        raise AmbiguousMigrationFormatError(
            name,
            sorted(
                _distribution_name(entry_point) or "<unknown>" for entry_point in found
            ),
        )
    entry_point = (own or found)[0]
    try:
        loaded = entry_point.load()
    except ImportError as error:
        raise ImportError(_install_hint(entry_point, error)) from error
    if not callable(loaded):
        raise TypeError(
            f"the migration format {name!r} names {type(loaded).__name__}, "
            "not a callable",
        )
    return loaded


def _entry_points() -> Iterable[Any]:
    """Return the entry points of ``MIGRATION_FORMATS_GROUP``, unloaded."""
    return entry_points(group=MIGRATION_FORMATS_GROUP)


def _distribution_name(entry_point: Any) -> Optional[str]:
    # An EntryPoint built by hand, rather than read from an installed
    # package, has no distribution.
    distribution = getattr(entry_point, "dist", None)
    return None if distribution is None else distribution.name


def _install_hint(entry_point: Any, error: ImportError) -> str:
    package = _distribution_name(entry_point) or _DISTRIBUTION
    # A format's optional dependencies go in an extra named after the format,
    # unless the entry point names its extras.
    extra = ",".join(getattr(entry_point, "extras", None) or [entry_point.name])
    return (
        f"Migration format {entry_point.name!r} is not installed ({error}). "
        f"Please install it with `pip install '{package}[{extra}]'`."
    )


def _component_kind(kind: object) -> ComponentKind:
    for known in COMPONENT_KINDS:
        if kind == known:
            return known
    raise ValueError(
        f"unknown component kind {kind!r}; expected one of "
        f"{', '.join(COMPONENT_KINDS)}",
    )


def _checked_tables(
    schemas: Iterable[TableSchema],
) -> list[tuple[TableSchema, Identifier]]:
    """Pair each table with its checked name, refusing one the rule refuses."""
    tables = list(schemas)
    if not tables:
        raise ValueError("a migration script needs at least one table schema")
    return [(table, table.table) for table in tables]


def _script_statements(
    tables: Sequence[tuple[Identifier, Sequence[MigrationStep]]],
    *,
    lock: bool,
) -> list[str]:
    statements: list[str] = []
    if lock:
        # Sorted, so two scripts that lock the same tables take the locks in
        # the same order and cannot deadlock each other.
        locked = {table.configured: table for table, _ in tables}
        statements.extend(
            migration_lock_script(locked[name]).text for name in sorted(locked)
        )
    statements.extend(
        statement
        for _, steps in tables
        for step in steps
        for statement in step.statements
    )
    return statements


def _steps_through(
    table: TableSchema,
    through: Optional[int],
) -> tuple[MigrationStep, ...]:
    if through is None:
        return table.steps
    numbers = [step.number for step in table.steps]
    if through not in numbers:
        raise ValueError(
            f"the {table.kind} table {table.name!r} has steps "
            f"{', '.join(map(str, numbers))}, not step {through}: name a step "
            "every table has, once per table if they differ",
        )
    return tuple(step for step in table.steps if step.number <= through)


def _script_text(
    header: Sequence[str],
    statements: Sequence[str],
    *,
    transaction: bool,
) -> str:
    if transaction:
        statements = in_transaction(statements)
    lines = [
        f"-- taskiq-postgresql {package_version()}",
        *header,
        *(f"{statement};" for statement in statements),
    ]
    return "\n".join(lines) + "\n"


def _lock_timeout_first(statements: Sequence[str], lock_timeout: int) -> list[str]:
    check_lock_timeout(lock_timeout)
    if not lock_timeout:
        return list(statements)
    # First, so that it also bounds the wait for the advisory lock.
    return [lock_timeout_scripts(lock_timeout).set_local.text, *statements]


# The type format_type() reports for each TypeName. SERIAL is an integer
# column with a sequence default.
_CATALOG_TYPES: Final[Mapping[TypeName, str]] = {
    "serial": "integer",
    "uuid": "uuid",
    "varchar": "character varying",
    "text": "text",
    "bytea": "bytea",
    "jsonb": "jsonb",
    "timestamptz": "timestamp with time zone",
}
# pg_class.relkind of a table and a partitioned table, and of an index and a
# partitioned index.
_TABLE_RELKINDS: Final = frozenset({"r", "p"})
_INDEX_RELKINDS: Final = frozenset({"i", "I"})


def _catalog_type(column: ColumnSchema) -> str:
    spelling = _CATALOG_TYPES[column.type_name]
    return spelling if column.length is None else f"{spelling}({column.length})"


def _compare_table(
    schema: TableSchema,
    table: TableState,
) -> tuple[list[PendingStep], list[SchemaProblem]]:
    problems = [
        problem
        for index in schema.indexes
        for problem in _index_problems(schema, index, table)
    ]
    if table.relkind is None:
        pending = [
            PendingStep(
                table=schema.name,
                kind=schema.kind,
                step=step,
                table_missing=True,
                missing_columns=step.objects.columns,
                missing_indexes=tuple(index.name for index in step.objects.indexes),
            )
            for step in schema.steps
        ]
        return pending, problems
    if table.relkind not in _TABLE_RELKINDS:
        problems.insert(
            0,
            SchemaProblem(
                table=schema.name,
                code="name_taken",
                severity="required",
                detail=(
                    f"{schema.name} is a relation of kind {table.relkind!r}, not "
                    "a table: rename it, or configure another table name"
                ),
            ),
        )
        return [], problems
    pending = []
    # What an earlier pending step adds is not there yet, and no catalog says
    # it is NOT NULL: a step that relaxes it must run in the same pass.
    adding: set[str] = set()
    for step in schema.steps:
        missing_columns = tuple(
            name for name in step.objects.columns if name not in table.columns
        )
        missing_indexes = tuple(
            index.name
            for index in step.objects.indexes
            if not _index_state(table, index.name).exists
        )
        if step.objects.creates_table:
            # CREATE TABLE IF NOT EXISTS skips an existing table: running the
            # step again would not bring a column back.
            problems.extend(
                SchemaProblem(
                    table=schema.name,
                    code="missing_column",
                    severity="required",
                    detail=(
                        f"{schema.name} has no column {name}, which step "
                        f"{step.number} creates with the table: add it by hand"
                    ),
                )
                for name in missing_columns
            )
            missing_columns = ()
        # A column the table lacks is reported by the earlier step that adds
        # it, or as a missing_column problem when CREATE TABLE created it; the
        # step that relaxes it is pending too only in the first case.
        not_null_columns = tuple(
            name
            for name in step.objects.nullable_columns
            if name in adding or (name in table.columns and name in table.not_null)
        )
        adding.update(missing_columns)
        if missing_columns or missing_indexes or not_null_columns:
            pending.append(
                PendingStep(
                    table=schema.name,
                    kind=schema.kind,
                    step=step,
                    table_missing=False,
                    missing_columns=missing_columns,
                    missing_indexes=missing_indexes,
                    not_null_columns=not_null_columns,
                ),
            )
    problems.extend(
        SchemaProblem(
            table=schema.name,
            code="column_type",
            severity="advisory",
            detail=(
                # !r: the type comes from the catalog, and a log line must not
                # be split by what a database holds.
                f"{schema.name}.{column.name} is {table.columns[column.name]!r}, "
                f"not {_catalog_type(column)}: check the component's "
                "configuration, such as field_for_task_id"
            ),
        )
        for column in schema.columns
        if column.name in table.columns
        and table.columns[column.name] != _catalog_type(column)
    )
    return pending, problems


def _index_state(table: TableState, name: str) -> IndexState:
    return table.indexes.get(name) or IndexState(name=name)


def _index_problems(
    schema: TableSchema,
    index: IndexSchema,
    table: TableState,
) -> list[SchemaProblem]:
    found = _index_state(table, index.name)
    if not found.exists:
        return []

    def problem(code: ProblemCode, detail: str) -> list[SchemaProblem]:
        return [SchemaProblem(schema.name, code, "advisory", detail)]

    if found.relkind not in _INDEX_RELKINDS:
        return problem(
            "name_taken",
            f"{index.name} is a relation of kind {found.relkind!r}, not an "
            f"index, so {schema.name} cannot get that index: rename the relation",
        )
    if not found.on_table:
        return problem(
            "index_on_other_table",
            f"the index {index.name} belongs to another table, so "
            f"{schema.name} cannot get an index of that name: rename or drop "
            "the other index",
        )
    if not found.valid:
        return problem(
            "invalid_index",
            f"the index {index.name} is invalid (a failed CREATE INDEX "
            "CONCURRENTLY leaves one), and queries do not use it: drop it, "
            "then run the migration again",
        )
    if (found.method, found.columns, found.unique, found.partial) != (
        index.method,
        index.columns,
        False,
        False,
    ):
        return problem(
            "index_shape",
            f"the index {index.name} is not the {index.method} index on "
            f"({', '.join(index.columns)}) the description has: drop it, then "
            "run the migration again",
        )
    return []


def _column_schema(column: Column) -> ColumnSchema:
    spelling = " ".join(column.type.upper().split())
    length: Optional[int] = None
    sized = _SIZED_TYPE.fullmatch(spelling)
    if sized is not None:
        spelling, length = sized["name"], int(sized["length"])
    type_name = _TYPE_NAMES.get(spelling)
    if type_name is None:
        raise ValueError(
            f"column {column.name!r} has the SQL type {column.type!r}, which "
            "taskiq_postgresql.migrations has no TypeName for",
        )
    return ColumnSchema(
        name=column.name,
        sql_type=column.type,
        type_name=type_name,
        length=length,
        nullable=column.nullable,
        default=None if column.default is None else str(column.default),
        primary_key=column.primary_key,
    )


def _schema(
    kind: ComponentKind,
    create_table: CreateTableQuery,
    create_index: CreateIndexQuery,
    index_columns: Sequence[Column],
    *,
    add_columns: AddColumnsQuery,
    added_columns: Sequence[Column],
    added_indexes: Sequence[AddedIndex],
) -> TableSchema:
    name = create_table.table_name
    names = index_names(name, index_columns, added_indexes)
    created_indexes = tuple(
        IndexSchema(name=index, columns=(column.name,), method="hash")
        for index, column in zip(names, index_columns, strict=False)
    )
    added = tuple(
        IndexSchema(
            name=index,
            columns=tuple(column.name for column in added_index.columns),
            method="btree",
        )
        for index, added_index in zip(
            names[len(index_columns) :],
            added_indexes,
            strict=True,
        )
    )
    # The same check the driver ran when it was built, from the same lists the
    # steps are built from, and run again on purpose: a description built
    # without a driver (by kind, or from a script) must be refused the same
    # way. CREATE INDEX IF NOT EXISTS skips a name that collides.
    check_table_objects(
        create_table.table,
        columns=create_table.columns,
        index_columns=index_columns,
        added_indexes=added_indexes,
    )
    steps = [
        MigrationStep(
            number=1,
            statements=(
                create_table.make_query(),
                # One statement per index; make_query ends each with ";".
                *(
                    create_index.make_query([column]).removesuffix(";")
                    for column in index_columns
                ),
            ),
            objects=StepObjects(
                creates_table=True,
                columns=tuple(column.name for column in create_table.columns),
                indexes=created_indexes,
            ),
        ),
    ]
    if added_columns or added_indexes:
        # What the driver runs after the CREATEs, so a table an older release
        # created gets it too.
        steps.append(
            MigrationStep(
                number=2,
                statements=_added_statements(add_columns, added_columns, added_indexes),
                objects=StepObjects(
                    creates_table=False,
                    columns=tuple(column.name for column in added_columns),
                    indexes=added,
                ),
            ),
        )
    return TableSchema(
        kind=kind,
        name=name,
        # ADD COLUMN appends, so the added columns come last in the table.
        columns=tuple(
            _column_schema(column) for column in (*create_table.columns, *added_columns)
        ),
        indexes=(*created_indexes, *added),
        steps=tuple(steps),
    )


def declared_schema(kind: ComponentKind, declaration: TableDeclaration) -> TableSchema:
    """
    Describe a table from its declaration, with the catalog's own builders.

    The one description of a table, whatever it comes from: a component, a
    query driver or a kind. The builders and their arguments are the ones the
    drivers run in ``on_startup``, so the steps are the statements it runs.

    Args:
        kind: The component that owns the table.
        declaration: What the component hands its driver.

    Returns:
        The table's description.

    Raises:
        IdentifierError: if the declared names clash with each other.
    """
    table = declaration.table
    return _schema(
        kind,
        CreateTableQuery(table, declaration.table_columns),
        CreateIndexQuery(table),
        declaration.index_columns or (),
        add_columns=AddColumnsQuery(table),
        added_columns=declaration.added_columns,
        added_indexes=declaration.added_indexes,
    )


def _added_statements(
    add_columns: AddColumnsQuery,
    added_columns: Sequence[Column],
    added_indexes: Sequence[AddedIndex],
) -> tuple[str, ...]:
    # The driver runs make_query(added_columns, added_indexes) as one string:
    # the ALTER TABLE, then each CREATE INDEX, each ending with ";". Built one
    # statement at a time from the same query, so the text stays the same.
    statements = []
    if added_columns:
        statements.append(add_columns.make_query(added_columns).removesuffix(";"))
    statements.extend(
        add_columns.make_query((), [index]).removesuffix(";") for index in added_indexes
    )
    return tuple(statements)


def _component_classes() -> dict[ComponentKind, type]:
    # Imported here so that a component module may one day import this one.
    from taskiq_postgresql.broker import PostgresqlBroker  # noqa: PLC0415
    from taskiq_postgresql.result_backend import (  # noqa: PLC0415
        PostgresqlResultBackend,
    )
    from taskiq_postgresql.scheduler_source import (  # noqa: PLC0415
        PostgresqlSchedulerSource,
    )

    return {
        "broker": PostgresqlBroker,
        "result_backend": PostgresqlResultBackend,
        "scheduler_source": PostgresqlSchedulerSource,
    }


def _kind_of(component: object) -> ComponentKind:
    for kind, component_class in _component_classes().items():
        if isinstance(component, component_class):
            return kind
    raise TypeError(
        "expected a taskiq-postgresql component or one of "
        f"{', '.join(COMPONENT_KINDS)}, got {type(component).__name__}",
    )


def _default(component_class: type, parameter: str) -> Any:
    # Read from the constructor, so the defaults have one source.
    return signature(component_class).parameters[parameter].default


def _schema_of_kind(
    kind: str,
    table_name: Optional[str],
    field_for_task_id: Optional[str],
) -> TableSchema:
    component_kind = _component_kind(kind)
    if field_for_task_id is not None and field_for_task_id not in TASK_ID_TYPES:
        raise ValueError(
            f"field_for_task_id must be one of {', '.join(TASK_ID_TYPES)}, "
            f"got {field_for_task_id!r}",
        )
    component_class = _component_classes()[component_kind]
    name = _default(component_class, "table_name") if table_name is None else table_name
    # Describing a table by kind is user configuration, so mixed case warns,
    # pointing at the caller of table_schema.
    table = Identifier.parse(name, what="table name", stacklevel=4)
    if component_kind == "scheduler_source":
        if field_for_task_id is not None:
            raise ValueError(
                "the scheduler source has no task_id column; "
                "field_for_task_id does not apply to it",
            )
        declaration = _scheduler_source_parts(table)
    else:
        task_id_type = field_for_task_id or _default(
            component_class,
            "field_for_task_id",
        )
        declaration = (
            _broker_parts(table, task_id_type)
            if component_kind == "broker"
            else _result_backend_parts(table, task_id_type)
        )
    return declared_schema(component_kind, declaration)


# Each builder below hands over what the component hands its driver: the key,
# the columns between it and created_at, the hash-indexed columns, and the
# columns and indexes added after the CREATEs.
# test_a_kind_describes_the_same_table_as_a_component pins them to it.


def _broker_parts(table: Identifier, task_id_type: str) -> TableDeclaration:
    from taskiq_postgresql.broker import Table  # noqa: PLC0415

    columns = Table(task_id=Column(name="task_id", type_=task_id_type))
    claim_columns = ClaimColumns(channel=columns.channel, not_before=columns.not_before)
    return TableDeclaration(
        table=table,
        primary_key=columns.primary_key,
        columns=[
            columns.task_id,
            columns.task_name,
            columns.message,
            columns.labels,
        ],
        created_at=columns.created_at,
        index_columns=[columns.primary_key],
        added_columns=claim_columns,
        added_indexes=[
            AddedIndex(
                name=index_name(table.configured, columns.not_before),
                columns=[*claim_columns, columns.primary_key],
            ),
        ],
    )


def _result_backend_parts(
    table: Identifier,
    task_id_type: str,
) -> TableDeclaration:
    from taskiq_postgresql.result_backend import Table  # noqa: PLC0415

    columns = Table(primary_key=PrimaryKeyColumn(name="task_id", type_=task_id_type))
    return TableDeclaration(
        table=table,
        primary_key=columns.primary_key,
        columns=[columns.result],
        # The result backend leaves created_at to the driver's default.
        created_at=CreatedAtColumn(),
        index_columns=[columns.primary_key],
    )


def _scheduler_source_parts(table: Identifier) -> TableDeclaration:
    from taskiq_postgresql.scheduler_source import Table  # noqa: PLC0415

    columns = Table()
    return TableDeclaration(
        table=table,
        primary_key=columns.primary_key,
        columns=[columns.task_name, columns.schedule, columns.updated_at],
        created_at=columns.created_at,
        index_columns=[columns.primary_key, columns.task_name],
    )


def _components_of(
    target: object,
    app_dir: Union[str, _SystemPath, None],
) -> list[Component]:
    loaded = _load_target(target, app_dir) if isinstance(target, str) else target
    components = _components_in(loaded)
    if not components:
        raise LookupError(
            f"{_describe(target)} holds no taskiq-postgresql component: "
            "expected a component, or a broker or TaskiqScheduler that uses one",
        )
    return components


def keep_table_schema(kept: dict[str, TableSchema], schema: TableSchema) -> None:
    """
    Keep the first description of each table; refuse a table described two ways.

    The one rule for every place that collects descriptions (this module, the
    SQLAlchemy and Alembic integrations). It keys by the folded name, because
    PostgreSQL has one relation for ``Jobs`` and ``jobs`` and two spellings
    would take two migration lock keys.

    Args:
        kept: The descriptions so far, by folded table name; updated in place.
        schema: The description to add.

    Raises:
        ValueError: if ``schema`` spells the table name differently from the
            description kept, or describes it with different columns.
    """
    first = kept.setdefault(fold(schema.name), schema)
    if first.name != schema.name:
        raise ValueError(
            f"the table is configured as {first.name!r} and as "
            f"{schema.name!r}, which PostgreSQL reads as one table: use "
            "one spelling",
        )
    if first != schema:
        raise ValueError(
            f"the table {schema.name!r} is described twice, with different "
            "columns: give each component a table of its own",
        )


def _one_per_table(described: Iterable[tuple[_T, TableSchema]]) -> list[_T]:
    """Keep the first item of each table; refuse a table described two ways."""
    kept: dict[str, TableSchema] = {}
    items: list[_T] = []
    for item, schema in described:
        is_new = fold(schema.name) not in kept
        keep_table_schema(kept, schema)
        if is_new:
            items.append(item)
    return items


def _describe(target: object) -> str:
    return repr(target) if isinstance(target, str) else type(target).__name__


def _load_target(spec: str, app_dir: Union[str, _SystemPath, None]) -> object:
    module_name, attribute = parse_target(spec)
    if app_dir is SYSTEM_PATH:
        loaded = getattr(import_module(module_name), attribute)
        return loaded() if isfunction(loaded) else loaded
    # As taskiq worker does: app_dir stays on sys.path, the working directory
    # only while the module is imported.
    if isinstance(app_dir, str) and app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    cwd = str(Path.cwd())
    added_cwd = cwd not in sys.path
    if added_cwd:
        sys.path.insert(0, cwd)
    try:
        module = import_module(module_name)
    finally:
        if added_cwd and cwd in sys.path:
            sys.path.remove(cwd)
    loaded = getattr(module, attribute)
    # taskiq worker accepts a factory function in place of the broker.
    return loaded() if isfunction(loaded) else loaded


def _components_in(target: object) -> list[Component]:
    from taskiq import AsyncBroker, TaskiqScheduler  # noqa: PLC0415

    from taskiq_postgresql.broker import PostgresqlBroker  # noqa: PLC0415
    from taskiq_postgresql.result_backend import (  # noqa: PLC0415
        PostgresqlResultBackend,
    )
    from taskiq_postgresql.scheduler_source import (  # noqa: PLC0415
        PostgresqlSchedulerSource,
    )

    found: list[Component] = []
    if isinstance(target, TaskiqScheduler):
        found.extend(_components_in(target.broker))
        found.extend(
            source
            for source in target.sources
            if isinstance(source, PostgresqlSchedulerSource)
        )
    elif isinstance(target, AsyncBroker):
        if isinstance(target, PostgresqlBroker):
            found.append(target)
        if isinstance(target.result_backend, PostgresqlResultBackend):
            found.append(target.result_backend)
    elif isinstance(target, (PostgresqlResultBackend, PostgresqlSchedulerSource)):
        found.append(target)
    return found
