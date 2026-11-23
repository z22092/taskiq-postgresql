"""
Describe the tables this package creates, and render the SQL that creates them.

``run_migrations=True``, ``taskiq postgresql generate`` and integrations with
other migration tools share this description. It is built from the columns
the components' drivers use, so the SQL a tool runs is the SQL a worker runs.

A table is described twice over: structurally (columns, types, indexes), for
tools that model tables themselves, and as an ordered list of migration steps
holding the exact SQL statements, for tools that run SQL. Neither needs the
other parsed.

Importing this module imports no driver library.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib import import_module
from importlib.metadata import PackageNotFoundError, entry_points, version
from inspect import isfunction, signature
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    NamedTuple,
    Optional,
    Union,
    cast,
    get_args,
)

from taskiq_postgresql.abc.query import (
    MIGRATION_LOCK_NAMESPACE,
    AddColumnsQuery,
    AddedIndex,
    ClaimColumns,
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    PrimaryKeyColumn,
    index_name,
)

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
    "TASK_ID_TYPES",
    "ColumnSchema",
    "ComponentKind",
    "FormatOptions",
    "IndexMethod",
    "IndexSchema",
    "MigrationFormat",
    "MigrationFormatOptionError",
    "MigrationStep",
    "TableSchema",
    "TaskIdType",
    "TypeName",
    "UnknownMigrationFormatError",
    "available_formats",
    "collect_components",
    "collect_schemas",
    "load_format",
    "render_sql",
    "sql_format",
    "table_schema",
]

ComponentKind = Literal["broker", "result_backend", "scheduler_source"]
"""The three components that own a table."""

COMPONENT_KINDS: Final[tuple[ComponentKind, ...]] = get_args(ComponentKind)

TaskIdType = Literal["VarChar", "Text", "Uuid"]
"""The values ``field_for_task_id`` accepts."""

TASK_ID_TYPES: Final[tuple[TaskIdType, ...]] = get_args(TaskIdType)

TypeName = Literal["serial", "uuid", "varchar", "text", "bytea", "jsonb", "timestamptz"]
"""A column type, named once whatever spelling the SQL uses."""

IndexMethod = Literal["hash", "btree"]
"""The access method of an index."""

MIGRATION_FORMATS_GROUP: Final = "taskiq_postgresql.migration_formats"
"""The entry-point group that ``taskiq postgresql generate --format`` reads."""

DEFAULT_LOCK_TIMEOUT: Final = 10
"""Seconds a script's statement waits for a lock before the script fails."""

# The server keeps lock_timeout as an int of milliseconds and refuses a larger
# value when the script runs.
_MAX_LOCK_TIMEOUT: Final = 2_147_483

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
class MigrationStep:
    """
    One additive, idempotent step of a table's migration.

    Steps run in ``number`` order and each one is idempotent, so the whole
    list brings a table of any earlier release up to date. Idempotent is not
    lock-free: on a table that already exists, ``ALTER TABLE`` and
    ``CREATE INDEX`` lock it before they find that nothing is missing.

    Attributes:
        number: Position of the step, from 1.
        statements: The SQL statements, without the trailing semicolon, in
            the order they run. They are the statements ``run_migrations=True``
            executes, byte for byte.
    """

    number: int
    statements: tuple[str, ...]


@dataclass(frozen=True)
class TableSchema:
    """
    A table this package creates: its final shape and the steps that build it.

    Attributes:
        kind: The component that owns the table.
        name: Table name, as configured on the component.
        columns: Every column, in table order, after all steps.
        indexes: Every index after all steps, besides the primary key's.
        steps: The migration steps, in the order they run.
    """

    kind: ComponentKind
    name: str
    columns: tuple[ColumnSchema, ...]
    indexes: tuple[IndexSchema, ...]
    steps: tuple[MigrationStep, ...]

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
    component: Union[Component, ComponentKind],
    *,
    table_name: Optional[str] = None,
    field_for_task_id: Optional[TaskIdType] = None,
) -> TableSchema:
    """
    Describe the table of a component, or of a component kind.

    Given a component, the description comes from the columns its driver
    uses. Given a kind, it needs no instance, no DSN and no driver: the
    component's defaults apply unless ``table_name`` or ``field_for_task_id``
    say otherwise.

    Args:
        component: A ``PostgresqlBroker``, ``PostgresqlResultBackend`` or
            ``PostgresqlSchedulerSource``, or one of ``COMPONENT_KINDS``.
        table_name: With a kind, the table name; the component's default
            otherwise.
        field_for_task_id: With the ``broker`` or ``result_backend`` kind, the
            type of the ``task_id`` column; ``"Uuid"`` by default.

    Returns:
        The table's description.

    Raises:
        TypeError: if ``component`` is neither a component nor a kind, or if
            ``table_name`` or ``field_for_task_id`` come with a component.
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
    kind = _kind_of(component)
    driver = component.driver
    return _schema(
        kind,
        driver.create_table_query,
        driver.create_index_query,
        driver.index_columns or (),
        add_columns=driver.add_columns_query,
        added_columns=driver.added_columns,
        added_indexes=driver.added_indexes,
    )


def collect_components(
    *targets: object,
    app_dir: Optional[str] = None,
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
        app_dir: A directory to add to ``sys.path`` before importing a path.

    Returns:
        The components, in target order.

    Raises:
        ValueError: if a path is not ``"module:object"``, or two components
            use one table name with different columns.
        LookupError: if a target holds no component of this package.
        ImportError: if a path's module cannot be imported.
        AttributeError: if a path's module has no such object.
    """
    return [component for component, _ in _collect(targets, app_dir)]


def collect_schemas(
    *targets: object,
    app_dir: Optional[str] = None,
) -> list[TableSchema]:
    """
    Describe the tables behind brokers, schedulers and import paths.

    Args:
        *targets: As for ``collect_components``.
        app_dir: As for ``collect_components``.

    Returns:
        One description per table, in target order.

    Raises:
        ValueError: as ``collect_components`` does.
        LookupError: as ``collect_components`` does.
        ImportError: as ``collect_components`` does.
        AttributeError: as ``collect_components`` does.
    """
    return [schema for _, schema in _collect(targets, app_dir)]


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
    if not tables:
        raise ValueError("render_sql() needs at least one table schema")
    for table in tables:
        _check_table_name(table.name)
    _check_lock_timeout(lock_timeout)
    statements: list[str] = []
    if lock_timeout:
        # First, so that it also bounds the wait for the advisory lock.
        statements.append(f"SET LOCAL lock_timeout = '{lock_timeout}s'")
    if lock:
        # Sorted, so two scripts that lock the same tables take the locks in
        # the same order and cannot deadlock each other.
        statements.extend(
            _lock_statement(name) for name in sorted({table.name for table in tables})
        )
    statements.extend(
        statement
        for table in tables
        for step in table.steps
        for statement in step.statements
    )
    if transaction:
        statements = ["BEGIN", *statements, "COMMIT"]
    lines = [
        f"-- taskiq-postgresql {_package_version()}",
        f"-- tables: {', '.join(table.name for table in tables)}",
        *(f"{statement};" for statement in statements),
    ]
    return "\n".join(lines) + "\n"


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
        _check_lock_timeout(self.lock_timeout)


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


class MigrationFormatOptionError(ValueError):
    """A migration format got an ``--option`` it does not accept."""


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

    Args:
        name: The entry-point name, for example ``"sql"``.

    Returns:
        The format callable.

    Raises:
        UnknownMigrationFormatError: if no installed package registers ``name``;
            the error lists the available formats.
        ImportError: if the format cannot be imported, usually because its
            optional dependency is missing; the message says what to install.
        TypeError: if the entry point does not name a callable.
    """
    for entry_point in _entry_points():
        if entry_point.name == name:
            break
    else:
        raise UnknownMigrationFormatError(name, available_formats())
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


def _install_hint(entry_point: Any, error: ImportError) -> str:
    distribution = getattr(entry_point, "dist", None)
    package = distribution.name if distribution is not None else "taskiq-postgresql"
    # A format's optional dependencies go in an extra named after the format,
    # unless the entry point names its extras.
    extra = ",".join(getattr(entry_point, "extras", None) or [entry_point.name])
    return (
        f"Migration format {entry_point.name!r} is not installed ({error}). "
        f"Please install it with `pip install '{package}[{extra}]'`."
    )


def _package_version() -> str:
    try:
        return version("taskiq-postgresql")
    except PackageNotFoundError:
        # A source tree that was never installed has no metadata.
        return "unknown"


def _check_table_name(name: str) -> None:
    # The name appears in a header comment, which a line break would end, and
    # a NUL cannot be sent to PostgreSQL at all.
    if not name or any(character in name for character in "\0\r\n"):
        raise ValueError(
            f"table name {name!r} cannot be written in a SQL script: it is "
            "empty or holds a line break or a NUL",
        )


def _check_lock_timeout(seconds: int) -> None:
    # A bool is an int, and True would read as one second.
    if (
        isinstance(seconds, bool)
        or not isinstance(seconds, int)
        or not 0 <= seconds <= _MAX_LOCK_TIMEOUT
    ):
        raise ValueError(
            "lock_timeout must be a whole number of seconds from 0 to "
            f"{_MAX_LOCK_TIMEOUT}, got {seconds!r}",
        )


def _sql_literal(value: str) -> str:
    quoted = value.replace("'", "''")
    if "\\" in value:
        # E'' reads a doubled backslash as one whatever
        # standard_conforming_strings is set to; a plain literal does not.
        return "E'" + quoted.replace("\\", "\\\\") + "'"
    return f"'{quoted}'"


def _lock_statement(table_name: str) -> str:
    # The key run_migrations takes (MigrationLockQuery), with the arguments
    # written as literals: a script has no bind parameters.
    return (
        "SELECT pg_advisory_xact_lock("
        f"hashtext({_sql_literal(MIGRATION_LOCK_NAMESPACE)}), "
        f"hashtext({_sql_literal(table_name)}))"
    )


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
    _check_table_name(name)
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
        ),
    ]
    if added_columns or added_indexes:
        # What the driver runs after the CREATEs, so a table an older release
        # created gets it too.
        steps.append(
            MigrationStep(
                number=2,
                statements=_added_statements(add_columns, added_columns, added_indexes),
            ),
        )
    return TableSchema(
        kind=kind,
        name=name,
        # ADD COLUMN appends, so the added columns come last in the table.
        columns=tuple(
            _column_schema(column) for column in (*create_table.columns, *added_columns)
        ),
        indexes=(
            *(
                IndexSchema(
                    name=index_name(name, column),
                    columns=(column.name,),
                    method="hash",
                )
                for column in index_columns
            ),
            *(
                IndexSchema(
                    name=index.name,
                    columns=tuple(column.name for column in index.columns),
                    method="btree",
                )
                for index in added_indexes
            ),
        ),
        steps=tuple(steps),
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
    if kind not in COMPONENT_KINDS:
        raise ValueError(
            f"unknown component kind {kind!r}; expected one of "
            f"{', '.join(COMPONENT_KINDS)}",
        )
    if field_for_task_id is not None and field_for_task_id not in TASK_ID_TYPES:
        raise ValueError(
            f"field_for_task_id must be one of {', '.join(TASK_ID_TYPES)}, "
            f"got {field_for_task_id!r}",
        )
    component_kind = cast("ComponentKind", kind)
    component_class = _component_classes()[component_kind]
    name = _default(component_class, "table_name") if table_name is None else table_name
    if component_kind == "scheduler_source":
        if field_for_task_id is not None:
            raise ValueError(
                "the scheduler source has no task_id column; "
                "field_for_task_id does not apply to it",
            )
        parts = _scheduler_source_parts()
    else:
        task_id_type = field_for_task_id or _default(
            component_class,
            "field_for_task_id",
        )
        parts = (
            _broker_parts(name, task_id_type)
            if component_kind == "broker"
            else _result_backend_parts(task_id_type)
        )
    return _schema(
        component_kind,
        CreateTableQuery(name, parts.columns),
        CreateIndexQuery(name),
        parts.index_columns,
        add_columns=AddColumnsQuery(name),
        added_columns=parts.added_columns,
        added_indexes=parts.added_indexes,
    )


class _TableParts(NamedTuple):
    """What a component hands its driver, for a table described by kind."""

    columns: list[Column]
    index_columns: list[Column]
    added_columns: Sequence[Column] = ()
    added_indexes: Sequence[AddedIndex] = ()


# Each builder below hands the driver what the component hands it: the
# CREATE TABLE columns in its order (primary key, columns, created_at), the
# hash-indexed columns, and the columns and indexes added after the CREATEs.
# test_a_kind_describes_the_same_table_as_a_component pins them to it.


def _broker_parts(table_name: str, task_id_type: str) -> _TableParts:
    from taskiq_postgresql.broker import Table  # noqa: PLC0415

    table = Table(task_id=Column(name="task_id", type_=task_id_type))
    claim_columns = ClaimColumns(channel=table.channel, not_before=table.not_before)
    return _TableParts(
        columns=[
            table.primary_key,
            table.task_id,
            table.task_name,
            table.message,
            table.labels,
            table.created_at,
        ],
        index_columns=[table.primary_key],
        added_columns=claim_columns,
        added_indexes=[
            AddedIndex(
                name=index_name(table_name, table.not_before),
                columns=[*claim_columns, table.primary_key],
            ),
        ],
    )


def _result_backend_parts(task_id_type: str) -> _TableParts:
    from taskiq_postgresql.result_backend import Table  # noqa: PLC0415

    table = Table(primary_key=PrimaryKeyColumn(name="task_id", type_=task_id_type))
    # The result backend leaves created_at to the driver's default.
    return _TableParts(
        columns=[table.primary_key, table.result, CreatedAtColumn()],
        index_columns=[table.primary_key],
    )


def _scheduler_source_parts() -> _TableParts:
    from taskiq_postgresql.scheduler_source import Table  # noqa: PLC0415

    table = Table()
    return _TableParts(
        columns=[
            table.primary_key,
            table.task_name,
            table.schedule,
            table.updated_at,
            table.created_at,
        ],
        index_columns=[table.primary_key, table.task_name],
    )


def _collect(
    targets: Sequence[object],
    app_dir: Optional[str],
) -> list[tuple[Component, TableSchema]]:
    found: dict[str, tuple[Component, TableSchema]] = {}
    for target in targets:
        loaded = _load_target(target, app_dir) if isinstance(target, str) else target
        components = _components_in(loaded)
        if not components:
            raise LookupError(
                f"{_describe(target)} holds no taskiq-postgresql component: "
                "expected a component, or a broker or TaskiqScheduler that "
                "uses one",
            )
        for component in components:
            schema = table_schema(component)
            _, kept = found.setdefault(schema.name, (component, schema))
            if kept != schema:
                raise ValueError(
                    f"two components use the table {schema.name!r} with "
                    "different columns",
                )
    return list(found.values())


def _describe(target: object) -> str:
    return repr(target) if isinstance(target, str) else type(target).__name__


def _load_target(spec: str, app_dir: Optional[str]) -> object:
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute or ":" in attribute:
        raise ValueError(f"{spec!r} is not a 'module:object' path")
    # As taskiq worker does: app_dir stays on sys.path, the working directory
    # only while the module is imported.
    if app_dir is not None and app_dir not in sys.path:
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
