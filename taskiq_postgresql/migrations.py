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
read-only query, and ``plan_migration`` tells which steps are still pending
and what is wrong in a way no step fixes.

Importing this module imports no driver library.
"""

from __future__ import annotations

import json
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
    "collect_components",
    "collect_schemas",
    "load_format",
    "migration_statements",
    "plan_migration",
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

Severity = Literal["required", "advisory"]
"""
How much a gap matters: without a ``required`` object (a table or a column)
the components fail; without an ``advisory`` one (an index) they only slow
down.
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

# The server keeps lock_timeout as an int of milliseconds and refuses a larger
# value when the script runs.
_MAX_LOCK_TIMEOUT: Final = 2_147_483
# A custom setting (two-part name) that holds a transaction's own lock_timeout
# while steps that share the transaction run with theirs.
_SAVED_LOCK_TIMEOUT: Final = "taskiq_postgresql_step.lock_timeout"
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
    """

    creates_table: bool
    columns: tuple[str, ...]
    indexes: tuple[IndexSchema, ...]


@dataclass(frozen=True)
class MigrationStep:
    """
    One additive, idempotent step of a table's migration.

    Steps run in ``number`` order and each one is idempotent, so the whole
    list brings a table of any earlier release up to date. Idempotent is not
    lock-free: on a table that already exists, ``ALTER TABLE`` and
    ``CREATE INDEX`` lock it before they find that nothing is missing. A
    released step never changes: a later release adds a step instead.

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
    tables = list(schemas)
    if not tables:
        raise ValueError("a migration script needs at least one table schema")
    for table in tables:
        _check_table_name(table.name)
    return _script_statements(
        [(table.name, _steps_through(table, through)) for table in tables],
        lock=lock,
    )


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
    """

    name: str
    relkind: Optional[str] = None
    columns: Mapping[str, str] = field(default_factory=dict)
    indexes: Mapping[str, IndexState] = field(default_factory=dict)


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
                tables[table["name"]] = TableState(
                    name=table["name"],
                    relkind=table["relkind"],
                    columns=dict(table["columns"] or ()),
                    indexes=indexes,
                )
        except (TypeError, KeyError, ValueError) as error:
            raise ValueError(
                f"not the result of catalog_query: {error!r}",
            ) from error
        return cls(tables=tables)


@dataclass(frozen=True)
class PendingStep:
    """
    A step that has not fully run on a table: some object it creates is absent.

    Running it again is safe: every statement only adds what is missing.

    Attributes:
        table: The table name.
        kind: The component that owns the table.
        step: The step.
        table_missing: Whether the table itself is absent.
        missing_columns: The step's columns the table lacks.
        missing_indexes: The step's indexes that no relation is named after.
    """

    table: str
    kind: ComponentKind
    step: MigrationStep
    table_missing: bool
    missing_columns: tuple[str, ...]
    missing_indexes: tuple[str, ...]

    @property
    def severity(self) -> Severity:
        """``required`` when the table or a column is missing, else ``advisory``."""
        if self.table_missing or self.missing_columns:
            return "required"
        return "advisory"

    @property
    def notes(self) -> tuple[str, ...]:
        """The locks the step takes on a table that already exists."""
        if self.table_missing:
            return ()
        notes = []
        if self.step.objects.columns and not self.step.objects.creates_table:
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

    def statements(self, *, lock: bool = True) -> list[str]:
        """
        Return the statements that run the step.

        Args:
            lock: Start with the advisory lock ``run_migrations=True`` takes.

        Returns:
            The statements, without the trailing semicolon.
        """
        return _script_statements([(self.table, (self.step,))], lock=lock)


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
        grouped: dict[str, list[MigrationStep]] = {}
        for item in self.pending:
            grouped.setdefault(item.table, []).append(item.step)
        return _script_statements(list(grouped.items()), lock=lock)

    def render_sql(
        self,
        *,
        transaction: bool = True,
        lock: bool = True,
        lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
    ) -> str:
        """
        Render the script that runs the pending steps.

        The script is only right for the database the plan was computed
        from, at that moment; every statement still only adds what is missing.

        Args:
            transaction: Wrap the script in ``BEGIN`` and ``COMMIT``.
            lock: Take the advisory locks, as ``render_sql`` does.
            lock_timeout: Seconds a statement waits for a lock, as for
                ``render_sql``; ``0`` leaves the statement out.

        Returns:
            The script, as ``render_sql`` lays it out, or an empty string when
            nothing is pending.

        Raises:
            ValueError: if ``lock_timeout`` is not a whole number of seconds
                from 0 to 2147483.
        """
        statements = _lock_timeout_first(self.statements(lock=lock), lock_timeout)
        if not self.pending:
            return ""
        pending = ", ".join(
            f"{item.table} step {item.step.number}" for item in self.pending
        )
        return _script_text(
            [f"-- pending: {pending}"],
            statements,
            transaction=transaction,
        )


def catalog_query(schemas: Iterable[TableSchema]) -> str:
    """
    Return the read-only query that reads what a database holds for the tables.

    The query has no parameters: the names are written in it as literals, so
    every driver runs it as it is. It returns one row with one text column,
    ``catalog``, which ``CatalogState.from_json`` reads. Names resolve through
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
    tables = list(schemas)
    if not tables:
        raise ValueError("a catalog query needs at least one table schema")
    for table in tables:
        _check_table_name(table.name)
    return (
        "SELECT json_build_object('tables', json_build_array("
        + ", ".join(_table_state_sql(table) for table in tables)
        + "))::text AS catalog"
    )


def plan_migration(
    schemas: Iterable[TableSchema],
    state: CatalogState,
) -> MigrationPlan:
    """
    Compare a database with the described tables.

    A step is pending when an object it creates is absent: then running it
    again completes it. An object that exists in another shape than the
    description, or under a name another object took, is a problem instead,
    because ``IF NOT EXISTS`` would skip it: nothing runs DDL for it.

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


def _package_version() -> str:
    try:
        return version(_DISTRIBUTION)
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


def _script_statements(
    tables: Sequence[tuple[str, Sequence[MigrationStep]]],
    *,
    lock: bool,
) -> list[str]:
    statements: list[str] = []
    if lock:
        # Sorted, so two scripts that lock the same tables take the locks in
        # the same order and cannot deadlock each other.
        statements.extend(
            _lock_statement(name) for name in sorted({name for name, _ in tables})
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
        statements = ["BEGIN", *statements, "COMMIT"]
    lines = [
        f"-- taskiq-postgresql {_package_version()}",
        *header,
        *(f"{statement};" for statement in statements),
    ]
    return "\n".join(lines) + "\n"


def _lock_timeout_first(statements: Sequence[str], lock_timeout: int) -> list[str]:
    _check_lock_timeout(lock_timeout)
    if not lock_timeout:
        return list(statements)
    # First, so that it also bounds the wait for the advisory lock.
    return [f"SET LOCAL lock_timeout = '{lock_timeout}s'", *statements]


def _scoped_lock_timeout(statements: Sequence[str], lock_timeout: int) -> list[str]:
    """
    Bound the lock waits of statements that share a transaction with others.

    The statements run with ``lock_timeout``; those after them, such as the
    rest of an Alembic revision, get back the value the transaction had.
    ``SET LOCAL lock_timeout TO DEFAULT`` would not do that: ``DEFAULT`` is
    "whatever value it would have had if no SET had been executed in the
    current session" (https://www.postgresql.org/docs/current/sql-set.html),
    so a value the session or the transaction set would be lost.
    """
    bounded = _lock_timeout_first(statements, lock_timeout)
    if not lock_timeout:
        return bounded
    saved = _sql_literal(_SAVED_LOCK_TIMEOUT)
    # missing_ok and NULLIF turn a value this transaction never saved (missing
    # or empty) into NULL, which set_config reads as a reset, instead of an
    # error.
    restore = (
        "SELECT set_config('lock_timeout', "
        f"NULLIF(current_setting({saved}, true), ''), true)"
    )
    return [
        f"SELECT set_config({saved}, current_setting('lock_timeout'), true)",
        *bounded,
        restore,
    ]


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


def _table_state_sql(table: TableSchema) -> str:
    # Names go in as escaped literals: the query has no bind parameters, so
    # every driver runs it the same way.
    name = _sql_literal(table.name)
    indexes = ", ".join(_index_state_sql(index.name) for index in table.indexes)
    return (
        "(SELECT json_build_object("  # noqa: S608
        f"'name', {name}, "
        "'relkind', c.relkind, "
        "'columns', (SELECT json_agg(json_build_array(a.attname,"
        " format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)"
        " FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0"
        " AND NOT a.attisdropped), "
        f"'indexes', json_build_array({indexes})) "
        f"FROM (SELECT to_regclass({name}) AS oid) AS t "
        "LEFT JOIN pg_class c ON c.oid = t.oid)"
    )


def _index_state_sql(index_name: str) -> str:
    # Correlated with the table's pg_class row (c) of _table_state_sql.
    name = _sql_literal(index_name)
    return (
        "(SELECT json_build_object("  # noqa: S608
        f"'name', {name}, "
        "'relkind', ic.relkind, "
        "'on_table', i.indrelid IS NOT DISTINCT FROM c.oid, "
        "'valid', i.indisvalid, "
        "'method', am.amname, "
        "'unique', i.indisunique, "
        "'partial', i.indpred IS NOT NULL, "
        "'columns', (SELECT json_agg(a.attname ORDER BY k.n)"
        " FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, n)"
        " LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid"
        " AND a.attnum = k.attnum WHERE k.n <= i.indnkeyatts)) "
        f"FROM (SELECT to_regclass({name}) AS oid) AS r "
        "LEFT JOIN pg_class ic ON ic.oid = r.oid "
        "LEFT JOIN pg_index i ON i.indexrelid = r.oid "
        "LEFT JOIN pg_am am ON am.oid = ic.relam)"
    )


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
        if missing_columns or missing_indexes:
            pending.append(
                PendingStep(
                    table=schema.name,
                    kind=schema.kind,
                    step=step,
                    table_missing=False,
                    missing_columns=missing_columns,
                    missing_indexes=missing_indexes,
                ),
            )
    problems.extend(
        SchemaProblem(
            table=schema.name,
            code="column_type",
            severity="advisory",
            detail=(
                f"{schema.name}.{column.name} is {table.columns[column.name]}, "
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
    _check_table_name(name)
    created_indexes = tuple(
        IndexSchema(
            name=index_name(name, column),
            columns=(column.name,),
            method="hash",
        )
        for column in index_columns
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
    added = tuple(
        IndexSchema(
            name=index.name,
            columns=tuple(column.name for column in index.columns),
            method="btree",
        )
        for index in added_indexes
    )
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
    component_kind: ComponentKind = kind
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
