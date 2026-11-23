"""
Alembic support for the tables of taskiq-postgresql.

Install it with ``pip install 'taskiq-postgresql[alembic]'``. Importing this
module teaches Alembic's autogenerate the package's migration steps:

- ``taskiq_metadata(...)`` in ``target_metadata`` (or ``add_taskiq_tables``
  on your own metadata): autogenerate reads which steps the database lacks
  and writes each one into the revision as a ``TaskiqStepOp``, with the
  step's SQL spelled out, in place of the generic ``create_table``,
  ``add_column`` and ``create_index`` it would write for these tables. On a
  database that has every step, it writes nothing; ``alembic check`` fails
  while a step is missing.
- ``exclude_taskiq_tables(...)`` as ``include_object``: autogenerate leaves the
  tables alone, for projects whose workers keep ``run_migrations=True``.
- ``upgrade_taskiq_tables(op, ..., through=N)`` in a hand-written
  ``upgrade()``, for table names that differ between environments.
- ``taskiq postgresql generate --format alembic``, which writes a revision
  file with every step in it.

The steps share Alembic's transaction with the revision's other migrations,
so they bound their own lock waits, as ``generate``'s script does
(``DEFAULT_LOCK_TIMEOUT`` seconds unless configured, none with ``0``), and
then give the transaction back the ``lock_timeout`` it had.

The comparator registers itself globally on import, as Alembic's
autogenerate hooks always have; it does nothing unless a metadata in
``target_metadata`` holds the descriptions ``taskiq_metadata`` and
``add_taskiq_tables`` record. No ``alembic.plugins`` entry point is
published: before Alembic 1.20.1 one breaks ``import alembic``.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Sequence
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Final, Optional, Union

try:
    from alembic.autogenerate import comparators, renderers
    from alembic.operations import MigrateOperation, Operations, ops
except ImportError as error:
    raise ImportError(
        "alembic is not installed. Please install it with "
        "`pip install taskiq-postgresql[alembic]`.",
    ) from error

from sqlalchemy import MetaData, Table, text

from taskiq_postgresql.contrib.sqlalchemy import (
    METADATA_INFO_KEY,
    _name,
    _schemas,
    taskiq_metadata,
)
from taskiq_postgresql.migrations import (
    COMPONENT_KINDS,
    DEFAULT_LOCK_TIMEOUT,
    CatalogState,
    ComponentKind,
    FormatOptions,
    MigrationFormatOptionError,
    MigrationStep,
    PendingStep,
    TableSchema,
    _check_lock_timeout,
    _check_table_name,
    _scoped_lock_timeout,
    _script_statements,
    catalog_query,
    migration_statements,
    plan_migration,
)

if TYPE_CHECKING:
    from types import ModuleType

    from alembic.autogenerate.api import AutogenContext
    from sqlalchemy.schema import SchemaItem

__all__ = [
    "DIFF_NAME",
    "LOCK_TIMEOUT_OPTION",
    "TaskiqStepNoopOp",
    "TaskiqStepOp",
    "alembic_format",
    "compare_taskiq_steps",
    "exclude_taskiq_tables",
    "taskiq_metadata",
    "upgrade_taskiq_tables",
]

log = logging.getLogger(__name__)

DIFF_NAME: Final = "taskiq_postgresql_step"
"""The first item of a ``TaskiqStepOp``'s diff tuple, as ``alembic check`` shows it."""

LOCK_TIMEOUT_OPTION: Final = "taskiq_postgresql_lock_timeout"
"""
The ``context.configure(...)`` keyword that sets the steps' lock timeout.

Autogenerate writes it into each step of the revision: the seconds a
statement of the step waits for a lock, ``DEFAULT_LOCK_TIMEOUT`` when it is
not given, ``0`` for no bound of the step's own.
"""

_OPTIONS: Final = ("revision", "down_revision", "downgrade")
_DOWNGRADES: Final = ("none", "drop")
_REVISION_ID: Final = re.compile(r"[A-Za-z0-9_]+")
_DEFAULT_REVISION_LENGTH: Final = 12
_INDENT: Final = "    "
_TRANSACTION_NOTE: Final = (
    "# The lock and the bound need the revision's transaction: never run this "
    "step in an autocommit block."
)
_REVISION: Final = '''\
"""Create or complete the taskiq-postgresql tables.

Revision ID: {revision}
Revises:{revises}
Written by `taskiq postgresql generate --format alembic` (taskiq-postgresql
{version}). Each statement only adds what is missing, so upgrade() is safe on
a database that already has the tables. The SQL is written out, so the
revision never imports taskiq-postgresql.
"""

from alembic import op

revision = {revision_literal}
down_revision = {down_revision_literal}
branch_labels = None
depends_on = None


def upgrade() -> None:
{upgrade}


def downgrade() -> None:
{downgrade}
'''


class TaskiqStepOp(MigrateOperation):
    """
    One migration step of one table, as an autogenerate operation.

    It renders as ``op.execute(...)`` calls with the step's SQL written out,
    so the revision runs the same statements ``run_migrations=True`` runs,
    and never imports taskiq-postgresql. It is not registered as an
    ``op.<name>`` directive for the same reason.

    The step bounds its own lock waits: it sets ``lock_timeout`` first, and
    its last statement gives the transaction back the ``lock_timeout`` it
    had, so the revision's other migrations wait for locks as before.

    Args:
        table: The table name.
        kind: The component that owns the table.
        step: The step.
        lock: Start with the advisory lock ``run_migrations=True`` takes, so
            starting workers wait for the revision's transaction.
        lock_timeout: Seconds a statement of the step waits for a lock, the
            advisory lock included, before it fails and Alembic's
            transaction rolls back. ``0`` sets none: the transaction's own
            ``lock_timeout`` applies.
        notes: The locks the step takes, written as comments.

    Raises:
        ValueError: if ``table`` is empty or holds a line break or a NUL,
            ``kind`` is not one of ``COMPONENT_KINDS``, the step number is
            not positive, or ``lock_timeout`` is not a whole number of
            seconds from 0 to 2147483.
        TypeError: if the step number is not an ``int``.
    """

    def __init__(
        self,
        table: str,
        kind: ComponentKind,
        step: MigrationStep,
        *,
        lock: bool = True,
        lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
        notes: Sequence[str] = (),
    ) -> None:
        _check_lock_timeout(lock_timeout)
        # table, kind and number are written into comments of the revision
        # file: a line break in one of them would put code into it.
        _check_table_name(table)
        if kind not in COMPONENT_KINDS:
            raise ValueError(
                f"unknown component kind {kind!r}; expected one of "
                f"{', '.join(COMPONENT_KINDS)}",
            )
        if type(step.number) is not int:
            raise TypeError(
                f"the step number must be an int, got {type(step.number).__name__}",
            )
        if step.number < 1:
            raise ValueError(f"the step number must be positive, got {step.number}")
        self.table = table
        self.kind = kind
        self.step = step
        self.lock = lock
        self.lock_timeout = lock_timeout
        self.notes = tuple(notes)

    @classmethod
    def from_pending(
        cls,
        pending: PendingStep,
        *,
        lock: bool = True,
        lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
    ) -> TaskiqStepOp:
        """
        Build the operation of a step ``plan_migration`` found pending.

        Args:
            pending: The pending step.
            lock: As for the constructor.
            lock_timeout: As for the constructor.

        Returns:
            The operation.

        Raises:
            ValueError: as the constructor does.
            TypeError: as the constructor does.
        """
        return cls(
            pending.table,
            pending.kind,
            pending.step,
            lock=lock,
            lock_timeout=lock_timeout,
            notes=pending.notes,
        )

    @property
    def number(self) -> int:
        """The step number."""
        return self.step.number

    @property
    def statements(self) -> list[str]:
        """The statements the revision executes, the lock timeout and lock first."""
        return _scoped_lock_timeout(
            _script_statements([(self.table, (self.step,))], lock=self.lock),
            self.lock_timeout,
        )

    def reverse(self) -> TaskiqStepNoopOp:
        """
        Return the downgrade: nothing, because every step is additive.

        Returns:
            The operation that renders the comment.
        """
        return TaskiqStepNoopOp(self)

    def to_diff_tuple(self) -> tuple[str, str, int]:
        """
        Return the diff ``alembic check`` and ``compare_metadata`` report.

        Returns:
            ``("taskiq_postgresql_step", table, step number)``.
        """
        return (DIFF_NAME, self.table, self.number)


class TaskiqStepNoopOp(MigrateOperation):
    """
    The downgrade of a ``TaskiqStepOp``: a comment, and no SQL.

    A step only adds columns and indexes, which the previous release ignores,
    and dropping them would lose data.

    Args:
        upgrade: The operation it undoes.
    """

    def __init__(self, upgrade: TaskiqStepOp) -> None:
        self.upgrade = upgrade

    def reverse(self) -> TaskiqStepOp:
        """
        Return the upgrade operation.

        Returns:
            The ``TaskiqStepOp``.
        """
        return self.upgrade

    def to_diff_tuple(self) -> tuple[str, str, int]:
        """
        Return the diff of the upgrade it belongs to.

        Returns:
            ``("taskiq_postgresql_step_downgrade", table, step number)``.
        """
        return (f"{DIFF_NAME}_downgrade", self.upgrade.table, self.upgrade.number)


@renderers.dispatch_for(TaskiqStepOp)
def _render_step(autogen_context: AutogenContext, op: TaskiqStepOp) -> list[str]:
    return _step_lines(op, _prefix(autogen_context))


@renderers.dispatch_for(TaskiqStepNoopOp)
def _render_noop(autogen_context: AutogenContext, op: TaskiqStepNoopOp) -> list[str]:
    return _noop_lines(op.upgrade)


def compare_taskiq_steps(
    autogen_context: AutogenContext,
    upgrade_ops: ops.UpgradeOps,
    schemas: set[Optional[str]],
) -> None:
    """
    Put the missing migration steps of the taskiq tables into a revision.

    Registered as a schema-level autogenerate comparator when this module is
    imported. It reads the catalog with one read-only query
    (``catalog_query``), replaces the ``create_table``, ``add_column`` and
    ``create_index`` operations Alembic generated for objects of the pending
    steps with one ``TaskiqStepOp`` per step, and keeps every other
    operation, so a type change or a stray column still shows. Problems no
    step fixes, such as an invalid index, are logged as warnings.

    Each step gets the lock timeout ``context.configure(...)`` sets under
    ``LOCK_TIMEOUT_OPTION`` in ``env.py``, ``DEFAULT_LOCK_TIMEOUT`` without
    it.

    It does nothing on a dialect other than PostgreSQL, when the default
    schema is not compared, or when no metadata of ``target_metadata`` holds
    descriptions under ``METADATA_INFO_KEY``. ``include_name`` and
    ``include_object`` apply to the tables.

    Args:
        autogen_context: Alembic's autogenerate context.
        upgrade_ops: The operations of the revision being generated.
        schemas: The schema names being compared; ``None`` is the default.

    Raises:
        ValueError: if the configured lock timeout is not a whole number of
            seconds from 0 to 2147483.
    """
    dialect = autogen_context.dialect
    connection = autogen_context.connection
    if dialect is None or dialect.name != "postgresql" or None not in schemas:
        return
    tables = _described_tables(autogen_context)
    if not tables or connection is None:
        return
    # Checked before the catalog is read, so a wrong value fails every run,
    # not only the ones that find a step missing.
    lock_timeout = autogen_context.opts.get(LOCK_TIMEOUT_OPTION, DEFAULT_LOCK_TIMEOUT)
    _check_lock_timeout(lock_timeout)
    found = connection.execute(text(_escape_colons(catalog_query(tables)))).scalar()
    plan = plan_migration(tables, CatalogState.from_json(found))
    for problem in plan.problems:
        log.warning("taskiq-postgresql: %s: %s", problem.code, problem.detail)
    _drop_covered(upgrade_ops, plan.pending)
    present = {
        operation.to_diff_tuple()
        for operation in upgrade_ops.ops
        if isinstance(operation, TaskiqStepOp)
    }
    upgrade_ops.ops.extend(
        operation
        for operation in (
            TaskiqStepOp.from_pending(pending, lock_timeout=lock_timeout)
            for pending in plan.pending
        )
        if operation.to_diff_tuple() not in present
    )


def _register(comparator: Callable[..., None]) -> None:
    try:
        from alembic.util import DispatchPriority  # noqa: PLC0415
    except ImportError:
        # Before 1.18 comparators run in registration order, and Alembic
        # registered its table comparator when it was imported. A "postgresql"
        # qualifier would hide that one: dispatch returns the qualifier's list
        # or the default's, never both.
        comparators.dispatch_for("schema")(comparator)
        return
    # From 1.18 global comparators run before the built-in ones unless they
    # ask to run last, and this one edits what the table comparator emits.
    comparators.dispatch_for("schema", priority=DispatchPriority.LAST)(comparator)


_register(compare_taskiq_steps)


def exclude_taskiq_tables(
    *targets: object,
    app_dir: Optional[str] = None,
) -> Callable[[SchemaItem, Optional[str], str, bool, Optional[SchemaItem]], bool]:
    """
    Return an ``include_object`` hook that hides the tables from autogenerate.

    Pass it to ``context.configure(include_object=...)`` in ``env.py`` when
    the tables stay out of ``target_metadata``, for example because the
    workers create them with ``run_migrations=True``: without it, every
    autogenerated revision drops them. The hook hides the tables, their
    columns, constraints and indexes in the default schema. Alembic does not
    compare sequences, so the broker's ``id`` sequence needs no filter. To keep
    a hook of your own, call both and include an object only when both do.

    Args:
        *targets: As for ``taskiq_metadata``.
        app_dir: As for ``taskiq_metadata``.

    Returns:
        The hook: it returns ``False`` for the objects of these tables, and
        ``True`` for everything else.

    Raises:
        TypeError: as ``taskiq_metadata`` does.
        ValueError: as ``taskiq_metadata`` does.
        LookupError: as ``taskiq_metadata`` does.
        ImportError: as ``taskiq_metadata`` does.
        AttributeError: as ``taskiq_metadata`` does.
    """
    names = frozenset(taskiq_metadata(*targets, app_dir=app_dir).tables)

    def include_object(
        item: SchemaItem,
        name: Optional[str],
        type_: str,
        reflected: bool,
        compare_to: Optional[SchemaItem],
    ) -> bool:
        table = item if type_ == "table" else getattr(item, "table", None)
        ours = isinstance(table, Table) and table.schema is None and table.name in names
        return not ours

    return include_object


def upgrade_taskiq_tables(
    op: Union[Operations, ModuleType],
    *targets: object,
    through: int,
    app_dir: Optional[str] = None,
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
) -> None:
    """
    Run the migration steps from a hand-written revision's ``upgrade()``.

    It runs steps 1 to ``through`` of each table, one statement per call
    (SQLAlchemy's asyncpg dialect refuses several in one), in the revision's
    transaction, so the advisory lock ``run_migrations=True`` takes lasts
    until Alembic commits. A released step never changes, so the revision
    runs the same SQL with every later release of the package. Every
    statement only adds what is missing. With ``alembic upgrade --sql`` the
    statements are printed instead.

    The statements bound their own lock waits: ``lock_timeout`` is set
    before them, and the transaction gets back the value it had after them,
    so the revision's other migrations wait for locks as before. Both the
    bound and the advisory lock last only inside a transaction, so online it
    refuses an autocommit connection, such as Alembic's
    ``autocommit_block()``, before any statement runs.

    Use it where an autogenerated revision would write the wrong table name:
    when the name comes from settings that differ between environments.

    Args:
        op: ``alembic.op``, as a revision imports it, or an ``Operations``.
        *targets: As for ``taskiq_metadata``. ``table_schema(...)``
            descriptions keep the revision from importing the application.
        through: The last step to run. Every table must have it: name the
            tables of different step counts in separate calls.
        app_dir: As for ``taskiq_metadata``.
        lock_timeout: Seconds a statement waits for a lock, the advisory lock
            included, before it fails and Alembic's transaction rolls back.
            ``0`` sets none: the transaction's own ``lock_timeout`` applies.

    Raises:
        TypeError: as ``taskiq_metadata`` does.
        ValueError: if a table has no step ``through``, ``lock_timeout`` is
            not a whole number of seconds from 0 to 2147483, or as
            ``taskiq_metadata`` does. Nothing runs then.
        LookupError: as ``taskiq_metadata`` does.
        ImportError: as ``taskiq_metadata`` does.
        AttributeError: as ``taskiq_metadata`` does.
        RuntimeError: if the connection is in autocommit mode and Alembic is
            not printing SQL (``--sql``). Nothing runs then.
    """
    statements = _scoped_lock_timeout(
        migration_statements(_schemas(targets, app_dir), through=through),
        lock_timeout,
    )
    _require_transaction(op)
    for statement in statements:
        op.execute(_escape_colons(statement))


def alembic_format(schemas: Sequence[TableSchema], options: FormatOptions) -> str:
    """
    Render the ``alembic`` migration format: a revision file.

    ``upgrade()`` is what autogenerate writes for these tables on an empty
    database: each step of each table, its SQL written out, without ``BEGIN``
    and ``COMMIT`` (Alembic runs each revision in a transaction of its own,
    so ``options.transaction`` changes nothing). ``options.lock`` keeps or
    leaves out the advisory locks, and ``options.lock_timeout`` is each
    step's own bound on lock waits (see ``TaskiqStepOp``).

    Options (``--option KEY=VALUE``):

    - ``revision``: the revision id, letters, digits and ``_``; by default the
      first 12 hex digits of the SHA-256 of the statements, so the same tables
      give the same id.
    - ``down_revision``: the revision it follows, your current head; by
      default none, a first revision.
    - ``downgrade``: ``none`` (default), ``downgrade()`` leaves the tables in
      place; or ``drop``, it drops them with ``DROP TABLE IF EXISTS``.

    Args:
        schemas: The tables.
        options: The options of ``taskiq postgresql generate``.

    Returns:
        The revision file.

    Raises:
        MigrationFormatOptionError: if an option is unknown or its value is
            not one the format accepts.
        ValueError: if a table name is not a plain SQL identifier (the name is
            written unquoted), or as ``TaskiqStepOp`` does.
        TypeError: as ``TaskiqStepOp`` does.
    """
    unknown = [key for key in options.extra if key not in _OPTIONS]
    if unknown:
        raise MigrationFormatOptionError(
            f"the alembic format takes {', '.join(_OPTIONS)}; unknown "
            f"option{'s' if len(unknown) > 1 else ''}: {', '.join(unknown)}",
        )
    downgrade = options.extra.get("downgrade", "none")
    if downgrade not in _DOWNGRADES:
        raise MigrationFormatOptionError(
            f"downgrade must be 'none' or 'drop', got {downgrade!r}",
        )
    for schema in schemas:
        # The SQLAlchemy path's rule: the steps and the drop write the name
        # unquoted, so "a, users" would name two tables.
        _name(schema.name)
    steps = [
        TaskiqStepOp(
            schema.name,
            schema.kind,
            step,
            lock=options.lock,
            lock_timeout=options.lock_timeout,
        )
        for schema in sorted(schemas, key=lambda table: table.name)
        for step in schema.steps
    ]
    revision = options.extra.get("revision")
    if revision is None:
        upgrade = [statement for step in steps for statement in step.statements]
        digest = hashlib.sha256("\n".join(upgrade).encode()).hexdigest()
        revision = digest[:_DEFAULT_REVISION_LENGTH]
    down_revision = options.extra.get("down_revision")
    for key, value in (("revision", revision), ("down_revision", down_revision)):
        if value is not None and _REVISION_ID.fullmatch(value) is None:
            raise MigrationFormatOptionError(
                f"{key} must be letters, digits and _, got {value!r}",
            )
    if downgrade == "drop":
        drops = [f"DROP TABLE IF EXISTS {schema.name}" for schema in reversed(schemas)]
        downgrade_lines = [
            _execute_line("op.", statement)
            for statement in _scoped_lock_timeout(drops, options.lock_timeout)
        ]
    else:
        downgrade_lines = [
            line for step in reversed(steps) for line in _noop_lines(step)
        ]
    return _REVISION.format(
        revision=revision,
        revises="" if down_revision is None else f" {down_revision}",
        version=_package_version(),
        revision_literal=f'"{revision}"',
        down_revision_literal="None" if down_revision is None else f'"{down_revision}"',
        upgrade=_body([line for step in steps for line in _step_lines(step, "op.")]),
        downgrade=_body(downgrade_lines),
    )


def _described_tables(autogen_context: AutogenContext) -> list[TableSchema]:
    metadata = autogen_context.metadata
    if metadata is None:
        collections: Sequence[MetaData] = ()
    elif isinstance(metadata, MetaData):
        collections = (metadata,)
    else:
        collections = tuple(metadata)
    described: dict[str, TableSchema] = {}
    for collection in collections:
        for schema in collection.info.get(METADATA_INFO_KEY, ()):
            described.setdefault(schema.name, schema)
    return [
        schema
        for schema in described.values()
        if _included(autogen_context, collections, schema)
    ]


def _included(
    autogen_context: AutogenContext,
    collections: Sequence[MetaData],
    schema: TableSchema,
) -> bool:
    name = _name(schema.name)
    if not autogen_context.run_name_filters(name, "table", {"schema_name": None}):
        return False
    for collection in collections:
        table = collection.tables.get(name)
        if table is not None:
            return autogen_context.run_object_filters(table, name, "table", False, None)
    return True


def _drop_covered(upgrade_ops: ops.UpgradeOps, pending: Sequence[PendingStep]) -> None:
    # Alembic's own comparators ran first and proposed generic operations for
    # the objects the pending steps create; the steps replace exactly those.
    created = {_name(item.table) for item in pending if item.table_missing}
    columns = {
        (_name(item.table), column)
        for item in pending
        for column in item.step.objects.columns
    }
    indexes = {
        (_name(item.table), _name(index.name))
        for item in pending
        for index in item.step.objects.indexes
    }

    def covered(operation: MigrateOperation) -> bool:
        if isinstance(operation, ops.AddColumnOp):
            return (operation.table_name, operation.column.name) in columns
        if isinstance(operation, ops.CreateIndexOp):
            return (operation.table_name, operation.index_name) in indexes
        return False

    kept: list[MigrateOperation] = []
    for operation in upgrade_ops.ops:
        if getattr(operation, "schema", None) is not None:
            kept.append(operation)
            continue
        if isinstance(operation, ops.CreateTableOp) and operation.table_name in created:
            continue
        if isinstance(operation, ops.ModifyTableOps):
            operation.ops = [inner for inner in operation.ops if not covered(inner)]
            if not operation.ops:
                continue
        elif covered(operation):
            continue
        kept.append(operation)
    upgrade_ops.ops[:] = kept


def _require_transaction(op: Union[Operations, ModuleType]) -> None:
    # Offline there is no connection: the printed script has its own BEGIN.
    if op.get_context().as_sql:
        return
    connection = op.get_bind()
    # Connection.get_isolation_level() never reports AUTOCOMMIT, so read what
    # Alembic's autocommit_block() and the engine option set: the execution
    # option, and the DB-API connection's own flag.
    autocommit = connection.get_execution_options().get(
        "isolation_level",
    ) == "AUTOCOMMIT" or bool(
        getattr(connection.connection.dbapi_connection, "autocommit", False),
    )
    if autocommit:
        raise RuntimeError(
            "the taskiq-postgresql steps need the revision's transaction: the "
            "advisory lock and the lock_timeout bound last only inside one, "
            "and this connection is in autocommit mode (an autocommit_block() "
            "or an AUTOCOMMIT isolation level); call upgrade_taskiq_tables "
            "outside the autocommit block",
        )


def _prefix(autogen_context: AutogenContext) -> str:
    return autogen_context.opts.get("alembic_module_prefix", "op.") or ""


def _step_lines(step: TaskiqStepOp, prefix: str) -> list[str]:
    title = f"step {step.number} of the {step.kind} table {step.table}"
    bound = (
        f"# Lock waits end after {step.lock_timeout} s; the last statement "
        "restores the transaction's lock timeout."
    )
    return [
        f"# taskiq-postgresql {title}: the SQL run_migrations=True runs,",
        "# and each statement only adds what is missing.",
        _TRANSACTION_NOTE,
        *(f"# {note}." for note in step.notes),
        *([bound] if step.lock_timeout else []),
        *(_execute_line(prefix, statement) for statement in step.statements),
    ]


def _noop_lines(step: TaskiqStepOp) -> list[str]:
    title = f"step {step.number} of {step.table}"
    # A body of comments alone is not valid Python: pass makes it a statement.
    return [f"# taskiq-postgresql {title} is additive: it stays.", "pass"]


def _execute_line(prefix: str, statement: str) -> str:
    return f"{prefix}execute({_escape_colons(statement)!r})"


def _escape_colons(statement: str) -> str:
    # op.execute() and text() read ":name" as a bind parameter; "\:" is a
    # literal colon, in online and offline (--sql) mode alike.
    return statement.replace(":", "\\:")


def _body(lines: Sequence[str]) -> str:
    return "\n".join(f"{_INDENT}{line}" for line in (lines or ["pass"]))


def _package_version() -> str:
    try:
        return version("taskiq-postgresql")
    except PackageNotFoundError:
        # A source tree that was never installed has no metadata.
        return "unknown"
