"""
The SQL catalog: every statement and script the package sends (ADR 0007).

Each builder keeps its SQL text in one private method. ``make_query`` returns
that text untyped, so a third-party driver that passes a column no
``ParamType`` covers keeps working; ``statement`` (or ``script`` for DDL) wraps
the same text with the types the server infers for it.
"""

from typing import Any, Final, Literal, NamedTuple, Optional, Sequence, Union

from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.sql.statement import (
    ParamType,
    Script,
    Statement,
    param_type,
)

# The server keeps lock_timeout as an int of milliseconds and refuses a larger
# value when the script runs.
MAX_LOCK_TIMEOUT: Final = 2_147_483
# A custom setting (two-part name) that holds a transaction's own lock_timeout
# while steps that share the transaction run with theirs.
SAVED_LOCK_TIMEOUT: Final = "taskiq_postgresql_step.lock_timeout"

_TEXT: Final = ParamType.TEXT


def _sql_literal(value: str) -> str:
    quoted = value.replace("'", "''")
    if "\\" in value:
        # E'' reads a doubled backslash as one whatever
        # standard_conforming_strings is set to; a plain literal does not.
        return "E'" + quoted.replace("\\", "\\\\") + "'"
    return f"'{quoted}'"


def check_lock_timeout(seconds: int) -> int:
    """
    Refuse a lock timeout that no migration could set.

    Args:
        seconds: The seconds a statement waits for a lock; ``0`` for none.

    Returns:
        ``seconds``, unchanged.

    Raises:
        ValueError: if ``seconds`` is not a whole number from 0 to 2147483
            (the server keeps it as milliseconds in an int).
    """
    # A bool is an int, and True would read as one second.
    if (
        isinstance(seconds, bool)
        or not isinstance(seconds, int)
        or not 0 <= seconds <= MAX_LOCK_TIMEOUT
    ):
        raise ValueError(
            "lock_timeout must be a whole number of seconds from 0 to "
            f"{MAX_LOCK_TIMEOUT}, got {seconds!r}",
        )
    return seconds


def _identifier(table: Union[str, Identifier]) -> Identifier:
    if isinstance(table, Identifier):
        return table
    return Identifier.parse(table, what="table name", warn=False)


class QueryBase:
    """Base class for all queries."""

    def __init__(self, table_name: Union[str, Identifier]) -> None:
        """
        Initialize the query.

        Args:
            table_name: The table, as configured or already checked.

        Raises:
            IdentifierError: ``table_name`` is a string the rule refuses.
        """
        self.table = _identifier(table_name)
        # The SQL names the table as configured: PostgreSQL folds it.
        self.table_name = self.table.configured


class Column(QueryBase):
    """Base class for all columns."""

    def __init__(
        self,
        name: str,
        type_: str,
        nullable: bool = False,
        default: Any = None,
        primary_key: bool = False,
    ) -> None:
        """Initialize the column."""
        self.name = name
        self.type = type_
        self.nullable = nullable
        self.default = default
        self.primary_key = primary_key

    @property
    def result_type(self) -> ParamType:
        """The type of the column in a result row."""
        return param_type(self.type)

    @property
    def bind_type(self) -> ParamType:
        """The type of the parameter that carries a value for the column."""
        return self.result_type

    def make_query(self) -> str:
        """Return the column definition as a SQL string."""
        parts = [self.name, self.type]

        if not self.nullable:
            parts.append("NOT NULL")

        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")

        if self.primary_key:
            parts.append("PRIMARY KEY")

        return " ".join(parts)

    def placeholder(self, position: int) -> str:
        """Return the SQL that stands for the value bound at ``position``."""
        return f"${position}"


class ClaimColumns(NamedTuple):
    """Columns that tell which channel a message belongs to and when it is due."""

    channel: Column
    not_before: Column


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

    def _text(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"({', '.join(column.make_query() for column in self.columns)})"
        )

    def make_query(self) -> str:
        """Return the query as a string."""
        return self._text()

    def script(self) -> Script:
        """Return the query as a script: it takes no parameters."""
        return Script(self._text())


def index_name(table_name: str, column: Column) -> str:
    """Return the name of the index ``CreateIndexQuery`` creates on ``column``."""
    return f"{table_name}_{column.name}_idx"


class CreateIndexQuery(QueryBase):
    """Query to create an index."""

    def _text(self, columns: Sequence[Column]) -> str:
        return "\n".join(
            "CREATE INDEX IF NOT EXISTS "
            f"{index_name(self.table_name, column)} "
            f"ON {self.table_name} USING HASH ({column.name});"
            for column in columns
        )

    def make_query(self, columns: Sequence[Column]) -> str:
        """Return the query as a string."""
        return self._text(columns)

    def script(self, columns: Sequence[Column]) -> Script:
        """Return the query as a script: it takes no parameters."""
        return Script(self._text(columns))


# The lock key is a contract between releases: every release takes
# pg_advisory_xact_lock(hashtext('taskiq_postgresql'), hashtext(<table name as
# configured>)), so two releases running side by side, as in a rolling deploy,
# still serialize their migrations. Changing the SQL or the namespace breaks it.
MIGRATION_LOCK_NAMESPACE: Final = "taskiq_postgresql"

# One template for both forms: the query binds the two arguments, the script
# writes them as literals.
_LOCK_TEMPLATE: Final = "SELECT pg_advisory_xact_lock(hashtext({ns}), hashtext({key}))"


def migration_lock_script(table_name: str) -> Script:
    """
    Return the lock ``MigrationLockQuery`` takes, with its arguments as literals.

    A script has no bind parameters.

    Args:
        table_name: The table name as configured.

    Returns:
        The script.
    """
    return Script(
        _LOCK_TEMPLATE.format(
            ns=_sql_literal(MIGRATION_LOCK_NAMESPACE),
            key=_sql_literal(table_name),
        ),
    )


class MigrationLockQuery(QueryBase):
    """Query taking the transaction lock that serializes migrations of a table."""

    def make_query(self) -> str:
        """Return the query as a string."""
        return _LOCK_TEMPLATE.format(ns="$1", key="$2")

    def statement(self) -> Statement:
        """Return the query typed; its result is ``void``, so it has no column."""
        return Statement(self.make_query(), (_TEXT, _TEXT))

    def params(self) -> tuple[str, str]:
        """Return the lock key arguments, in the order of the query."""
        return (MIGRATION_LOCK_NAMESPACE, self.table_name)


class AddedIndex(NamedTuple):
    """A btree index that ``AddColumnsQuery`` creates after adding its columns."""

    name: str
    columns: Sequence[Column]


class AddColumnsQuery(QueryBase):
    """Query adding columns, then indexes, to a table that may already exist."""

    def _text(
        self,
        columns: Sequence[Column],
        indexes: Sequence[AddedIndex],
    ) -> str:
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

    def make_query(
        self,
        columns: Sequence[Column],
        indexes: Sequence[AddedIndex] = (),
    ) -> str:
        """Return the query as a string: one statement each, without parameters."""
        return self._text(columns, indexes)

    def script(
        self,
        columns: Sequence[Column],
        indexes: Sequence[AddedIndex] = (),
    ) -> Script:
        """Return the statements as a script: they take no parameters."""
        return Script(self._text(columns, indexes))


class DropNotNullQuery(QueryBase):
    """Query making an existing column accept ``NULL``; a no-op when it already does."""

    def _text(self, column: str) -> str:
        return f"ALTER TABLE {self.table_name} ALTER COLUMN {column} DROP NOT NULL;"

    def make_query(self, column: str) -> str:
        """Return the query as a string: one statement, without parameters."""
        return self._text(column)

    def script(self, column: str) -> Script:
        """Return the statement as a script: it takes no parameters."""
        return Script(self._text(column))


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
            *(index_name(self.table_name, column) for column in index_columns or ()),
            *(index.name for index in added_indexes),
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
        count = len(
            self.params(
                index_columns,
                added_columns=added_columns,
                added_indexes=added_indexes,
            ),
        )
        return Statement(
            self.make_query(
                index_columns,
                added_columns=added_columns,
                added_indexes=added_indexes,
            ),
            (_TEXT,) * count,
            (("?column?", ParamType.BOOL),),
        )


def _returning(
    columns: Optional[Sequence[Column]],
) -> tuple[tuple[str, ParamType], ...]:
    return tuple((column.name, column.result_type) for column in columns or ())


class InsertQuery(QueryBase):
    """Query to insert a row into a table."""

    def make_query(
        self,
        columns: Sequence[Column],
        returning: Optional[Sequence[Column]] = None,
    ) -> str:
        """Return the query as a string."""
        placeholders = ", ".join(
            column.placeholder(position)
            for position, column in enumerate(columns, start=1)
        )
        return (
            f"INSERT INTO {self.table_name} "  # noqa: S608
            f"({', '.join(column.name for column in columns)}) "
            f"VALUES ({placeholders})"
            + (
                f" RETURNING {', '.join(column.name for column in returning)}"
                if returning is not None
                else ""
            )
        )

    def statement(
        self,
        columns: Sequence[Column],
        returning: Optional[Sequence[Column]] = None,
    ) -> Statement:
        """Return the query typed, one parameter per column."""
        return Statement(
            self.make_query(columns, returning),
            tuple(column.bind_type for column in columns),
            _returning(returning),
        )


class InsertOrUpdateQuery(QueryBase):
    """Query to insert or update a row into a table."""

    def make_query(
        self,
        columns: Sequence[Column],
        returning: Optional[Sequence[Column]],
        on_conflict_columns: Sequence[Column],
        on_conflict_action: Literal["UPDATE", "NOTHING"] = "UPDATE",
        on_conflict_update_columns: Optional[Sequence[Column]] = None,
    ) -> str:
        """Return the query as a string."""
        insert_query = InsertQuery(self.table).make_query(columns)
        returning_query = (
            f" RETURNING {', '.join(column.name for column in returning)}"
            if returning is not None
            else ""
        )
        conflict_query = ", ".join(column.name for column in on_conflict_columns)
        if on_conflict_action == "UPDATE":
            if on_conflict_update_columns is None:
                raise ValueError(
                    "on_conflict_update_columns is required when "
                    "on_conflict_action is UPDATE",
                )

            set_query = ", ".join(
                f"{column.name} = EXCLUDED.{column.name}"
                for column in on_conflict_update_columns
            )
            update_query = f"ON CONFLICT ({conflict_query}) DO UPDATE SET {set_query}"
            return f"{insert_query} {update_query} {returning_query}"

        nothing_query = f"ON CONFLICT ({conflict_query}) DO NOTHING"
        return f"{insert_query} {nothing_query}{returning_query}"

    def statement(
        self,
        columns: Sequence[Column],
        returning: Optional[Sequence[Column]],
        on_conflict_columns: Sequence[Column],
        on_conflict_action: Literal["UPDATE", "NOTHING"] = "UPDATE",
        on_conflict_update_columns: Optional[Sequence[Column]] = None,
    ) -> Statement:
        """Return the query typed, one parameter per inserted column."""
        return Statement(
            self.make_query(
                columns,
                returning,
                on_conflict_columns,
                on_conflict_action,
                on_conflict_update_columns,
            ),
            tuple(column.bind_type for column in columns),
            _returning(returning),
        )


class DeleteQuery(QueryBase):
    """Query to delete a row from a table."""

    def make_query(self, column: Column) -> str:
        """Return the query as a string."""
        return f"DELETE FROM {self.table_name} WHERE {column.name} = $1"  # noqa: S608

    def statement(self, column: Column) -> Statement:
        """Return the query typed: ``$1`` is the value of ``column``."""
        return Statement(self.make_query(column), (column.bind_type,))


class ClaimDueQuery(QueryBase):
    """
    Query deleting the oldest due row of the channel ``$1``, returning columns.

    It skips rows another claim holds instead of waiting for them.
    """

    def make_query(
        self,
        primary_key: Column,
        claim_columns: ClaimColumns,
        returning: Sequence[Column],
    ) -> str:
        """Return the query as a string."""
        channel = claim_columns.channel.name
        not_before = claim_columns.not_before.name
        return (
            f"DELETE FROM {self.table_name} WHERE {primary_key.name} = ("  # noqa: S608
            f"SELECT {primary_key.name} FROM {self.table_name} "
            f"WHERE {channel} = $1 AND {not_before} <= NOW() "
            f"ORDER BY {not_before}, {primary_key.name} LIMIT 1 FOR UPDATE SKIP LOCKED"
            f") RETURNING {', '.join(column.name for column in returning)}"
        )

    def statement(
        self,
        primary_key: Column,
        claim_columns: ClaimColumns,
        returning: Sequence[Column],
    ) -> Statement:
        """Return the query typed: ``$1`` is the channel."""
        return Statement(
            self.make_query(primary_key, claim_columns, returning),
            (claim_columns.channel.bind_type,),
            _returning(returning),
        )


class ClaimByIdQuery(QueryBase):
    """
    Query deleting the row whose id is ``$2``, returning columns.

    It takes the row only once it is due, and only if it belongs to the
    channel ``$1`` or to none (a row written before the channel column).
    """

    def make_query(
        self,
        primary_key: Column,
        claim_columns: ClaimColumns,
        returning: Sequence[Column],
    ) -> str:
        """Return the query as a string."""
        channel = claim_columns.channel.name
        not_before = claim_columns.not_before.name
        return (
            f"DELETE FROM {self.table_name} "  # noqa: S608
            f"WHERE {primary_key.name} = $2 AND {not_before} <= NOW() "
            f"AND ({channel} = $1 OR {channel} IS NULL) "
            f"RETURNING {', '.join(column.name for column in returning)}"
        )

    def statement(
        self,
        primary_key: Column,
        claim_columns: ClaimColumns,
        returning: Sequence[Column],
    ) -> Statement:
        """Return the query typed: ``$1`` is the channel, ``$2`` the id."""
        return Statement(
            self.make_query(primary_key, claim_columns, returning),
            (claim_columns.channel.bind_type, primary_key.bind_type),
            _returning(returning),
        )


class DeleteByDateQuery(QueryBase):
    """
    Query deleting the rows whose ``column`` is ``$1`` or later.

    Rows later than ``$2`` are kept, unless ``$2`` is NULL: then there is no
    upper bound.
    """

    def make_query(self, column: Column) -> str:
        """
        Return the query as a string.

        Returns:
            The query, with both bounds inclusive. ``$2`` is tested for NULL
            on its own: ``BETWEEN $1 AND NULL`` is never true and would delete
            nothing. Its cast gives the server its type when it is NULL.
        """
        return (
            f"DELETE FROM {self.table_name} "  # noqa: S608
            f"WHERE {column.name} >= $1 "
            f"AND ($2::{column.type} IS NULL OR {column.name} <= $2)"
        )

    def statement(self, column: Column) -> Statement:
        """Return the query typed: both bounds have the type of ``column``."""
        return Statement(
            self.make_query(column),
            (column.bind_type, column.bind_type),
        )


class SelectQuery(QueryBase):
    """Query to select a row from a table."""

    def make_query(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
    ) -> str:
        """Return the query as a string."""
        return (
            f"SELECT {', '.join(column.name for column in columns)} "  # noqa: S608
            f"FROM {self.table_name} "
            + (
                f"WHERE {' AND '.join(f'{column.name} = ${i}' for i, column in enumerate(where_columns, start=1))}"  # noqa: E501
                if where_columns is not None
                else ""
            )
        )

    def statement(
        self,
        columns: Sequence[Column],
        where_columns: Optional[Sequence[Column]] = None,
    ) -> Statement:
        """Return the query typed: one parameter per ``where_columns`` item."""
        return Statement(
            self.make_query(columns, where_columns),
            tuple(column.bind_type for column in where_columns or ()),
            _returning(columns),
        )


class CreatedAtColumn(Column):
    """Column for the created at timestamp."""

    def __init__(self) -> None:
        """Initialize the column."""
        super().__init__(
            "created_at",
            "TIMESTAMP WITH TIME ZONE",
            nullable=False,
            default="NOW()",
        )


class NotBeforeColumn(Column):
    """
    Column for the time a message becomes due, by the server's clock.

    Its bound value is a delay in seconds, added to the ``NOW()`` that the
    insert also gives ``created_at``.
    """

    def __init__(self) -> None:
        """Initialize the column."""
        super().__init__(
            "not_before",
            "TIMESTAMP WITH TIME ZONE",
            nullable=False,
            default="NOW()",
        )

    @property
    def bind_type(self) -> ParamType:
        """The type of the delay in seconds that stands for the due time."""
        return ParamType.FLOAT8

    def placeholder(self, position: int) -> str:
        """Return the due time ``position``'s delay in seconds gives."""
        return f"NOW() + make_interval(secs => ${position}::double precision)"


class UpdatedAtColumn(Column):
    """Column for the updated at timestamp."""

    def __init__(self) -> None:
        """Initialize the column."""
        super().__init__(
            "updated_at",
            "TIMESTAMP WITH TIME ZONE",
            nullable=False,
            default="NOW()",
        )


class PrimaryKeyColumn(Column):
    """Column for the primary key."""

    def __init__(
        self,
        name: str = "id",
        type_: str = "UUID",
        default: Any = None,
    ) -> None:
        """Initialize the column."""
        super().__init__(name, type_, primary_key=True, default=default)


# --- statements and scripts that belong to no builder -------------------------


def exists_statement(table: Union[str, Identifier], key: Column) -> Statement:
    """
    Return the query telling whether a row with the key ``$1`` exists.

    Args:
        table: The table.
        key: The column ``$1`` is compared with.

    Returns:
        The statement; the one column it returns is the constant ``1``.
    """
    name = _identifier(table).configured
    return Statement(
        f"SELECT 1 FROM {name} WHERE {key.name} = $1",  # noqa: S608
        (key.bind_type,),
        (("?column?", ParamType.INT4),),
    )


def notify_statement() -> Statement:
    """
    Return the statement that sends a notification: channel ``$1``, payload ``$2``.

    ``pg_notify`` returns ``void``. The statement selects it as a table
    function, which has no column, so no driver has to decode a ``void`` value
    (psqlpy cannot).
    """
    return Statement("SELECT FROM pg_notify($1, $2)", (_TEXT, _TEXT))


def listen_script(channel: Identifier) -> Script:
    """Return the ``LISTEN`` of ``channel``, by the name PostgreSQL folds it to."""
    return Script(f"LISTEN {channel.name}")


def drop_table_script(table: Union[str, Identifier]) -> Script:
    """Return the script that drops ``table`` if it exists."""
    return Script(f"DROP TABLE IF EXISTS {_identifier(table).configured}")


def script_transaction(statements: Sequence[str]) -> list[str]:
    """Return ``statements`` between ``BEGIN`` and ``COMMIT``."""
    return ["BEGIN", *statements, "COMMIT"]


class LockTimeoutScripts(NamedTuple):
    """The scripts that bound the lock waits of the steps of one transaction."""

    set_local: Script
    save: Script
    restore: Script


def lock_timeout_scripts(lock_timeout: int) -> LockTimeoutScripts:
    """
    Return the scripts that bound lock waits to ``lock_timeout`` seconds.

    Args:
        lock_timeout: Seconds a statement waits for a lock.

    Returns:
        ``set_local``, which bounds the waits that follow it in the
        transaction; ``save``, which keeps the transaction's own
        ``lock_timeout``; and ``restore``, which gives the kept value back.

    Raises:
        ValueError: if ``lock_timeout`` is not a whole number of seconds from
            0 to 2147483.
    """
    check_lock_timeout(lock_timeout)
    saved = _sql_literal(SAVED_LOCK_TIMEOUT)
    # missing_ok and NULLIF turn a value this transaction never saved (missing
    # or empty) into NULL, which set_config reads as a reset, instead of an
    # error.
    return LockTimeoutScripts(
        set_local=Script(f"SET LOCAL lock_timeout = '{lock_timeout}s'"),
        save=Script(
            f"SELECT set_config({saved}, current_setting('lock_timeout'), true)",
        ),
        restore=Script(
            "SELECT set_config('lock_timeout', "
            f"NULLIF(current_setting({saved}, true), ''), true)",
        ),
    )


# --- reading a database's catalog ---------------------------------------------

CatalogTable = tuple[Identifier, Sequence[str]]
"""A table to read, and the names of the indexes on it, as configured."""


def _table_state_sql(table: Identifier, index_names: Sequence[str]) -> str:
    # Names go in as escaped literals: the query has no bind parameters, so
    # every driver runs it the same way.
    name = _sql_literal(table.configured)
    indexes = ", ".join(_index_state_sql(index) for index in index_names)
    return (
        "(SELECT json_build_object("  # noqa: S608
        f"'name', {name}, "
        "'relkind', c.relkind, "
        "'columns', ac.columns, "
        # Beside the columns, not inside them: the pairs keep their shape.
        "'not_null', ac.not_null, "
        f"'indexes', json_build_array({indexes})) "
        f"FROM (SELECT to_regclass({name}) AS oid) AS t "
        "LEFT JOIN pg_class c ON c.oid = t.oid "
        # One scan of pg_attribute for both. An aggregate without GROUP BY
        # yields one row even for none, with NULL, as the subqueries did.
        "LEFT JOIN LATERAL (SELECT json_agg(json_build_array(a.attname,"
        " format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum) AS columns,"
        " json_agg(a.attname ORDER BY a.attnum) FILTER (WHERE a.attnotnull)"
        " AS not_null FROM pg_attribute a WHERE a.attrelid = c.oid"
        " AND a.attnum > 0 AND NOT a.attisdropped) AS ac ON true)"
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


def catalog_read(tables: Sequence[CatalogTable]) -> Statement:
    """
    Return the read-only query that reads what a database holds for ``tables``.

    The query has no parameters: the names are written in it as literals, so
    every driver runs it as it is. It returns one row with one text column,
    ``catalog``. Names resolve through the ``search_path``, as the components'
    own SQL does, and the query reads only the ``pg_catalog`` tables.

    Args:
        tables: Each table, with the names of its indexes as configured.

    Returns:
        The statement.

    Raises:
        ValueError: if ``tables`` is empty.
    """
    if not tables:
        raise ValueError("a catalog query needs at least one table schema")
    return Statement(
        "SELECT json_build_object('tables', json_build_array("
        + ", ".join(_table_state_sql(table, names) for table, names in tables)
        + "))::text AS catalog",
        (),
        (("catalog", _TEXT),),
    )


def database_read(tables: Sequence[CatalogTable]) -> Statement:
    """
    Return the query that reads the catalog and what names the database.

    Args:
        tables: Each table, with the names of its indexes as configured.

    Returns:
        One row: ``database``, ``search_path`` and ``catalog``, all text.
    """
    catalog = catalog_read(tables)
    return Statement(
        "SELECT current_database()::text AS database,"
        " current_schemas(false)::text AS search_path,"
        f" ({catalog.text}) AS catalog",
        (),
        (("database", _TEXT), ("search_path", _TEXT), ("catalog", _TEXT)),
    )
