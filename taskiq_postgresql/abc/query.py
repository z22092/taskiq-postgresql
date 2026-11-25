from typing import Any, Final, Literal, NamedTuple, Optional, Sequence


class QueryBase:
    """Base class for all queries."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        self.table_name = table_name


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

    def __init__(self, table_name: str, columns: Sequence[Column]) -> None:
        """Initialize the query."""
        super().__init__(table_name)
        self.columns = columns

    def make_query(self) -> str:
        """Return the query as a string."""
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"({', '.join(column.make_query() for column in self.columns)})"
        )


def index_name(table_name: str, column: Column) -> str:
    """Return the name of the index ``CreateIndexQuery`` creates on ``column``."""
    return f"{table_name}_{column.name}_idx"


class CreateIndexQuery(QueryBase):
    """Query to create an index."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(self, columns: Sequence[Column]) -> str:
        """Return the query as a string."""
        return "\n".join(
            "CREATE INDEX IF NOT EXISTS "
            f"{index_name(self.table_name, column)} "
            f"ON {self.table_name} USING HASH ({column.name});"
            for column in columns
        )


# The lock key is a contract between releases: every release takes
# pg_advisory_xact_lock(hashtext('taskiq_postgresql'), hashtext(<table name as
# configured>)), so two releases running side by side, as in a rolling deploy,
# still serialize their migrations. Changing the SQL or the namespace breaks it.
MIGRATION_LOCK_NAMESPACE: Final = "taskiq_postgresql"


class MigrationLockQuery(QueryBase):
    """Query taking the transaction lock that serializes migrations of a table."""

    def make_query(self) -> str:
        """Return the query as a string."""
        return "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))"

    def params(self) -> tuple[str, str]:
        """Return the lock key arguments, in the order of the query."""
        return (MIGRATION_LOCK_NAMESPACE, self.table_name)


class AddedIndex(NamedTuple):
    """A btree index that ``AddColumnsQuery`` creates after adding its columns."""

    name: str
    columns: Sequence[Column]


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


class DropNotNullQuery(QueryBase):
    """Query making a column of a table that may already exist accept ``NULL``."""

    def make_query(self, column: str) -> str:
        """Return the query as a string: one statement, without parameters."""
        return f"ALTER TABLE {self.table_name} ALTER COLUMN {column} DROP NOT NULL;"


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


class InsertQuery(QueryBase):
    """Query to insert a row into a table."""

    def __init__(
        self,
        table_name: str,
    ) -> None:
        """Initialize the query."""
        super().__init__(table_name)

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


class InsertOrUpdateQuery(QueryBase):
    """Query to insert or update a row into a table."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(
        self,
        columns: Sequence[Column],
        returning: Optional[Sequence[Column]],
        on_conflict_columns: Sequence[Column],
        on_conflict_action: Literal["UPDATE", "NOTHING"] = "UPDATE",
        on_conflict_update_columns: Optional[Sequence[Column]] = None,
    ) -> str:
        """Return the query as a string."""
        insert_query = InsertQuery(self.table_name).make_query(columns)
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


class DeleteQuery(QueryBase):
    """Query to delete a row from a table."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(self, column: Column) -> str:
        """Return the query as a string."""
        return f"DELETE FROM {self.table_name} WHERE {column.name} = $1"  # noqa: S608


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


class DeleteByDateQuery(QueryBase):
    """Query to delete a row from a table by date."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(self, column: Column) -> str:
        """Return the query as a string."""
        return f"DELETE FROM {self.table_name} WHERE {column.name} BETWEEN $1 AND $2"  # noqa: S608


class SelectQuery(QueryBase):
    """Query to select a row from a table."""

    def __init__(
        self,
        table_name: str,
    ) -> None:
        """Initialize the query."""
        super().__init__(table_name)

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
