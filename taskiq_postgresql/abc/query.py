from abc import ABC, abstractmethod
from typing import Any, Literal, Optional, Sequence


class QueryBase(ABC):
    """Base class for all queries."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        self.table_name = table_name

    @abstractmethod
    def make_query(self, *values: Any) -> str:
        """Return the query as a string."""


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


class CreateIndexQuery(QueryBase):
    """Query to create an index."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(self, columns: Sequence[Column]) -> str:
        """Return the query as a string."""
        return "\n".join(
            "CREATE INDEX IF NOT EXISTS "
            f"{self.table_name}_{column.name}_idx "
            f"ON {self.table_name} USING HASH ({column.name});"
            for column in columns
        )


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
        return (
            f"INSERT INTO {self.table_name} "  # noqa: S608
            f"({', '.join(column.name for column in columns)}) "
            f"VALUES ({', '.join(f'${i}' for i in range(1, len(columns) + 1))})"
            + (
                f" RETURNING {', '.join(column.name for column in returning)}"
                if returning is not None
                else ""
            )
        )


class InsertOrUpdateQuery(InsertQuery):
    """Query to insert or update a row into a table."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(
        self,
        columns: Sequence[Column],
        returning: Sequence[Column],
        on_conflict_columns: Sequence[Column],
        on_conflict_action: Literal["UPDATE", "NOTHING"] = "UPDATE",
        on_conflict_update_columns: Optional[Sequence[Column]] = None,
    ) -> str:
        """Return the query as a string."""
        insert_query = super().make_query(columns)
        returning_query = (
            f" RETURNING {', '.join(column.name for column in returning)}"
            if returning is not None
            else ""
        )
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
            conflict_query = ", ".join(column.name for column in on_conflict_columns)
            update_query = f"ON CONFLICT ({conflict_query}) DO UPDATE SET {set_query}"
            return f"{insert_query} {update_query} {returning_query}"

        return (
            f"{insert_query} ON CONFLICT ({', '.join(on_conflict_columns)}) DO NOTHING"
            + returning_query
        )


class DeleteQuery(QueryBase):
    """Query to delete a row from a table."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(self, column: Column) -> str:
        """Return the query as a string."""
        return f"DELETE FROM {self.table_name} WHERE {column.name} = $1"  # noqa: S608


class DeleteReturningQuery(QueryBase):
    """Query to delete a row from a table and return specified columns."""

    def __init__(self, table_name: str) -> None:
        """Initialize the query."""
        super().__init__(table_name)

    def make_query(self, where_column: Column, returning: Sequence[Column]) -> str:
        """Return the query as a string."""
        return (
            f"DELETE FROM {self.table_name} "  # noqa: S608
            f"WHERE {where_column.name} = $1 "
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
