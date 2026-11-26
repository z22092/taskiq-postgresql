"""The row queries the components run, and the notification the broker sends."""

from typing import Final, Literal, Optional, Sequence

from taskiq_postgresql.core.sql.catalog.base import QueryBase
from taskiq_postgresql.core.sql.catalog.columns import ClaimColumns, Column
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.sql.statement import ParamType, Script, Statement


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


class ExistsQuery(QueryBase):
    """Query telling whether a row with the key ``$1`` exists."""

    def make_query(self, key: Column) -> str:
        """
        Return the query as a string.

        The text does not type the key, so a third-party driver whose key
        column no ``ParamType`` covers keeps working.
        """
        return f"SELECT 1 FROM {self.table_name} WHERE {key.name} = $1"  # noqa: S608

    def statement(self, key: Column) -> Statement:
        """Return the query typed; the one column it returns is the constant ``1``."""
        return Statement(
            self.make_query(key),
            (key.bind_type,),
            (("?column?", ParamType.INT4),),
        )


_NOTIFY: Final = Statement(
    "SELECT FROM pg_notify($1, $2)",
    (ParamType.TEXT, ParamType.TEXT),
)


def notify_statement() -> Statement:
    """
    Return the statement that sends a notification: channel ``$1``, payload ``$2``.

    ``pg_notify`` returns ``void``. The statement selects it as a table
    function, which has no column, so no driver has to decode a ``void`` value
    (psqlpy cannot).
    """
    return _NOTIFY


def listen_script(channel: Identifier) -> Script:
    """Return the ``LISTEN`` of ``channel``, by the name PostgreSQL folds it to."""
    return Script(f"LISTEN {channel.name}")
