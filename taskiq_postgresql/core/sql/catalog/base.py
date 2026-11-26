"""What every builder of the catalog shares: the table it names, and literals."""

from typing import Union

from taskiq_postgresql.core.sql.identifier import Identifier, as_identifier


def _sql_literal(value: str) -> str:
    quoted = value.replace("'", "''")
    if "\\" in value:
        # E'' reads a doubled backslash as one whatever
        # standard_conforming_strings is set to; a plain literal does not.
        return "E'" + quoted.replace("\\", "\\\\") + "'"
    return f"'{quoted}'"


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
        self.table = as_identifier(table_name, what="table name")
        # The SQL names the table as configured: PostgreSQL folds it.
        self.table_name = self.table.configured
