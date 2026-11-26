"""The declaration of a table: what a component hands its driver."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from taskiq_postgresql.core.sql.catalog import AddedIndex, Column

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.core.sql.identifier import Identifier


@dataclass(frozen=True)
class TableDeclaration:
    """
    One table, as a component declares it.

    The values are kept as given: nothing is converted or checked here, so a
    description built from a declaration is the one built from the same
    arguments before. The name check stays with the driver's constructor.

    Attributes:
        table: The table name.
        primary_key: The primary-key column.
        columns: The columns between the key and ``created_at``, in the order
            of ``CREATE TABLE``.
        created_at: The ``created_at`` column.
        index_columns: The columns with a hash index; ``None`` creates none.
        added_columns: Columns added after ``CREATE TABLE``, so that a table an
            older release created gets them too.
        added_indexes: Indexes added with ``added_columns``.
    """

    table: Identifier
    primary_key: Column
    columns: Sequence[Column]
    created_at: Column
    index_columns: Optional[Sequence[Column]]
    added_columns: Sequence[Column] = ()
    added_indexes: Sequence[AddedIndex] = ()

    @property
    def table_columns(self) -> tuple[Column, ...]:
        """The columns of ``CREATE TABLE``, in order."""
        return (self.primary_key, *self.columns, self.created_at)

    @classmethod
    def of_driver(cls, driver: QueryDriver) -> TableDeclaration:
        """
        Read the declaration a query driver was built with.

        Args:
            driver: The query driver of a component, or one a third party wrote.

        Returns:
            The driver's own objects, not copies.
        """
        return cls(
            table=driver.table,
            primary_key=driver.primary_key,
            columns=driver.columns,
            created_at=driver.created_at,
            index_columns=driver.index_columns,
            added_columns=driver.added_columns,
            added_indexes=driver.added_indexes,
        )
