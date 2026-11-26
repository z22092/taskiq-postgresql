"""The columns the components declare."""

from typing import Any, NamedTuple

from taskiq_postgresql.core.sql.catalog.base import QueryBase
from taskiq_postgresql.core.sql.statement import ParamType, param_type

_SERIAL_TYPES = frozenset(
    {"SMALLSERIAL", "SERIAL2", "SERIAL", "SERIAL4", "BIGSERIAL", "SERIAL8"},
)


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

    @property
    def is_serial(self) -> bool:
        """
        Whether PostgreSQL gives the column a sequence of its own.

        True for the serial pseudo-types: ``SMALLSERIAL``, ``SERIAL``,
        ``BIGSERIAL`` and their ``SERIAL2``, ``SERIAL4`` and ``SERIAL8``
        spellings, in any case.
        """
        return self.type.upper() in _SERIAL_TYPES

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
