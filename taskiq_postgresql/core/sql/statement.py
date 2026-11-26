"""
Typed statements: SQL text plus the types the server infers for it (ADR 0007).

This module knows no catalog and no migrations, so both can import it.
"""

import re
from dataclasses import dataclass
from enum import Enum, auto


class ParamType(Enum):
    """The type of a bind parameter or a result column."""

    TEXT = auto()
    INT4 = auto()
    INT8 = auto()
    FLOAT8 = auto()
    BOOL = auto()
    JSONB = auto()
    UUID = auto()
    BYTEA = auto()
    TIMESTAMPTZ = auto()
    INT4_ARRAY = auto()
    UUID_ARRAY = auto()
    TEXT_ARRAY = auto()


# A ``$`` inside a name (``jobs$5``) is part of the name, not a placeholder.
_PLACEHOLDER = re.compile(r"(?<![A-Za-z0-9_$])\$(\d+)")
_VARCHAR = re.compile(r"VARCHAR(\(\d+\))?")

_SQL_TYPES = {
    "SERIAL": ParamType.INT4,
    "UUID": ParamType.UUID,
    "TEXT": ParamType.TEXT,
    "BYTEA": ParamType.BYTEA,
    "JSONB": ParamType.JSONB,
    "TIMESTAMP WITH TIME ZONE": ParamType.TIMESTAMPTZ,
}


@dataclass(frozen=True)
class Statement:
    """
    SQL text with bind parameters.

    Attributes:
        text: The SQL, values as ``$1``, ``$2``, and so on.
        params: The type of each parameter, in order.
        columns: Name and type of each result column; empty for no rows
            or a ``void`` result.
    """

    text: str
    params: tuple[ParamType, ...] = ()
    columns: tuple[tuple[str, ParamType], ...] = ()

    def __post_init__(self) -> None:
        """Require the placeholders to be exactly ``$1`` to ``$len(params)``."""
        found = {int(number) for number in _PLACEHOLDER.findall(self.text)}
        expected = set(range(1, len(self.params) + 1))
        if found != expected:
            msg = (
                f"placeholders {sorted(found)} in the statement do not match "
                f"its {len(self.params)} declared parameters"
            )
            raise ValueError(msg)


@dataclass(frozen=True)
class Script:
    """SQL that takes no bind parameters: DDL, ``LISTEN``, script lines."""

    text: str


def param_type(sql_type: str) -> ParamType:
    """
    Map a column's SQL type to the type its bind parameter has.

    Args:
        sql_type: The type as declared, in any case.

    Returns:
        The matching ``ParamType``.

    Raises:
        ValueError: The type is not one the components use.
    """
    upper = sql_type.strip().upper()
    if _VARCHAR.fullmatch(upper):
        return ParamType.TEXT
    try:
        return _SQL_TYPES[upper]
    except KeyError:
        msg = f"no ParamType for the SQL type {sql_type!r}"
        raise ValueError(msg) from None
