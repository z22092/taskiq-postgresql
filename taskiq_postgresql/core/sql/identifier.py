"""
The rule for a configured table or channel name, and the names derived from it.

Names are written unquoted into SQL, so the rule is a gate: only what
PostgreSQL itself reads as a plain identifier gets through (ADR 0008).
"""

import re
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional

# PostgreSQL's NAMEDATALEN - 1.
MAX_IDENTIFIER_BYTES = 63
_MAX_EXCERPT = 64

_GRAMMAR = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")

# The lower-case words of ``pg_get_keywords()`` with catcode 'R' (reserved) or
# 'T' (reserved, can be a type or function name) on PostgreSQL 17 and 18, which
# agree. The grammar takes only IDENT, unreserved_keyword and
# col_name_keyword as a table or channel name, so both R and T words fail
# unquoted: ``CREATE TABLE left ()`` is a syntax error.
RESERVED_WORDS: frozenset[str] = frozenset(
    {
        "all",
        "analyse",
        "analyze",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "asymmetric",
        "authorization",
        "binary",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "collation",
        "column",
        "concurrently",
        "constraint",
        "create",
        "cross",
        "current_catalog",
        "current_date",
        "current_role",
        "current_schema",
        "current_time",
        "current_timestamp",
        "current_user",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "fetch",
        "for",
        "foreign",
        "freeze",
        "from",
        "full",
        "grant",
        "group",
        "having",
        "ilike",
        "in",
        "initially",
        "inner",
        "intersect",
        "into",
        "is",
        "isnull",
        "join",
        "lateral",
        "leading",
        "left",
        "like",
        "limit",
        "localtime",
        "localtimestamp",
        "natural",
        "not",
        "notnull",
        "null",
        "offset",
        "on",
        "only",
        "or",
        "order",
        "outer",
        "overlaps",
        "placing",
        "primary",
        "references",
        "returning",
        "right",
        "select",
        "session_user",
        "similar",
        "some",
        "symmetric",
        "system_user",
        "table",
        "tablesample",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "variadic",
        "verbose",
        "when",
        "where",
        "window",
        "with",
    },
)


class IdentifierError(ValueError):
    """A table or channel name the package cannot write into SQL."""


def excerpt(value: str) -> str:
    """Return ``value`` as an error or log line may carry it: quoted, cut if long."""
    # A configured value can come from anywhere (CWE-117): keep the line
    # break escaped and the length fixed.
    shown = repr(value[:_MAX_EXCERPT])
    if len(value) > _MAX_EXCERPT:
        shown += f" (truncated, {len(value)} characters)"
    return shown


def fold(name: str) -> str:
    """Fold ``A`` to ``Z`` to lower case, as PostgreSQL does in a multibyte encoding."""
    return "".join(char.lower() if "A" <= char <= "Z" else char for char in name)


def truncate(name: str) -> str:
    """Cut ``name`` to 63 bytes, as ``truncate_identifier`` does."""
    raw = name.encode("utf-8")
    if len(raw) <= MAX_IDENTIFIER_BYTES:
        return name
    return raw[:MAX_IDENTIFIER_BYTES].decode("utf-8", errors="ignore")


def object_name(name1: str, name2: Optional[str], label: str) -> str:
    """
    Build a name the way ``makeObjectName`` (indexcmds.c) does.

    The longer of the two names loses characters until the whole fits in 63
    bytes; the label is never cut.

    Args:
        name1: First part, usually the table name.
        name2: Second part (a column), or None.
        label: Suffix such as ``pkey`` or ``seq``.

    Returns:
        The name PostgreSQL gives the object.
    """
    overhead = len(label) + 1
    if name2 is not None:
        overhead += 1
    budget = MAX_IDENTIFIER_BYTES - overhead
    first = name1
    second = name2 if name2 is not None else ""
    while len(first) + len(second) > budget:
        if len(first) > len(second):
            first = first[:-1]
        else:
            second = second[:-1]
    result = first
    if name2 is not None:
        result += "_" + second
    return result + "_" + label


@dataclass(frozen=True, slots=True)
class Identifier:
    """
    A table or channel name that passed the rule.

    Attributes:
        configured: The accepted value, as configured.
        name: The name PostgreSQL uses for it: ``configured`` folded.
    """

    configured: str
    name: str

    @classmethod
    def parse(
        cls,
        value: object,
        *,
        what: str,
        warn: bool = True,
        stacklevel: int = 2,
    ) -> "Identifier":
        """
        Check ``value`` against the rule and return it as an ``Identifier``.

        Args:
            value: The configured name.
            what: What the name is, for the error ("table name").
            warn: Whether mixed case gets a ``DeprecationWarning``.
            stacklevel: ``warnings.warn`` level; 2 points at the caller of
                this method, 3 at that caller's caller.

        Returns:
            The accepted name.

        Raises:
            TypeError: ``value`` is not a ``str``.
            IdentifierError: ``value`` is empty, outside the grammar, over 63
                bytes or a reserved word.
        """
        if not isinstance(value, str):
            msg = f"{what} must be a str, not {type(value).__name__}"
            raise TypeError(msg)
        if not value:
            msg = f"{what} '' cannot be written in SQL: it is empty"
            raise IdentifierError(msg)
        if not _GRAMMAR.fullmatch(value):
            msg = (
                f"{what} {excerpt(value)} is not a plain SQL identifier "
                "(ASCII letters, digits, _ and $, not starting with a digit "
                "or $), and the SQL writes it unquoted"
            )
            raise IdentifierError(msg)
        # ASCII only here, so bytes and characters are the same count.
        if len(value) > MAX_IDENTIFIER_BYTES:
            msg = (
                f"{what} {excerpt(value)} is {len(value)} bytes; "
                f"PostgreSQL keeps {MAX_IDENTIFIER_BYTES}"
            )
            raise IdentifierError(msg)
        folded = fold(value)
        if folded in RESERVED_WORDS:
            msg = f"{what} {excerpt(value)} is a reserved word in PostgreSQL"
            raise IdentifierError(msg)
        if warn and folded != value:
            warnings.warn(
                f"{what} {value!r} has upper case letters; PostgreSQL folds it "
                f"to {folded!r}, the name the table or channel really has. "
                f"Configure {folded!r}.",
                DeprecationWarning,
                stacklevel=stacklevel,
            )
        return cls(configured=value, name=folded)


def index_relation_name(table: Identifier, column: str) -> str:
    """Return the name ``CREATE INDEX`` ends up with for ``column`` of ``table``."""
    return truncate(f"{table.name}_{column}_idx")


def derived_names(
    table: Identifier,
    *,
    index_names: Iterable[str],
    serial_column: Optional[str],
) -> tuple[tuple[str, str], ...]:
    """
    Return the names PostgreSQL gives the objects of a table, kind first.

    Args:
        table: The checked table name.
        index_names: Index names as the package writes them, with the
            configured case; each is folded and then cut here.
        serial_column: The ``SERIAL`` column, or None when there is none.

    Returns:
        ``(kind, name)`` pairs: the table, each index, the primary key's
        index and, with a ``SERIAL`` column, its sequence.
    """
    named: list[tuple[str, str]] = [("table", table.name)]
    named.extend(("index", truncate(fold(name))) for name in index_names)
    named.append(("primary key", object_name(table.name, None, "pkey")))
    if serial_column is not None:
        named.append(
            ("sequence", object_name(table.name, serial_column, "seq")),
        )
    return tuple(named)


def check_table_names(
    table: Identifier,
    *,
    index_names: Iterable[str],
    serial_column: Optional[str],
) -> None:
    """
    Refuse a table name whose derived names collide.

    ``CREATE INDEX IF NOT EXISTS`` skips an index whose cut name already
    exists, so a collision loses an index without an error.

    Args:
        table: The checked table name.
        index_names: Index names as the package writes them, with the
            configured case; each is folded and then cut here.
        serial_column: The ``SERIAL`` column, or None when there is none.

    Raises:
        IdentifierError: Two of the names, the table name included, are equal.
    """
    named = derived_names(
        table,
        index_names=index_names,
        serial_column=serial_column,
    )
    clashes = [
        f"{excerpt(first[1])} ({first[0]}) and {excerpt(second[1])} ({second[0]})"
        for position, first in enumerate(named)
        for second in named[position + 1 :]
        if first[1] == second[1]
    ]
    if clashes:
        msg = (
            f"table name {excerpt(table.configured)} is too long: PostgreSQL "
            "would give two objects the same name: " + "; ".join(clashes)
        )
        raise IdentifierError(msg)
