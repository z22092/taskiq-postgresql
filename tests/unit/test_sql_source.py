"""Keep SQL construction inside the core catalog (ADR 0007).

Only ``taskiq_postgresql/core/sql/`` builds SQL. The identifier rule plus bind
parameters are the control; these tests read the package source with ``ast`` and
keep the hygiene that control relies on.
"""

from __future__ import annotations

import ast
import io
import tokenize
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[2] / "taskiq_postgresql"
CORE_SQL = PACKAGE / "core" / "sql"
CATALOG = CORE_SQL / "catalog"

SQL_COMMANDS = frozenset(
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "LISTEN",
        "UNLISTEN",
        "NOTIFY",
        "SET",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
    },
)
LOG_METHODS = frozenset(
    {"debug", "info", "warning", "error", "exception", "critical", "log"},
)
LOG_RECEIVERS = frozenset({"logger", "logging", "log"})
CONSTRUCTED_TYPES = frozenset({"Statement", "Script"})


def _sources() -> list[Path]:
    return sorted(PACKAGE.rglob("*.py"))


def _location(path: Path, line: int) -> str:
    return f"{path.relative_to(PACKAGE.parent).as_posix()}:{line}"


def _inside_core_sql(path: Path) -> bool:
    return CORE_SQL in path.parents


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _called_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _starts_with_command(text: str) -> bool:
    # Case-sensitive on purpose: the package writes SQL keywords in upper case,
    # and lower-case DML is already flagged by ruff's S608, which ignores case.
    # Matching case-insensitively would flag prose ("Create the tables") and
    # option values ("drop").
    words = text.split(None, 1)
    return bool(words) and words[0] in SQL_COMMANDS


def _is_log_call(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr in LOG_METHODS
        and isinstance(func.value, ast.Name)
        and func.value.id in LOG_RECEIVERS
    )


def _docstrings(tree: ast.Module) -> set[int]:
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                found.add(id(first.value))
    return found


def _exempt_log_messages(tree: ast.Module) -> set[int]:
    exempt: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_log_call(node) and node.args:
            exempt.update(id(child) for child in ast.walk(node.args[0]))
    return exempt


def _joined_str_parts(tree: ast.Module) -> set[int]:
    # The literal parts of an f-string are Constants of their own; only the
    # first one decides, and that is judged on the JoinedStr.
    return {
        id(value)
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
        for value in node.values
        if isinstance(value, ast.Constant)
    }


def _sql_looking_strings(path: Path) -> list[int]:
    """Lines of strings that start with an SQL command word.

    Covers plain constants (which includes the left operand of ``+`` and ``%``
    and the receiver of ``.format``), and the first literal part of an f-string.
    """
    tree = _parse(path)
    skip = _docstrings(tree) | _exempt_log_messages(tree) | _joined_str_parts(tree)
    lines: list[int] = []
    for node in ast.walk(tree):
        if id(node) in skip:
            continue
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and _starts_with_command(node.value)
        ):
            lines.append(node.lineno)
        elif isinstance(node, ast.JoinedStr):
            first = node.values[0] if node.values else None
            if (
                isinstance(first, ast.Constant)
                and isinstance(first.value, str)
                and id(node) not in skip
                and _starts_with_command(first.value)
            ):
                lines.append(node.lineno)
    return lines


def test_statement_and_script_are_constructed_only_in_core_sql() -> None:
    hits = [
        _location(path, node.lineno)
        for path in _sources()
        if not _inside_core_sql(path)
        for node in ast.walk(_parse(path))
        if isinstance(node, ast.Call) and _called_name(node) in CONSTRUCTED_TYPES
    ]
    assert not hits, f"Statement/Script built outside core/sql/: {hits}"


def test_noqa_s608_appears_only_in_the_catalog() -> None:
    hits: list[str] = []
    for path in _sources():
        if path.is_relative_to(CATALOG):
            continue
        source = path.read_text(encoding="utf-8")
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type == tokenize.COMMENT and "noqa: S608" in token.string:
                hits.append(_location(path, token.start[0]))
    assert not hits, f"# noqa: S608 outside the catalog: {hits}"


def test_no_sql_text_is_built_outside_core_sql() -> None:
    hits = [
        _location(path, line)
        for path in _sources()
        if not _inside_core_sql(path)
        for line in _sql_looking_strings(path)
    ]
    assert not hits, f"SQL text outside core/sql/: {hits}"


def test_the_package_is_found() -> None:
    # A wrong root would make every rule above pass over zero files.
    if not CATALOG.is_dir():
        pytest.fail(f"catalog not found at {CATALOG}")
    assert _sources()
