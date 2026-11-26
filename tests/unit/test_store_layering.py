"""Keep the components, the CLI and the migration tools off the query driver.

A component talks to the database through its store, and the CLI and
``migrations.py`` go through the core. These tests read the sources with
``ast`` and fail on any access to an attribute named ``driver`` outside the
``driver`` property of the three component classes, which stays readable and
assignable. ``listen_driver`` is another attribute name and is not covered.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[2] / "taskiq_postgresql"
SOURCES = (
    "broker.py",
    "result_backend.py",
    "scheduler_source.py",
    "cli.py",
    "migrations.py",
)
COMPONENT_CLASSES = frozenset(
    {"PostgresqlBroker", "PostgresqlResultBackend", "PostgresqlSchedulerSource"},
)
CLI_FORBIDDEN_NAMES = frozenset({"QueryDriver", "get_db_driver"})


def _driver_accesses(path: Path) -> list[int]:
    """Line numbers of ``<expr>.driver`` outside a component's ``driver`` property."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    allowed: set[int] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name in COMPONENT_CLASSES):
            continue
        for member in node.body:
            if isinstance(member, ast.FunctionDef) and member.name == "driver":
                allowed.update(id(child) for child in ast.walk(member))
    return sorted(
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr == "driver"
        and id(node) not in allowed
    )


@pytest.mark.parametrize("name", SOURCES)
def test_nothing_reads_the_query_driver_outside_the_store(name: str) -> None:
    lines = _driver_accesses(PACKAGE / name)
    assert not lines, (
        f"{name} reaches the query driver at "
        + ", ".join(f"taskiq_postgresql/{name}:{line}" for line in lines)
        + "; go through the store"
    )


def test_the_cli_names_no_query_driver() -> None:
    tree = ast.parse((PACKAGE / "cli.py").read_text(encoding="utf-8"))
    hits = sorted(
        (node.lineno, node.id if isinstance(node, ast.Name) else node.attr)
        for node in ast.walk(tree)
        if (isinstance(node, ast.Name) and node.id in CLI_FORBIDDEN_NAMES)
        or (isinstance(node, ast.Attribute) and node.attr in CLI_FORBIDDEN_NAMES)
    )
    hits += sorted(
        (node.lineno, alias.name)
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if alias.name in CLI_FORBIDDEN_NAMES
    )
    assert not hits, "taskiq_postgresql/cli.py names " + ", ".join(
        f"{name} at taskiq_postgresql/cli.py:{line}" for line, name in hits
    )
