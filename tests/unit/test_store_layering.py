"""Keep the components, the CLI and the migration tools off the query driver.

A component talks to the database through its store, and the CLI and
``migrations.py`` go through the core. These tests read the sources with
``ast`` and fail on any access to an attribute named ``driver`` outside the
``__init__`` of the three component classes, where there are exactly two: the
assignment of the driver they build, which stays an attribute anyone may
patch or replace, and the read inside the function handed to the schema store.
``listen_driver`` is another attribute name and is not covered.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[2] / "taskiq_postgresql"
COMPONENT_SOURCES = ("broker.py", "result_backend.py", "scheduler_source.py")
SOURCES = (*COMPONENT_SOURCES, "cli.py", "migrations.py")
COMPONENT_CLASSES = frozenset(
    {"PostgresqlBroker", "PostgresqlResultBackend", "PostgresqlSchedulerSource"},
)
CLI_FORBIDDEN_NAMES = frozenset({"QueryDriver", "get_db_driver"})


def _driver_attributes(tree: ast.AST) -> list[ast.Attribute]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "driver"
    ]


def _component_inits(tree: ast.AST) -> list[ast.FunctionDef]:
    return [
        member
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name in COMPONENT_CLASSES
        for member in node.body
        if isinstance(member, ast.FunctionDef) and member.name == "__init__"
    ]


def _outside_the_init(path: Path) -> list[int]:
    """Line numbers of ``<expr>.driver`` outside a component's ``__init__``."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    allowed = {id(child) for init in _component_inits(tree) for child in ast.walk(init)}
    return sorted(
        node.lineno for node in _driver_attributes(tree) if id(node) not in allowed
    )


@pytest.mark.parametrize("name", SOURCES)
def test_nothing_reads_the_query_driver_outside_the_init(name: str) -> None:
    lines = _outside_the_init(PACKAGE / name)
    assert not lines, (
        f"{name} reaches the query driver at "
        + ", ".join(f"taskiq_postgresql/{name}:{line}" for line in lines)
        + "; go through the store"
    )


@pytest.mark.parametrize("name", COMPONENT_SOURCES)
def test_the_init_assigns_the_driver_and_hands_the_store_a_function_for_it(
    name: str,
) -> None:
    tree = ast.parse((PACKAGE / name).read_text(encoding="utf-8"))
    (init,) = _component_inits(tree)
    in_a_function = {
        id(child)
        for lambda_ in ast.walk(init)
        if isinstance(lambda_, ast.Lambda)
        for child in ast.walk(lambda_)
    }
    accesses = [
        (isinstance(node.ctx, ast.Store), id(node) in in_a_function)
        for node in _driver_attributes(init)
    ]
    assert sorted(accesses) == [(False, True), (True, False)], (
        f"{name}: __init__ must assign self.driver once and read it once, "
        "in the function given to the schema store"
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
