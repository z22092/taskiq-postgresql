"""
What a public ``RenderedStep`` and ``TaskiqStepOp`` accept, and where targets load.

None of these tests needs a database: the comment lines go into generated
Python files, and a target path is imported from the working directory only
where the caller asks for it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from taskiq_postgresql.migrations import (
    RenderedStep,
    collect_schemas,
    table_schema,
)
from tests.helpers import UNUSED_DSN, importable_modules

HOSTILE_LINES = [
    pytest.param("first\nimport os", id="line-break"),
    pytest.param("first\rimport os", id="carriage-return"),
    pytest.param("first\0second", id="nul"),
    pytest.param("# coding: utf-7", id="encoding-cookie"),
    pytest.param("-*- CODING=latin-1 -*-", id="encoding-cookie-upper-case"),
]
BROKER_SOURCE = f"""\
from taskiq_postgresql import PostgresqlBroker

broker = PostgresqlBroker(dsn={UNUSED_DSN!r})
"""


def a_step() -> RenderedStep:
    """A valid step, to copy with a hostile line."""
    step = table_schema("broker").steps[0]
    return RenderedStep.of(
        "taskiq_messages",
        "broker",
        step,
        transaction_note="Runs in the tool's transaction.",
    )


def test_a_rendered_step_keeps_the_comments_it_was_given() -> None:
    step = a_step()

    assert (
        RenderedStep(
            table=step.table,
            kind=step.kind,
            number=step.number,
            comments=step.comments,
            statements=step.statements,
        )
        == step
    )


@pytest.mark.parametrize("line", HOSTILE_LINES)
def test_a_rendered_step_refuses_a_comment_a_file_cannot_hold(line: str) -> None:
    step = a_step()

    with pytest.raises(ValueError, match="cannot be written in a migration file"):
        RenderedStep(
            table=step.table,
            kind=step.kind,
            number=step.number,
            comments=(*step.comments, line),
            statements=step.statements,
        )


@pytest.mark.parametrize("line", HOSTILE_LINES)
def test_a_note_of_a_step_is_refused_the_same_way(line: str) -> None:
    with pytest.raises(ValueError, match="cannot be written in a migration file"):
        RenderedStep.of(
            "taskiq_messages",
            "broker",
            table_schema("broker").steps[0],
            transaction_note="Runs in the tool's transaction.",
            notes=[line],
        )


@pytest.mark.parametrize("line", HOSTILE_LINES)
def test_an_alembic_step_operation_refuses_a_note_a_file_cannot_hold(
    line: str,
) -> None:
    from taskiq_postgresql.contrib.alembic import TaskiqStepOp  # noqa: PLC0415

    with pytest.raises(ValueError, match="cannot be written in a migration file"):
        TaskiqStepOp(
            "taskiq_messages",
            "broker",
            table_schema("broker").steps[0],
            notes=[line],
        )


def test_a_path_target_is_imported_from_the_working_directory_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The taskiq CLI convention the Django app does not follow.
    with importable_modules(tmp_path, monkeypatch) as write:
        monkeypatch.chdir(tmp_path)
        before = list(sys.path)

        schemas = collect_schemas(f"{write(BROKER_SOURCE)}:broker")

        assert sys.path == before
    assert [schema.kind for schema in schemas] == ["broker"]
