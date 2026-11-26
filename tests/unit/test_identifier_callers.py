"""
The table-name rule at the places that read a name outside a component.

``check_plain_identifier``, the description of a table by kind and the target
collection of the CLI and the integrations share the rule of ADR 0008: no
reserved word, at most 63 bytes, derived names that do not collide, and one
spelling per folded table name.
"""

from __future__ import annotations

import warnings

import pytest

from taskiq_postgresql.migrations import (
    check_plain_identifier,
    collect_schemas,
    table_schema,
)


def test_check_plain_identifier_refuses_a_reserved_word() -> None:
    with pytest.raises(ValueError, match="reserved word"):
        check_plain_identifier("order")


def test_check_plain_identifier_refuses_64_bytes() -> None:
    with pytest.raises(ValueError, match="64 bytes"):
        check_plain_identifier("a" * 64)


def test_check_plain_identifier_returns_mixed_case_unchanged_without_a_warning() -> (
    None
):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert check_plain_identifier("Jobs") == "Jobs"


def test_table_schema_refuses_a_name_whose_derived_names_collide() -> None:
    with pytest.raises(ValueError, match="too long"):
        table_schema("broker", table_name="a" * 62)


def test_table_schema_warns_once_for_a_mixed_case_name() -> None:
    with pytest.warns(DeprecationWarning, match="upper case") as caught:
        table_schema("broker", table_name="Jobs")
    assert len(caught) == 1


def test_collect_schemas_refuses_two_spellings_of_one_table() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        upper = table_schema("broker", table_name="Jobs")
        lower = table_schema("broker", table_name="jobs")
    with pytest.raises(ValueError, match=r"'Jobs'.*'jobs'"):
        collect_schemas(upper, lower)


def test_collect_schemas_keeps_deduplicating_one_spelling() -> None:
    first = table_schema("broker", table_name="jobs")
    assert collect_schemas(first, table_schema("broker", table_name="jobs")) == [first]
