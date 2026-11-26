"""
The table-name rule at the places that read a name outside a component.

``check_plain_identifier``, the description of a table by kind and the target
collection of the CLI and the integrations share the rule of ADR 0008: no
reserved word, at most 63 bytes, derived names that do not collide, and one
spelling per folded table name.
"""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import MetaData

from taskiq_postgresql.abc.driver import ListenDriver
from taskiq_postgresql.contrib.sqlalchemy import add_taskiq_tables
from taskiq_postgresql.core.sql.identifier import Identifier, IdentifierError
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


def test_add_taskiq_tables_refuses_one_table_described_with_different_columns() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        broker = table_schema("broker", table_name="jobs")
        results = table_schema("result_backend", table_name="jobs")

    metadata = MetaData()
    add_taskiq_tables(metadata, broker)

    with pytest.raises(ValueError, match="described twice, with different columns"):
        add_taskiq_tables(metadata, results)


class _Listen(ListenDriver):
    async def on_startup(self) -> None: ...

    async def on_shutdown(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[str]:
        raise NotImplementedError


def test_the_base_listen_driver_refuses_a_channel_outside_the_rule() -> None:
    with pytest.raises(IdentifierError, match="reserved word"):
        _Listen("postgresql://unused", "select")


def test_the_base_listen_driver_keeps_the_folded_channel() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        driver = _Listen("postgresql://unused", "Jobs_Mixed")

    assert driver.channel == Identifier.parse(
        "Jobs_Mixed",
        what="channel name",
        warn=False,
    )
    assert driver.channel_name == "jobs_mixed"
    assert driver.listen_text == "LISTEN jobs_mixed"
