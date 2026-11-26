"""
The stores: each operation is one call on the query driver, and nothing else.

A driver that records what it is asked stands in for a real one, so no test
here needs a database. The calls pinned are the ones the components made
before the stores existed.
"""

from __future__ import annotations

import warnings
from typing import Any, Optional

import pytest
from taskiq import BrokerMessage
from taskiq_postgresql.core.stores.messages import MessageStore
from typing_extensions import override

from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.broker import Table as MessageTable
from taskiq_postgresql.core.sql.catalog import (
    ClaimByIdQuery,
    ClaimColumns,
    ClaimDueQuery,
    Column,
    PrimaryKeyColumn,
    notify_statement,
)
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.stores.schema import SchemaStore

DSN = "postgresql://postgres@localhost:5432/postgres"

Call = tuple[str, tuple[Any, ...]]

# What the recording driver's insert returns, and a query timeout to hand over.
INSERTED_ID = 7
QUERY_TIMEOUT = 12.5


class RecordingDriver(QueryDriver):
    """A query driver that records each call and answers with fixed values."""

    def __init__(self, table: str = "taskiq_messages") -> None:
        key = PrimaryKeyColumn(name="id", type_="SERIAL")
        super().__init__(
            DSN,
            Identifier.parse(table, what="table name"),
            columns=[Column("body", "TEXT")],
            primary_key=key,
            index_columns=[key],
        )
        self.calls: list[Call] = []
        self.insert_returns: Any = INSERTED_ID
        self.fetch_row_returns: Optional[dict[str, Any]] = None
        self.select_returns: list[dict[str, Any]] = []
        self.exists_returns = True

    @override
    async def __aenter__(self) -> Any:
        return self

    @override
    async def __aexit__(self, *exc_info: object) -> None:
        return None

    @override
    async def insert(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("insert", args))
        assert kwargs == {}
        return self.insert_returns

    @override
    async def insert_or_update(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("insert_or_update", args))
        assert kwargs == {}

    @override
    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("delete", args))
        assert kwargs == {}

    @override
    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        self.calls.append(("fetch_row", (query, *values)))
        return self.fetch_row_returns

    @override
    async def delete_by_date(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("delete_by_date", args))
        assert kwargs == {}

    @override
    async def select(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("select", args))
        assert kwargs == {}
        return self.select_returns

    @override
    async def exists(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("exists", args))
        assert kwargs == {}
        return self.exists_returns

    @override
    async def execute(self, query: str, *values: Any) -> Any:
        self.calls.append(("execute", (query, *values)))

    @override
    async def on_startup(self) -> None:
        self.calls.append(("on_startup", ()))

    @override
    async def on_shutdown(self) -> None:
        self.calls.append(("on_shutdown", ()))


def _message_store() -> tuple[MessageStore, RecordingDriver, MessageTable]:
    columns = MessageTable(task_id=Column(name="task_id", type_="Uuid"))
    driver = RecordingDriver()
    return MessageStore(SchemaStore("broker", driver), columns), driver, columns


@pytest.mark.anyio
async def test_a_message_is_inserted_with_its_channel_and_its_delay() -> None:
    store, driver, columns = _message_store()
    message = BrokerMessage(
        task_id="task-1",
        task_name="tests:work",
        message=b"payload",
        labels={"a": "b"},
    )

    inserted = await store.insert(message, "MixedChannel", 2.5)

    assert inserted == INSERTED_ID
    assert driver.calls == [
        (
            "insert",
            (
                [
                    columns.task_id,
                    columns.task_name,
                    columns.message,
                    columns.labels,
                    columns.channel,
                    columns.not_before,
                ],
                ["task-1", "tests:work", b"payload", {"a": "b"}, "MixedChannel", 2.5],
                [columns.primary_key],
            ),
        ),
    ]


@pytest.mark.anyio
async def test_a_notify_sends_the_folded_channel_and_the_id_as_text() -> None:
    store, driver, _ = _message_store()
    with warnings.catch_warnings():
        # A mixed-case channel warns; the fold is what the call carries.
        warnings.simplefilter("ignore")
        channel = Identifier.parse("MixedChannel", what="channel name")

    await store.notify(channel, 7)

    assert driver.calls == [("execute", (notify_statement().text, "mixedchannel", "7"))]


@pytest.mark.anyio
async def test_a_due_claim_runs_the_claim_query_on_the_raw_channel() -> None:
    store, driver, columns = _message_store()
    driver.fetch_row_returns = {"message": b"claimed"}
    claim_columns = ClaimColumns(channel=columns.channel, not_before=columns.not_before)
    query = ClaimDueQuery(driver.table).make_query(
        columns.primary_key,
        claim_columns,
        [columns.message],
    )

    row = await store.claim_due("MixedChannel")

    assert row == {"message": b"claimed"}
    assert driver.calls == [("fetch_row", (query, "MixedChannel"))]
    assert store.claim_due_statement.text == query


@pytest.mark.anyio
async def test_a_claim_by_id_runs_the_claim_query_with_the_id() -> None:
    store, driver, columns = _message_store()
    claim_columns = ClaimColumns(channel=columns.channel, not_before=columns.not_before)
    query = ClaimByIdQuery(driver.table).make_query(
        columns.primary_key,
        claim_columns,
        [columns.message],
    )

    row = await store.claim_by_id("MixedChannel", 42)

    assert row is None
    assert driver.calls == [("fetch_row", (query, "MixedChannel", 42))]
    assert store.claim_by_id_statement.text == query


def test_configure_sets_the_kind_and_the_query_timeout_on_the_driver() -> None:
    store, driver, _ = _message_store()

    store.configure(QUERY_TIMEOUT)

    assert driver.component_kind == "broker"
    assert driver.query_timeout == QUERY_TIMEOUT


@pytest.mark.anyio
async def test_a_message_store_binds_starts_and_closes_its_schema_store() -> None:
    store, driver, _ = _message_store()

    store.bind("postgresql://postgres@localhost:5432/other")
    await store.start()
    await store.close()

    assert driver.connection_string == "postgresql://postgres@localhost:5432/other"
    assert driver.component_kind == "broker"
    assert [name for name, _ in driver.calls] == ["on_startup", "on_shutdown"]


def test_a_message_store_follows_a_driver_that_was_swapped() -> None:
    store, driver, _ = _message_store()
    other = RecordingDriver("other_messages")

    assert store.driver is driver

    store.driver = other

    assert store.driver is other
    assert store.schema.driver is other
