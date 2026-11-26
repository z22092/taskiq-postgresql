"""
The schema store: startup, migrate and catalog reads over one query driver.

A driver that records what it is asked stands in for a real one, so no test
here needs a database.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import pytest
from typing_extensions import override

from taskiq_postgresql import migrations
from taskiq_postgresql.abc.driver import QueryDriver
from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.core.sql.catalog import (
    AddedIndex,
    Column,
    CreatedAtColumn,
    PrimaryKeyColumn,
    database_read,
)
from taskiq_postgresql.core.stores import schema as schema_module
from taskiq_postgresql.core.stores.declaration import TableDeclaration
from taskiq_postgresql.core.stores.schema import (
    SchemaStore,
    read_database,
    schema_store,
)
from taskiq_postgresql.core.stores.startup import Session, start_table

DSN = "postgresql://postgres@localhost:5432/postgres"
OTHER_DSN = "postgresql://postgres@localhost:5432/other"


class FakeDriver(QueryDriver):
    """A query driver that records its calls and the flag it saw at startup."""

    def __init__(self, table: str = "notes", **kwargs: Any) -> None:
        key = PrimaryKeyColumn(name="id", type_="SERIAL")
        super().__init__(
            DSN,
            table,
            columns=[Column("body", "TEXT")],
            primary_key=key,
            index_columns=[key],
            **kwargs,
        )
        self.calls: list[str] = []
        self.seen_at_startup: Optional[tuple[bool, str, Any]] = None
        self.startup_error: Optional[BaseException] = None
        self.row: Optional[dict[str, Any]] = None
        self.fetched: list[str] = []

    @override
    async def __aenter__(self) -> Any:
        return self

    @override
    async def __aexit__(self, *exc_info: object) -> None:
        return None

    @override
    async def insert(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def insert_or_update(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def fetch_row(self, query: str, *values: Any) -> Optional[dict[str, Any]]:
        self.fetched.append(query)
        return self.row

    @override
    async def delete_by_date(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def select(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def exists(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @override
    async def on_startup(self) -> None:
        self.calls.append("on_startup")
        self.seen_at_startup = (
            self.run_migrations,
            self.connection_string,
            self.component_kind,
        )
        if self.startup_error is not None:
            raise self.startup_error

    @override
    async def on_shutdown(self) -> None:
        self.calls.append("on_shutdown")


class PooledDriver(FakeDriver):
    """A fake whose pool is open."""

    pool = object()


@pytest.mark.anyio
async def test_migrate_turns_the_flag_on_only_while_it_starts() -> None:
    driver = FakeDriver(run_migrations=False)

    await SchemaStore("broker", driver).migrate(OTHER_DSN)

    assert driver.seen_at_startup == (True, OTHER_DSN, None)
    assert driver.run_migrations is False
    assert driver.component_kind is None
    assert driver.connection_string == OTHER_DSN
    assert driver.calls == ["on_startup"]


@pytest.mark.anyio
async def test_migrate_restores_the_flag_when_the_startup_fails() -> None:
    driver = FakeDriver(run_migrations=True)
    driver.startup_error = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await SchemaStore("broker", driver).migrate(OTHER_DSN)

    assert driver.run_migrations is True
    assert driver.calls == ["on_startup"]


@pytest.mark.anyio
async def test_migrate_closes_the_pool_it_left_open() -> None:
    driver = PooledDriver()

    await SchemaStore("broker", driver).migrate(OTHER_DSN)

    assert driver.calls == ["on_startup", "on_shutdown"]


@pytest.mark.anyio
async def test_migrate_closes_the_pool_when_the_startup_fails() -> None:
    driver = PooledDriver(run_migrations=True)
    driver.startup_error = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await SchemaStore("broker", driver).migrate(OTHER_DSN)

    assert driver.run_migrations is True
    assert driver.calls == ["on_startup", "on_shutdown"]


def test_bind_sets_the_dsn_and_the_kind() -> None:
    driver = FakeDriver()

    SchemaStore("result_backend", driver).bind(OTHER_DSN)

    assert driver.connection_string == OTHER_DSN
    assert driver.component_kind == "result_backend"


@pytest.mark.anyio
async def test_start_and_close_call_the_driver() -> None:
    driver = FakeDriver()
    store = SchemaStore("scheduler_source", driver)

    await store.start()
    await store.close()

    assert driver.calls == ["on_startup", "on_shutdown"]


def test_the_declaration_follows_a_driver_that_was_swapped() -> None:
    first, second = FakeDriver("first_notes"), FakeDriver("second_notes")
    store = SchemaStore("broker", first)

    assert store.declaration.table is first.table

    store.driver = second

    assert store.declaration.table is second.table
    assert store.schema.name == "second_notes"


def test_the_schema_is_the_declared_one() -> None:
    driver = FakeDriver()
    store = SchemaStore("result_backend", driver)

    assert store.schema == migrations.declared_schema(
        "result_backend",
        TableDeclaration.of_driver(driver),
    )


def test_build_makes_the_driver_the_declaration_describes() -> None:
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    body = Column("body", "TEXT")
    created_at = CreatedAtColumn()
    added = Column("extra", "TEXT", nullable=True)
    index = AddedIndex(name="notes_extra_idx", columns=[added])
    declaration = TableDeclaration(
        table=FakeDriver().table,
        primary_key=key,
        columns=[body],
        created_at=created_at,
        index_columns=[key],
        added_columns=[added],
        added_indexes=[index],
    )

    store = SchemaStore.build(
        "broker",
        declaration,
        "asyncpg",
        DSN,
        run_migrations=True,
    )

    driver = store.driver
    assert driver.connection_string == DSN
    assert driver.component_kind == "broker"
    assert driver.run_migrations is True
    assert TableDeclaration.of_driver(driver) == declaration
    assert driver.primary_key is key
    assert driver.created_at is created_at
    assert store.kind == "broker"


class _Connection:
    """The statements a ``Session`` ran, with the value its probe answers."""

    def __init__(self, probe_answer: Any) -> None:
        self.probe_answer = probe_answer
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.fetched: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch_value(self, query: str, *values: Any) -> Any:
        self.fetched.append((query, values))
        return self.probe_answer

    async def execute(self, query: str, *values: Any) -> Any:
        self.executed.append((query, values))


def _session(connection: _Connection) -> Any:
    @asynccontextmanager
    async def open_session() -> AsyncGenerator[Session, None]:
        yield Session(fetch_value=connection.fetch_value, execute=connection.execute)

    return open_session


async def _no_catalog(query: str) -> Any:
    raise AssertionError("no catalog read is expected")


@pytest.mark.anyio
async def test_a_table_is_created_with_the_statements_the_driver_builds() -> None:
    # Without a description to compare with, a migrating start probes the
    # relations, then runs the lock, the table, the index and the added columns.
    key = PrimaryKeyColumn(name="id", type_="SERIAL")
    claim = Column("channel", "VARCHAR", nullable=True)
    index = AddedIndex(name="notes_channel_idx", columns=[claim, key])
    driver = FakeDriver(added_columns=[claim], added_indexes=[index])
    connection = _Connection(probe_answer=False)

    await start_table(
        TableDeclaration.of_driver(driver),
        None,
        run_migrations=True,
        fetch_value=_no_catalog,
        session=_session(connection),
    )

    probe = driver.relations_exist_query
    args = (driver.index_columns,)
    options = {
        "added_columns": driver.added_columns,
        "added_indexes": driver.added_indexes,
    }
    assert connection.fetched == [
        (probe.make_query(*args, **options), tuple(probe.params(*args, **options))),
    ]
    assert connection.executed == [
        (
            driver.migration_lock_query.make_query(),
            tuple(driver.migration_lock_query.params()),
        ),
        (driver.create_table_query.make_query(), ()),
        (driver.create_index_query.make_query(driver.index_columns), ()),
        (
            driver.add_columns_query.make_query(
                driver.added_columns,
                driver.added_indexes,
            ),
            (),
        ),
    ]


@pytest.mark.anyio
async def test_a_current_table_runs_no_ddl() -> None:
    driver = FakeDriver()
    connection = _Connection(probe_answer=True)

    await start_table(
        TableDeclaration.of_driver(driver),
        None,
        run_migrations=True,
        fetch_value=_no_catalog,
        session=_session(connection),
    )

    assert connection.executed == []


@pytest.mark.anyio
async def test_nothing_is_checked_without_a_description_and_without_migrating() -> None:
    driver = FakeDriver()
    connection = _Connection(probe_answer=False)

    await start_table(
        TableDeclaration.of_driver(driver),
        None,
        run_migrations=False,
        fetch_value=_no_catalog,
        session=_session(connection),
    )

    assert (connection.fetched, connection.executed) == ([], [])


@pytest.mark.anyio
async def test_read_database_with_a_store_reads_on_its_driver_and_closes_the_pool() -> (
    None
):
    driver = PooledDriver()
    driver.row = {"database": "db", "search_path": "public", "catalog": None}
    schemas = [migrations.table_schema("result_backend")]

    row = await read_database(OTHER_DSN, schemas, SchemaStore("broker", driver))

    assert row == driver.row
    assert driver.connection_string == OTHER_DSN
    assert driver.fetched == [database_read(migrations.catalog_tables(schemas)).text]
    assert driver.calls == ["on_shutdown"]


@pytest.mark.anyio
async def test_read_database_without_a_store_uses_a_catalog_only_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = PooledDriver()
    driver.row = {"database": "db"}
    asked: list[str] = []

    def catalog_driver(dsn: str) -> QueryDriver:
        asked.append(dsn)
        return driver

    monkeypatch.setattr(schema_module, "_catalog_driver", catalog_driver)

    row = await read_database(OTHER_DSN, [migrations.table_schema("broker")])

    assert row == {"database": "db"}
    assert asked == [OTHER_DSN]
    assert driver.connection_string == OTHER_DSN
    assert driver.calls == ["on_shutdown"]


@pytest.mark.anyio
async def test_read_database_leaves_a_pool_that_never_opened() -> None:
    driver = FakeDriver()

    await read_database(
        OTHER_DSN,
        [migrations.table_schema("broker")],
        SchemaStore("broker", driver),
    )

    assert driver.calls == []


def test_the_catalog_only_driver_reads_no_table_of_its_own() -> None:
    driver = schema_module._catalog_driver(DSN)

    assert driver.table_name == "taskiq_catalog"
    assert list(driver.columns) == []
    assert driver.connection_string == DSN


def test_schema_store_of_a_broker_is_its_store_and_follows_a_swapped_driver() -> None:
    broker = PostgresqlBroker(DSN)
    store = schema_store(broker)

    assert store is broker._store.schema
    assert store.kind == "broker"
    assert store.driver is broker.driver

    other = FakeDriver("other_messages")
    broker.driver = other

    assert schema_store(broker) is store
    assert store.driver is other
