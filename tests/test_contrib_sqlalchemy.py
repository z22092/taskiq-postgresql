"""
The SQLAlchemy and SQLModel integrations, in ``taskiq_postgresql.contrib``.

The parity tests create one table with ``run_migrations=True`` in an empty
database and with ``MetaData.create_all`` in another, under the same names, and
compare what the two catalogs hold: columns, indexes, constraints and
sequences.
"""

from __future__ import annotations

import asyncio
import importlib
import random
import string
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, get_args

import asyncpg
import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL, make_url
from sqlalchemy.schema import CreateTable
from sqlmodel import SQLModel

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.contrib import sqlmodel as sqlmodel_contrib
from taskiq_postgresql.contrib.sqlalchemy import (
    METADATA_INFO_KEY,
    add_taskiq_tables,
    taskiq_metadata,
)
from taskiq_postgresql.migrations import (
    ColumnSchema,
    TableSchema,
    TypeName,
    table_schema,
)
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import create_database, drop_database, in_database
from tests.test_migration_script import SHAPE_IDS, SHAPES, build_component
from tests.test_migrations import close_component

UNUSED_DSN = "postgresql://localhost/unused"
KINDS = ("broker", "result_backend", "scheduler_source")
# Mixed case, and long enough for PostgreSQL to shorten the names it derives.
LONG_NAME = "Taskiq_" + "Long" * 13
CASES = [
    *((kind, field, None) for kind, field in SHAPES),
    ("result_backend", "Text", LONG_NAME),
]
CASE_IDS = [*SHAPE_IDS, "long-mixed-case-name"]
# A naming convention like the one Alembic's documentation suggests; the
# integration names every object itself, so it must not rename any.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
CATALOG_QUERIES = {
    "columns": (
        "SELECT table_name, column_name, ordinal_position, data_type, udt_name,"
        " character_maximum_length, is_nullable, column_default, is_identity,"
        " pg_get_serial_sequence(quote_ident(table_name), column_name)"
        " FROM information_schema.columns WHERE table_schema = current_schema()"
        " ORDER BY table_name, ordinal_position"
    ),
    "indexes": (
        "SELECT tablename, indexname, indexdef FROM pg_indexes"
        " WHERE schemaname = current_schema() ORDER BY indexname"
    ),
    "constraints": (
        "SELECT conrelid::regclass::text, conname, contype,"
        " pg_get_constraintdef(oid) FROM pg_constraint"
        " WHERE connamespace = current_schema()::regnamespace ORDER BY conname"
    ),
    "sequences": (
        "SELECT sequencename, data_type::text, start_value, min_value, max_value,"
        " increment_by, cycle, cache_size FROM pg_sequences"
        " WHERE schemaname = current_schema() ORDER BY sequencename"
    ),
}
EXPECTED_TYPES = {
    "serial": "INTEGER",
    "uuid": "UUID",
    "varchar": "VARCHAR(7)",
    "text": "TEXT",
    "bytea": "BYTEA",
    "jsonb": "JSONB",
    "timestamptz": "TIMESTAMP WITH TIME ZONE",
}


def sqlalchemy_url(dsn: str, driver: str = "psycopg") -> URL:
    """Return ``dsn`` as a SQLAlchemy URL for ``driver``'s dialect."""
    return make_url(dsn).set(drivername=f"postgresql+{driver}")


def create_all(metadata: MetaData, dsn: str) -> None:
    engine = create_engine(sqlalchemy_url(dsn))
    try:
        metadata.create_all(engine)
    finally:
        engine.dispose()


def run_migrations(
    dsn: str,
    kind: str,
    *,
    field: Optional[str] = None,
    table_name: Optional[str] = None,
    driver: str = "asyncpg",
) -> None:
    """Create a component's table the way a worker does: ``startup()``."""
    component = build_component(
        kind,
        dsn=dsn,
        table_name=table_name,
        field=field,
        driver=driver,
    )

    async def start() -> None:
        try:
            await component.startup()
        finally:
            await close_component(component)

    asyncio.run(start())


def catalog(dsn: str) -> dict[str, list[tuple[Any, ...]]]:
    """Return what the catalog of the database ``dsn`` holds, query by query."""

    async def fetch() -> dict[str, list[tuple[Any, ...]]]:
        connection = await asyncpg.connect(dsn)
        try:
            return {
                name: [tuple(row) for row in await connection.fetch(query)]
                for name, query in CATALOG_QUERIES.items()
            }
        finally:
            await connection.close()

    return asyncio.run(fetch())


def block(monkeypatch: pytest.MonkeyPatch, package: str) -> None:
    """Make ``package`` and every submodule of it unimportable, as uninstalled."""
    loaded = [name for name in sys.modules if name.startswith(f"{package}.")]
    for name in (package, *loaded):
        monkeypatch.setitem(sys.modules, name, None)


def tables_in(dsn: str) -> set[str]:
    return {row[0] for row in catalog(dsn)["columns"]}


def databases(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Yield a function that creates an empty database; drop them all after."""
    created: list[str] = []

    def create() -> str:
        name = "contrib_" + "".join(random.choices(string.ascii_lowercase, k=10))
        asyncio.run(create_database(postgresql_dsn, name))
        created.append(name)
        return in_database(postgresql_dsn, name)

    yield create
    for name in created:
        asyncio.run(drop_database(postgresql_dsn, name))


@pytest.fixture
def new_database(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Create empty databases for one test, and drop them after it."""
    yield from databases(postgresql_dsn)


# --- against PostgreSQL ------------------------------------------------------


@pytest.mark.parametrize(("kind", "field", "table_name"), CASES, ids=CASE_IDS)
def test_create_all_builds_the_catalog_run_migrations_builds(
    kind: str,
    field: Optional[str],
    table_name: Optional[str],
    driver: str,
    new_database: Callable[[], str],
) -> None:
    migrated, created = new_database(), new_database()
    run_migrations(
        migrated,
        kind,
        field=field,
        table_name=table_name,
        driver=driver,
    )
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    add_taskiq_tables(
        metadata,
        table_schema(kind, table_name=table_name, field_for_task_id=field),
    )

    create_all(metadata, created)

    expected = catalog(migrated)
    assert all(expected[name] for name in ("columns", "indexes", "constraints"))
    assert catalog(created) == expected


def test_sqlmodel_metadata_holds_the_tables_and_creates_them(
    new_database: Callable[[], str],
) -> None:
    name = "sqlmodel_" + "".join(random.choices(string.ascii_lowercase, k=10))
    schema = table_schema("scheduler_source", table_name=name)
    (table,) = sqlmodel_contrib.add_taskiq_tables(schema)
    try:
        assert SQLModel.metadata.tables[name] is table
        migrated, created = new_database(), new_database()
        run_migrations(migrated, "scheduler_source", table_name=name)

        create_all(SQLModel.metadata, created)

        assert name in tables_in(created)
        assert catalog(created) == catalog(migrated)
    finally:
        SQLModel.metadata.remove(table)


# --- the tables, without a server ---------------------------------------------


def test_sqlmodel_takes_another_metadata() -> None:
    metadata = MetaData()

    tables = sqlmodel_contrib.add_taskiq_tables(
        table_schema("broker"),
        metadata=metadata,
    )

    assert list(metadata.tables) == ["taskiq_messages"]
    assert [table.name for table in tables] == ["taskiq_messages"]
    assert "taskiq_messages" not in SQLModel.metadata.tables


def test_every_type_name_has_a_sqlalchemy_type() -> None:
    assert set(EXPECTED_TYPES) == set(get_args(TypeName))
    schema = TableSchema(
        kind="broker",
        name="every_type",
        columns=tuple(
            ColumnSchema(
                name=f"{type_name}_column",
                sql_type=type_name,
                type_name=type_name,
                length=7 if type_name == "varchar" else None,
                nullable=type_name != "serial",
                default=None,
                primary_key=type_name == "serial",
            )
            for type_name in get_args(TypeName)
        ),
        indexes=(),
        steps=(),
    )

    (table,) = add_taskiq_tables(MetaData(), schema)

    dialect = postgresql.dialect()
    assert {
        column.name.removesuffix("_column"): str(column.type.compile(dialect=dialect))
        for column in table.columns
    } == EXPECTED_TYPES
    # SQLAlchemy writes SERIAL for an integer primary key that autoincrements.
    assert "serial_column SERIAL NOT NULL" in str(
        CreateTable(table).compile(dialect=dialect),
    )


def test_a_type_without_a_sqlalchemy_type_is_refused() -> None:
    broker = table_schema("broker")
    money = ColumnSchema("price", "MONEY", "money", None, False, None, False)  # type: ignore[arg-type]
    schema = TableSchema(
        kind="broker",
        name="priced",
        columns=(*broker.columns, money),
        indexes=(),
        steps=(),
    )

    with pytest.raises(ValueError, match=r"'price' has the type 'money'"):
        add_taskiq_tables(MetaData(), schema)


def test_the_table_is_the_description() -> None:
    metadata = taskiq_metadata(table_schema("scheduler_source"))

    table = metadata.tables["taskiq_schedulers"]
    assert [
        (column.name, column.nullable, column.primary_key) for column in table.columns
    ] == [
        ("id", False, True),
        ("task_name", False, False),
        ("schedule", False, False),
        ("updated_at", False, False),
        ("created_at", False, False),
    ]
    assert table.c.task_name.type.length == 100  # noqa: PLR2004
    assert str(table.c.created_at.server_default.arg) == "NOW()"  # type: ignore[union-attr]
    assert table.primary_key.name == "taskiq_schedulers_pkey"
    assert sorted(
        (index.name, [column.name for column in index.columns])
        for index in table.indexes
    ) == [
        ("taskiq_schedulers_id_idx", ["id"]),
        ("taskiq_schedulers_task_name_idx", ["task_name"]),
    ]
    assert {
        index.dialect_options["postgresql"]["using"] for index in table.indexes
    } == {"hash"}


def test_the_metadata_keeps_each_description_once() -> None:
    metadata = MetaData()
    broker, backend = table_schema("broker"), table_schema("result_backend")

    add_taskiq_tables(metadata, broker, backend)
    add_taskiq_tables(metadata, broker)

    assert metadata.info[METADATA_INFO_KEY] == (broker, backend)
    assert taskiq_metadata(broker).info[METADATA_INFO_KEY] == (broker,)


def test_names_are_the_ones_postgresql_gives_an_unquoted_name() -> None:
    (table,) = add_taskiq_tables(MetaData(), table_schema("broker", table_name="Jobs"))

    assert table.name == "jobs"
    assert table.primary_key.name == "jobs_pkey"
    assert sorted(index.name for index in table.indexes) == [
        "jobs_id_idx",
        "jobs_not_before_idx",
    ]


def test_long_names_are_shortened_as_postgresql_shortens_them() -> None:
    (table,) = add_taskiq_tables(
        MetaData(),
        table_schema("result_backend", table_name=LONG_NAME),
    )

    folded = LONG_NAME.lower()
    assert table.name == folded
    assert table.primary_key.name == f"{folded[:58]}_pkey"
    assert [index.name for index in table.indexes] == [
        f"{folded}_task_id_idx"[:63],
    ]


@pytest.mark.parametrize(
    "name",
    ["public.jobs", "jobs-table", "1jobs", "o'brien"],
    ids=["schema-qualified", "dash", "leading-digit", "quote"],
)
def test_a_name_that_is_not_a_plain_identifier_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match="not a plain SQL identifier"):
        add_taskiq_tables(MetaData(), table_schema("broker", table_name=name))


def test_targets_are_found_as_collect_schemas_finds_them() -> None:
    broker = PostgresqlBroker(dsn=UNUSED_DSN, table_name="m")
    source = PostgresqlSchedulerSource(dsn=UNUSED_DSN, table_name="s")
    metadata = MetaData()

    tables = add_taskiq_tables(
        metadata,
        broker,
        table_schema("result_backend", table_name="r"),
        source,
        broker,
    )

    # The broker named twice brings its table once.
    assert [table.name for table in tables] == ["m", "r", "s"]
    assert sorted(metadata.tables) == ["m", "r", "s"]


def test_an_import_path_is_loaded_with_app_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = "tkq_" + "".join(random.choices(string.ascii_lowercase, k=10))
    (tmp_path / f"{module}.py").write_text(
        "from taskiq_postgresql import PostgresqlResultBackend\n"
        f"backend = PostgresqlResultBackend(dsn={UNUSED_DSN!r}, table_name='r')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "path", list(sys.path))
    try:
        metadata = taskiq_metadata(f"{module}:backend", app_dir=str(tmp_path))
    finally:
        sys.modules.pop(module, None)

    assert list(metadata.tables) == ["r"]


def test_the_same_table_already_on_the_metadata_is_reused() -> None:
    metadata = MetaData()
    (first,) = add_taskiq_tables(metadata, table_schema("broker"))

    (second,) = add_taskiq_tables(metadata, table_schema("broker"))

    assert second is first
    assert list(metadata.tables) == ["taskiq_messages"]


def test_another_table_of_the_same_name_is_refused() -> None:
    metadata = MetaData()
    Table("taskiq_messages", metadata, Column("id", Integer, primary_key=True))
    uuid_ids = MetaData()
    add_taskiq_tables(uuid_ids, table_schema("result_backend"))

    with pytest.raises(ValueError, match="'taskiq_messages'"):
        add_taskiq_tables(metadata, table_schema("broker"))
    with pytest.raises(ValueError, match="'taskiq_results'"):
        add_taskiq_tables(
            uuid_ids,
            table_schema("result_backend", field_for_task_id="Text"),
        )


def test_no_target_is_refused() -> None:
    with pytest.raises(TypeError, match="at least one target"):
        add_taskiq_tables(MetaData())
    with pytest.raises(TypeError, match="at least one target"):
        taskiq_metadata()


@pytest.mark.parametrize(
    ("module", "dependency"),
    [
        ("taskiq_postgresql.contrib.sqlalchemy", "sqlalchemy"),
        ("taskiq_postgresql.contrib.sqlmodel", "sqlmodel"),
        ("taskiq_postgresql.contrib.alembic", "alembic"),
    ],
    ids=["sqlalchemy", "sqlmodel", "alembic"],
)
def test_a_contrib_module_without_its_extra_says_what_to_install(
    module: str,
    dependency: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block(monkeypatch, dependency)
    monkeypatch.delitem(sys.modules, module, raising=False)

    with pytest.raises(ImportError) as missing:
        importlib.import_module(module)

    assert str(missing.value) == (
        f"{dependency} is not installed. Please install it with "
        f"`pip install taskiq-postgresql[{dependency}]`."
    )


def test_the_package_imports_no_integration() -> None:
    probe = (
        "import sys\n"
        "import taskiq_postgresql.cli, taskiq_postgresql.migrations\n"
        "watched = ('sqlalchemy', 'sqlmodel', 'alembic', 'taskiq_postgresql.contrib')\n"
        "print(sorted(name for name in watched if name in sys.modules))\n"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )

    assert result.stdout == "[]\n"


def test_kinds_and_components_give_the_same_tables(driver: str) -> None:
    dialect = postgresql.dialect()
    for kind, field in SHAPES:
        component = build_component(kind, table_name="jobs", field=field, driver=driver)
        (from_component,) = add_taskiq_tables(MetaData(), component)
        (from_kind,) = add_taskiq_tables(
            MetaData(),
            table_schema(kind, table_name="jobs", field_for_task_id=field),
        )
        assert str(CreateTable(from_component).compile(dialect=dialect)) == str(
            CreateTable(from_kind).compile(dialect=dialect),
        )
