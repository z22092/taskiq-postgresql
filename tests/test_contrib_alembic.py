"""
The Alembic integration, ``taskiq_postgresql.contrib.alembic``.

Autogenerate runs against databases that ``run_migrations=True`` set up. The
revision tests run the ``alembic`` command itself, in a throwaway Alembic
environment, through a sync (psycopg) and an async (asyncpg) SQLAlchemy
dialect: asyncpg prepares every statement it sends, so a script of several
statements in one call fails there. ``test_contrib_alembic_autogenerate.py``
covers the migration steps autogenerate writes.

Every test here carries the ``alembic`` mark, so CI can run them alone on
other Alembic versions: ``pytest -m alembic``.
"""

from __future__ import annotations

import ast
import asyncio
import io
import json
import os
import re
import subprocess
import sys
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence

import alembic as alembic_package
import pytest
from alembic import op
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Connection, Integer, MetaData, Table, create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from taskiq_postgresql import migrations
from taskiq_postgresql.cli import PostgresqlCMD
from taskiq_postgresql.contrib.alembic import (
    alembic_format,
    exclude_taskiq_tables,
    taskiq_metadata,
    upgrade_taskiq_tables,
)
from taskiq_postgresql.migrations import (
    DEFAULT_LOCK_TIMEOUT,
    MIGRATION_FORMATS_GROUP,
    FormatOptions,
    MigrationFormatOptionError,
    TableSchema,
    available_formats,
    load_format,
    migration_statements,
    render_sql,
    table_schema,
)
from tests.helpers import bounded, run_script
from tests.test_cli import taskiq
from tests.test_contrib_sqlalchemy import (
    CASE_IDS,
    CASES,
    KINDS,
    block,
    create_all,
    databases,
    run_migrations,
    sqlalchemy_url,
    tables_in,
)
from tests.test_migration_formats import register
from tests.test_migrations import BROKER_TABLE_0_4_SQL

pytestmark = pytest.mark.alembic

TIMEOUT = 60
DEFAULT_TABLES = {"taskiq_messages", "taskiq_results", "taskiq_schedulers"}
SQLALCHEMY_DRIVERS = ("psycopg", "asyncpg")
URL_VARIABLE = "TASKIQ_TEST_ALEMBIC_URL"
# JSON list of [kind, table name] pairs: the tables env.py hands autogenerate.
TABLES_VARIABLE = "TASKIQ_TEST_ALEMBIC_TABLES"
# Set to make taskiq_postgresql unimportable, as on a host without it.
BLOCK_VARIABLE = "TASKIQ_TEST_ALEMBIC_BLOCK"
# A minimal Alembic environment: the standard online and offline paths, and
# the async one for the asyncpg dialect. The URL comes from the environment, so
# no credential is written to disk.
ENV_PY = f"""\
import asyncio
import json
import os
import sys

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.ext.asyncio import create_async_engine

url = os.environ[{URL_VARIABLE!r}]
tables = json.loads(os.environ.get({TABLES_VARIABLE!r}, "[]"))
if os.environ.get({BLOCK_VARIABLE!r}):
    sys.modules["taskiq_postgresql"] = None
target_metadata = None
if tables:
    from taskiq_postgresql.contrib.alembic import taskiq_metadata
    from taskiq_postgresql.migrations import table_schema

    target_metadata = taskiq_metadata(
        *(table_schema(kind, table_name=name) for kind, name in tables),
    )


def run(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async():
    engine = create_async_engine(url, poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(run)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=url, literal_binds=True, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()
elif "+asyncpg" in url:
    asyncio.run(run_async())
else:
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        run(connection)
"""
ALEMBIC_INI = "[alembic]\nscript_location = %(here)s/migrations\n"


def default_schemas() -> list[TableSchema]:
    return [table_schema(kind) for kind in KINDS]


def run_upgrade(
    dsn: str,
    sqlalchemy_driver: str,
    upgrade: Callable[[], None],
) -> None:
    """Run ``upgrade`` as Alembic runs a revision: online, in a transaction."""

    def in_context(connection: Connection) -> None:
        with Operations.context(MigrationContext.configure(connection)):
            upgrade()

    url = sqlalchemy_url(dsn, sqlalchemy_driver)
    if sqlalchemy_driver == "asyncpg":

        async def run_async() -> None:
            engine = create_async_engine(url)
            try:
                async with engine.begin() as connection:
                    await connection.run_sync(in_context)
            finally:
                await engine.dispose()

        asyncio.run(run_async())
        return
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            in_context(connection)
    finally:
        engine.dispose()


def offline_sql(upgrade: Callable[[], None]) -> str:
    """Return what ``upgrade`` prints under ``alembic upgrade --sql``."""
    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": buffer},
    )
    with Operations.context(context):
        upgrade()
    return buffer.getvalue()


def autogenerate(
    dsn: str,
    metadata: Any,
    **options: Any,
) -> list[Any]:
    engine = create_engine(sqlalchemy_url(dsn))
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection, opts=options)
            return compare_metadata(context, metadata)
    finally:
        engine.dispose()


def revision_namespace(source: str) -> dict[str, Any]:
    """Execute a revision file as Alembic loads it, and return its globals."""
    namespace: dict[str, Any] = {"__name__": "revision"}
    exec(compile(source, "revision.py", "exec"), namespace)  # noqa: S102
    return namespace


@pytest.fixture
def new_database(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Create empty databases for one test, and drop them after it."""
    yield from databases(postgresql_dsn)


def make_alembic_project(path: Path) -> Path:
    """Write an Alembic environment with an empty ``versions`` directory."""
    (path / "alembic.ini").write_text(ALEMBIC_INI, encoding="utf-8")
    (path / "migrations" / "versions").mkdir(parents=True)
    (path / "migrations" / "env.py").write_text(ENV_PY, encoding="utf-8")
    # The template `alembic init` copies, for `alembic revision`.
    template = Path(alembic_package.__file__).parent / "templates" / "generic"
    (path / "migrations" / "script.py.mako").write_text(
        (template / "script.py.mako").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def alembic_project(tmp_path: Path) -> Path:
    """An Alembic environment with an empty ``versions`` directory."""
    return make_alembic_project(tmp_path)


def alembic(
    project: Path,
    url: str,
    *args: str,
    tables: Sequence[tuple[str, str]] = (),
    block: bool = False,
) -> subprocess.CompletedProcess[str]:
    """
    Run the ``alembic`` command on ``project``, connecting to ``url``.

    ``tables`` become env.py's ``target_metadata``, and ``block`` makes
    taskiq_postgresql unimportable in the command.
    """
    environment = {**os.environ, URL_VARIABLE: url, TABLES_VARIABLE: json.dumps(tables)}
    if block:
        environment[BLOCK_VARIABLE] = "1"
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", "-c", str(project / "alembic.ini"), *args],
        env=environment,
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=False,
    )


# --- autogenerate ------------------------------------------------------------


@pytest.mark.parametrize(("kind", "field", "table_name"), CASES, ids=CASE_IDS)
def test_autogenerate_finds_nothing_to_change_after_run_migrations(
    kind: str,
    field: Optional[str],
    table_name: Optional[str],
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    run_migrations(dsn, kind, field=field, table_name=table_name, driver=driver)
    metadata = taskiq_metadata(
        table_schema(kind, table_name=table_name, field_for_task_id=field),
    )

    assert autogenerate(dsn, metadata) == []
    assert autogenerate(dsn, metadata, compare_server_default=True) == []


def test_autogenerate_proposes_every_step_on_an_empty_database(
    new_database: Callable[[], str],
) -> None:
    diffs = autogenerate(new_database(), taskiq_metadata(*default_schemas()))

    # One operation per migration step, the tables in name order, in place of
    # the add_table Alembic would propose.
    assert diffs == [
        ("taskiq_postgresql_step", "taskiq_messages", 1),
        ("taskiq_postgresql_step", "taskiq_messages", 2),
        ("taskiq_postgresql_step", "taskiq_results", 1),
        ("taskiq_postgresql_step", "taskiq_results", 2),
        ("taskiq_postgresql_step", "taskiq_schedulers", 1),
    ]


def test_the_filter_keeps_autogenerate_from_dropping_the_tables(
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    for kind in KINDS:
        run_migrations(dsn, kind, driver=driver)
    application = MetaData()
    Table("app_users", application, Column("id", Integer, primary_key=True))
    create_all(application, dsn)
    include_object = exclude_taskiq_tables(*default_schemas())

    unfiltered = autogenerate(dsn, application)
    filtered = autogenerate(
        dsn,
        application,
        include_object=include_object,
        compare_server_default=True,
    )

    # Without the filter, the next revision would drop the tables.
    assert {diff[0] for diff in unfiltered} == {"remove_table", "remove_index"}
    assert {diff[1].name for diff in unfiltered if diff[0] == "remove_table"} == (
        DEFAULT_TABLES
    )
    # With it, nothing: no table, index, column or sequence of ours.
    assert filtered == []


def test_the_filter_keeps_tables_of_the_same_name_in_other_schemas() -> None:
    include_object = exclude_taskiq_tables(table_schema("broker"))
    ours = Table("taskiq_messages", MetaData(), Column("id", Integer))
    elsewhere = Table("taskiq_messages", MetaData(), Column("id", Integer), schema="x")
    unrelated = Table("app_users", MetaData(), Column("id", Integer))

    assert not include_object(ours, ours.name, "table", True, None)
    assert not include_object(ours.c.id, "id", "column", True, None)
    assert include_object(elsewhere, elsewhere.name, "table", True, None)
    assert include_object(unrelated, unrelated.name, "table", True, None)


# --- upgrade_taskiq_tables in a hand-written revision -------------------------


def upgrade_default_tables() -> None:
    """What a hand-written revision runs for the default tables of this release."""
    broker, *others = default_schemas()
    upgrade_taskiq_tables(op, broker, through=2)
    upgrade_taskiq_tables(op, *others, through=1)


def default_statements() -> list[str]:
    broker, *others = default_schemas()
    # Each call bounds the lock waits of its own statements.
    return [
        *bounded(migration_statements([broker], through=2)),
        *bounded(migration_statements(others, through=1)),
    ]


@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
def test_upgrade_taskiq_tables_runs_in_a_revision_twice(
    sqlalchemy_driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()

    run_upgrade(dsn, sqlalchemy_driver, upgrade_default_tables)
    run_upgrade(dsn, sqlalchemy_driver, upgrade_default_tables)

    assert tables_in(dsn) == DEFAULT_TABLES


def test_upgrade_taskiq_tables_prints_the_script_offline(
    driver: str,
    new_database: Callable[[], str],
) -> None:
    printed = offline_sql(upgrade_default_tables)

    assert printed == "".join(f"{statement};\n\n" for statement in default_statements())
    # What `alembic upgrade --sql` prints runs as it is, as through psql.
    dsn = new_database()
    asyncio.run(run_script(dsn, driver, f"BEGIN;\n{printed}COMMIT;\n"))
    assert tables_in(dsn) == DEFAULT_TABLES


def test_upgrade_taskiq_tables_takes_import_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = "tkq_alembic_app"
    (tmp_path / f"{module}.py").write_text(
        "from taskiq_postgresql import PostgresqlBroker\n"
        "broker = PostgresqlBroker(dsn='postgresql://localhost/unused')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "path", list(sys.path))
    try:
        printed = offline_sql(
            lambda: upgrade_taskiq_tables(
                op,
                f"{module}:broker",
                through=2,
                app_dir=str(tmp_path),
            ),
        )
    finally:
        sys.modules.pop(module, None)

    assert printed == "".join(
        f"{statement};\n\n"
        for statement in bounded(
            migration_statements([table_schema("broker")], through=2),
        )
    )


# --- the alembic migration format --------------------------------------------


def test_the_alembic_format_is_registered() -> None:
    assert "alembic" in available_formats()
    assert load_format("alembic") is alembic_format


def step_statements(
    schemas: list[TableSchema],
    *,
    lock: bool = True,
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
) -> list[str]:
    """
    Each step of each table, by table name.

    Every step has its own lock and its own bound on lock waits.
    """
    statements = []
    for schema in sorted(schemas, key=lambda table: table.name):
        for step in schema.steps:
            locks = migration_statements([schema])[:1] if lock else []
            statements.extend(bounded([*locks, *step.statements], lock_timeout))
    return statements


def test_the_revision_runs_the_steps_and_drops_nothing_by_default() -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    source = alembic_format(schemas, FormatOptions())

    namespace = revision_namespace(source)
    assert re.fullmatch(r"[0-9a-f]{12}", namespace["revision"])
    assert namespace["down_revision"] is None
    assert namespace["branch_labels"] is None
    assert namespace["depends_on"] is None
    assert offline_sql(namespace["upgrade"]) == "".join(
        f"{statement};\n\n" for statement in step_statements(schemas)
    )
    assert offline_sql(namespace["downgrade"]) == ""
    # The SQL is written out: the revision imports nothing but alembic.
    imports = [
        line for line in source.splitlines() if line.startswith(("import ", "from "))
    ]
    assert imports == ["from alembic import op"]
    # The same tables give the same file; other tables another revision.
    assert alembic_format(schemas, FormatOptions()) == source
    assert (
        revision_namespace(alembic_format(schemas[:1], FormatOptions()))["revision"]
        != namespace["revision"]
    )


def test_the_revision_takes_its_options() -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    source = alembic_format(
        schemas,
        FormatOptions(
            transaction=False,
            lock=False,
            extra={
                "revision": "taskiq_tables",
                "down_revision": "app_0042",
                "downgrade": "drop",
            },
        ),
    )

    namespace = revision_namespace(source)
    assert (namespace["revision"], namespace["down_revision"]) == (
        "taskiq_tables",
        "app_0042",
    )
    assert offline_sql(namespace["upgrade"]) == "".join(
        f"{statement};\n\n" for statement in step_statements(schemas, lock=False)
    )
    # The drops share one bound on lock waits, as the upgrade's steps have.
    assert offline_sql(namespace["downgrade"]) == "".join(
        f"{statement};\n\n"
        for statement in bounded(
            [
                "DROP TABLE IF EXISTS taskiq_results",
                "DROP TABLE IF EXISTS taskiq_messages",
            ],
        )
    )
    assert "Revision ID: taskiq_tables\nRevises: app_0042\n" in source
    # Alembic runs every revision in its own transaction: never BEGIN/COMMIT.
    assert alembic_format(
        schemas,
        FormatOptions(transaction=True, extra={"revision": "x"}),
    ) == alembic_format(
        schemas, FormatOptions(transaction=False, extra={"revision": "x"})
    )
    ast.parse(source)


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"author": "me"}, "unknown option: author"),
        ({"downgrade": "truncate"}, "downgrade must be 'none' or 'drop'"),
        ({"revision": ""}, "revision must be"),
        ({"revision": "a-b"}, "revision must be"),
        ({"down_revision": 'x"\nimport os'}, "down_revision must be"),
    ],
    ids=["unknown", "downgrade", "empty-revision", "dash", "injection"],
)
def test_the_format_refuses_an_option_it_cannot_honour(
    extra: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(MigrationFormatOptionError, match=re.escape(message)):
        alembic_format([table_schema("broker")], FormatOptions(extra=extra))


def test_the_revision_starts_with_its_comments() -> None:
    schemas = [table_schema("broker")]
    plain = alembic_format(schemas, FormatOptions())

    source = alembic_format(
        schemas,
        FormatOptions(),
        comments=["plan for database app (search_path {public})", 'a """ name'],
    )

    # Comment lines, so a name read from a server is never code, and the
    # docstring stays the module's first statement.
    assert source == (
        '# plan for database app (search_path {public})\n# a """ name\n' + plain
    )
    assert ast.get_docstring(ast.parse(source)) == ast.get_docstring(ast.parse(plain))
    revision = revision_namespace(source)["revision"]
    assert revision == revision_namespace(plain)["revision"]


@pytest.mark.parametrize(
    "comment",
    ["a\nimport os", "a\rb", "a\0b"],
    ids=["line-feed", "carriage-return", "nul"],
)
def test_the_revision_refuses_a_comment_a_line_break_would_end(comment: str) -> None:
    with pytest.raises(ValueError, match="line break or a NUL"):
        alembic_format([table_schema("broker")], FormatOptions(), comments=[comment])


@pytest.mark.parametrize(
    "comment",
    [
        pytest.param("db coding:utf-7", id="colon"),
        pytest.param("-*- coding: latin-1 -*-", id="emacs"),
        pytest.param("vim: set fileencoding=utf-8 :", id="vim"),
        pytest.param("CODING=ascii", id="upper-case"),
    ],
)
def test_the_revision_refuses_a_comment_that_declares_an_encoding(
    comment: str,
) -> None:
    # On one of a file's first two lines, Python reads it as the encoding of
    # the whole file (PEP 263): utf-7 would turn "+AAo-" into a line break.
    with pytest.raises(ValueError, match="encoding declaration"):
        alembic_format([table_schema("broker")], FormatOptions(), comments=[comment])


@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
@pytest.mark.parametrize("downgrade", ["none", "drop"])
def test_the_generated_revision_runs_with_the_alembic_command(
    sqlalchemy_driver: str,
    downgrade: str,
    alembic_project: Path,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    url = sqlalchemy_url(dsn, sqlalchemy_driver).render_as_string(hide_password=False)
    (alembic_project / "migrations" / "versions" / "taskiq_tables.py").write_text(
        alembic_format(
            default_schemas(),
            FormatOptions(extra={"revision": "taskiq_tables", "downgrade": downgrade}),
        ),
        encoding="utf-8",
    )

    for command in (["upgrade", "head"], ["downgrade", "base"], ["upgrade", "head"]):
        result = alembic(alembic_project, url, *command)
        assert result.returncode == 0, result.stderr
        if command[0] == "downgrade":
            after_downgrade = tables_in(dsn)

    # The second upgrade ran the statements again, on the tables or without.
    assert tables_in(dsn) == {*DEFAULT_TABLES, "alembic_version"}
    assert after_downgrade == (
        {"alembic_version"}
        if downgrade == "drop"
        else {*DEFAULT_TABLES, "alembic_version"}
    )


def test_the_generated_revision_prints_its_sql_offline(
    alembic_project: Path,
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    (alembic_project / "migrations" / "versions" / "taskiq_tables.py").write_text(
        alembic_format(
            default_schemas(), FormatOptions(extra={"revision": "taskiq_tables"})
        ),
        encoding="utf-8",
    )

    result = alembic(
        alembic_project,
        sqlalchemy_url(dsn).render_as_string(hide_password=False),
        "upgrade",
        "head",
        "--sql",
    )

    assert result.returncode == 0, result.stderr
    for statement in migration_statements(default_schemas()):
        assert f"{statement};\n" in result.stdout
    assert tables_in(dsn) == set()
    asyncio.run(run_script(dsn, driver, result.stdout))
    assert tables_in(dsn) == {*DEFAULT_TABLES, "alembic_version"}


def test_generate_writes_the_revision_through_the_cli() -> None:
    expected = alembic_format(
        [table_schema("broker", table_name="jobs")],
        FormatOptions(extra={"revision": "taskiq_jobs", "down_revision": "app_0042"}),
    )

    result = taskiq(
        "postgresql",
        "generate",
        "--component",
        "broker",
        "--table-name",
        "jobs",
        "--format",
        "alembic",
        "--option",
        "revision=taskiq_jobs",
        "--option",
        "down_revision=app_0042",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


@pytest.mark.parametrize(
    ("option", "message"),
    [
        ("author=me", "unknown option: author"),
        ("downgrade=truncate", "downgrade must be 'none' or 'drop'"),
    ],
    ids=["unknown", "downgrade"],
)
def test_generate_exits_with_2_on_an_option_the_format_refuses(
    option: str,
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = PostgresqlCMD().exec(
        [
            "generate",
            "--component",
            "broker",
            "--format",
            "alembic",
            "--option",
            option,
        ],
    )

    captured = capsys.readouterr()
    assert (code, captured.out) == (2, "")
    assert message in captured.err


def plan_command(dsn: str, *options: str) -> list[str]:
    """``taskiq postgresql plan`` of the default broker table, as a revision."""
    return [
        "plan",
        "--component",
        "broker",
        "--dsn",
        dsn,
        "--format",
        "alembic",
        *(text for option in options for text in ("--option", option)),
    ]


@pytest.mark.parametrize(
    ("option", "message"),
    [
        ("author=me", "unknown option: author"),
        ("downgrade=truncate", "downgrade must be 'none' or 'drop'"),
    ],
    ids=["unknown", "downgrade"],
)
def test_plan_exits_with_2_on_an_option_the_format_refuses_with_nothing_pending(
    option: str,
    message: str,
    new_database: Callable[[], str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    dsn = new_database()
    asyncio.run(run_script(dsn, "asyncpg", render_sql([table_schema("broker")])))

    code = PostgresqlCMD().exec(plan_command(dsn, option))

    # A database with nothing to plan renders nothing, which must not let the
    # option through: it is checked before the catalog is read.
    captured = capsys.readouterr()
    assert (code, captured.out) == (2, "")
    assert message in captured.err


def test_plan_drops_only_a_table_its_revision_creates(
    new_database: Callable[[], str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    dsn = new_database()
    command = plan_command(dsn, "downgrade=drop")
    table_0_4 = BROKER_TABLE_0_4_SQL.format(table="taskiq_messages")

    # Every step pending: the revision creates the table, so it may drop it.
    creating = PostgresqlCMD().exec(command)
    created = capsys.readouterr()
    asyncio.run(run_script(dsn, "asyncpg", table_0_4))
    completing = PostgresqlCMD().exec(command)
    completed = capsys.readouterr()

    assert creating == 0, created.err
    assert "DROP TABLE IF EXISTS taskiq_messages" in created.out
    # Only step 2 pending: the downgrade of a revision that adds two columns
    # would drop the table 0.4.x created, and every message in it.
    assert (completing, completed.out) == (2, "")
    assert "downgrade=drop" in completed.err
    assert "taskiq_messages" in completed.err


def test_plan_knows_the_alembic_format_under_another_name(
    new_database: Callable[[], str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Another package can register this format under a name of its own: the
    # format, not the name asked for, gets the header and the drop guard.
    register(
        monkeypatch,
        EntryPoint(
            name="revision",
            value="taskiq_postgresql.contrib.alembic:alembic_format",
            group=MIGRATION_FORMATS_GROUP,
        ),
    )
    dsn = new_database()
    command = [
        "plan",
        "--component",
        "broker",
        "--dsn",
        dsn,
        "--format",
        "revision",
        "--option",
        "downgrade=drop",
    ]

    creating = PostgresqlCMD().exec(command)
    created = capsys.readouterr()
    table_0_4 = BROKER_TABLE_0_4_SQL.format(table="taskiq_messages")
    asyncio.run(run_script(dsn, "asyncpg", table_0_4))
    completing = PostgresqlCMD().exec(command)
    completed = capsys.readouterr()

    assert creating == 0, created.err
    assert created.out.startswith("# plan for database ")
    assert "DROP TABLE IF EXISTS taskiq_messages" in created.out
    assert (completing, completed.out) == (2, "")
    assert "downgrade=drop" in completed.err
    assert "taskiq_messages" in completed.err


def test_the_format_without_alembic_says_what_to_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block(monkeypatch, "alembic")
    monkeypatch.delitem(sys.modules, "taskiq_postgresql.contrib.alembic")

    with pytest.raises(ImportError) as missing:
        migrations.load_format("alembic")

    assert "pip install 'taskiq-postgresql[alembic]'" in str(missing.value)
