"""
The ``django`` migration format: ``taskiq postgresql generate --format django``.

It writes one Django migration that holds every step of the tables, its SQL
written out, for a project that keeps the tables in a migration of its own
instead of installing the app. ``plan --format django`` writes only the steps a
database still lacks.
"""

from __future__ import annotations

import ast
import asyncio
import random
import re
import string
import sys
from dataclasses import replace
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Callable, Iterator

import pytest
from django.db import migrations as django_migrations
from django.test.utils import override_settings

from taskiq_postgresql import migrations
from taskiq_postgresql.cli import PostgresqlCMD
from taskiq_postgresql.contrib.django.formats import django_format
from taskiq_postgresql.migrations import (
    COMPONENT_KINDS,
    DEFAULT_LOCK_TIMEOUT,
    MIGRATION_FORMATS_GROUP,
    FormatOptions,
    MigrationFormatOptionError,
    TableSchema,
    available_formats,
    load_format,
    table_schema,
)
from tests.helpers import run_script
from tests.test_cli import taskiq
from tests.test_contrib_alembic import bounded
from tests.test_contrib_django import (
    APP,
    configure_django,
    django_database,
    lock_of,
    migrate,
    plan_of,
)
from tests.test_contrib_sqlalchemy import block, databases, tables_in
from tests.test_migrations import BROKER_TABLE_0_4_SQL

pytestmark = pytest.mark.usefixtures("django_configured")

IMPORTS = ["from django.db import migrations"]


@pytest.fixture(scope="session")
def django_configured() -> None:
    configure_django()


@pytest.fixture
def new_database(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Create empty databases for one test, and drop them after it."""
    yield from databases(postgresql_dsn)


def default_schemas() -> list[TableSchema]:
    return [table_schema(kind) for kind in COMPONENT_KINDS]


def migration_of(source: str) -> Any:
    """Execute a migration file as Django loads it, and return its class."""
    namespace: dict[str, Any] = {"__name__": "generated_migration"}
    exec(compile(source, "0001_taskiq_tables.py", "exec"), namespace)  # noqa: S102
    return namespace["Migration"]


def run_sql_of(source: str) -> list[list[str]]:
    """The statements of each ``RunSQL`` of a migration file, in order."""
    operations = migration_of(source).operations
    assert all(
        isinstance(operation, django_migrations.RunSQL)
        and operation.reverse_sql == django_migrations.RunSQL.noop
        for operation in operations
    )
    return [list(operation.sql) for operation in operations]


def step_statements(
    schemas: list[TableSchema],
    *,
    lock: bool = True,
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
) -> list[list[str]]:
    """Each step of each table, by table name, with its own lock and bound."""
    return [
        bounded([*([lock_of(schema)] if lock else []), *step.statements], lock_timeout)
        for schema in sorted(schemas, key=lambda table: table.name)
        for step in schema.steps
    ]


def imports_of(source: str) -> list[str]:
    return [
        line for line in source.splitlines() if line.startswith(("import ", "from "))
    ]


def test_the_django_format_is_registered_by_the_package() -> None:
    (entry_point,) = entry_points(group=MIGRATION_FORMATS_GROUP, name="django")

    assert entry_point.value == "taskiq_postgresql.contrib.django.formats:django_format"
    assert entry_point.dist is not None
    assert entry_point.dist.name == "taskiq-postgresql"
    assert "django" in available_formats()
    assert load_format("django") is django_format


def test_the_migration_runs_each_step_and_imports_only_django() -> None:
    schemas = default_schemas()

    source = django_format(schemas, FormatOptions())

    assert run_sql_of(source) == step_statements(schemas)
    assert migration_of(source).dependencies == []
    # The SQL is written out: the file never imports this package.
    assert imports_of(source) == IMPORTS
    # The same tables give the same file; Django runs each migration in a
    # transaction of its own, so BEGIN and COMMIT never appear.
    assert django_format(schemas, FormatOptions()) == source
    assert django_format(schemas, FormatOptions(transaction=False)) == source
    assert "BEGIN" not in source
    ast.parse(source)


def test_the_migration_takes_the_options() -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    source = django_format(
        schemas,
        FormatOptions(
            lock=False,
            lock_timeout=3,
            extra={"dependencies": "shop.0004_orders,billing.0001_initial"},
        ),
    )
    unbounded = django_format(schemas, FormatOptions(lock_timeout=0))

    assert migration_of(source).dependencies == [
        ("shop", "0004_orders"),
        ("billing", "0001_initial"),
    ]
    assert run_sql_of(source) == step_statements(schemas, lock=False, lock_timeout=3)
    assert run_sql_of(unbounded) == step_statements(schemas, lock_timeout=0)
    assert "lock_timeout" not in unbounded


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"author": "me"}, "unknown option: author"),
        ({"downgrade": "drop"}, "unknown option: downgrade"),
        ({"dependencies": ""}, "dependencies must be"),
        ({"dependencies": "shop"}, "dependencies must be"),
        ({"dependencies": "shop.0004_orders,"}, "dependencies must be"),
        ({"dependencies": "1shop.0004_orders"}, "dependencies must be"),
        ({"dependencies": "shop.0004-orders"}, "dependencies must be"),
        ({"dependencies": 'shop.0004")\nimport os'}, "dependencies must be"),
    ],
    ids=[
        "unknown",
        "no-drop",
        "empty",
        "no-migration",
        "trailing-comma",
        "label-not-an-identifier",
        "dash",
        "injection",
    ],
)
def test_the_format_refuses_an_option_it_cannot_honour(
    extra: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(MigrationFormatOptionError, match=re.escape(message)):
        django_format([table_schema("broker")], FormatOptions(extra=extra))
    # Checked before any table: plan checks the options with none.
    with pytest.raises(MigrationFormatOptionError, match=re.escape(message)):
        django_format([], FormatOptions(extra=extra))


@pytest.mark.parametrize(
    "table_name",
    ["jobs, users", 'jobs"', "1jobs", "jobs\u2028x"],
    ids=["comma", "quote", "leading-digit", "unicode-line-separator"],
)
def test_the_format_refuses_a_table_name_that_is_not_an_identifier(
    table_name: str,
) -> None:
    with pytest.raises(ValueError, match="plain SQL identifier"):
        django_format(
            [table_schema("broker", table_name=table_name)],
            FormatOptions(),
        )


def broker_with_step_number(number: object) -> TableSchema:
    broker = table_schema("broker")
    return replace(broker, steps=(replace(broker.steps[0], number=number),))


@pytest.mark.parametrize(
    ("schema", "error", "message"),
    [
        (replace(table_schema("broker"), kind="queue"), ValueError, "kind 'queue'"),
        (broker_with_step_number("1\nimport os"), TypeError, "must be an int"),
        (broker_with_step_number(True), TypeError, "must be an int"),
        (broker_with_step_number(0), ValueError, "must be positive"),
    ],
    ids=["unknown-kind", "text-step", "bool-step", "step-zero"],
)
def test_the_format_refuses_a_kind_or_step_a_comment_cannot_hold(
    schema: TableSchema,
    error: type[Exception],
    message: str,
) -> None:
    # Both are written into the file's comments.
    with pytest.raises(error, match=re.escape(message)):
        django_format([schema], FormatOptions())


def test_the_migration_starts_with_its_comments() -> None:
    schemas = [table_schema("broker")]
    plain = django_format(schemas, FormatOptions())

    source = django_format(
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


@pytest.mark.parametrize(
    "comment",
    ["a\nimport os", "a\rb", "a\0b"],
    ids=["line-feed", "carriage-return", "nul"],
)
def test_the_migration_refuses_a_comment_a_line_break_would_end(comment: str) -> None:
    with pytest.raises(ValueError, match="line break or a NUL"):
        django_format([table_schema("broker")], FormatOptions(), comments=[comment])


def test_the_generated_migration_runs_with_migrate_without_this_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    app = "tkq_django_" + "".join(random.choices(string.ascii_lowercase, k=10))
    (tmp_path / app / "migrations").mkdir(parents=True)
    (tmp_path / app / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / app / "migrations" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / app / "migrations" / "0001_taskiq_tables.py").write_text(
        django_format(default_schemas(), FormatOptions()),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        with django_database(dsn), override_settings(INSTALLED_APPS=[app]):
            with monkeypatch.context() as blocked:
                # As on a host where taskiq-postgresql is not installed.
                block(blocked, "taskiq_postgresql")
                migrate(app)
                migrate(app, "zero")
            after_zero = tables_in(dsn)
            migrate(app)
    finally:
        for name in [name for name in sys.modules if name.split(".")[0] == app]:
            del sys.modules[name]

    plan = plan_of(dsn, default_schemas())
    assert plan.is_current, plan.pending
    assert plan.problems == ()
    # Reversed, the migration leaves the tables in place.
    assert after_zero == {
        *(schema.name for schema in default_schemas()),
        "django_migrations",
    }


def test_generate_writes_the_migration_through_the_cli() -> None:
    expected = django_format(
        [table_schema("broker", table_name="jobs")],
        FormatOptions(extra={"dependencies": "shop.0004_orders"}),
    )

    result = taskiq(
        "postgresql",
        "generate",
        "--component",
        "broker",
        "--table-name",
        "jobs",
        "--format",
        "django",
        "--option",
        "dependencies=shop.0004_orders",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


def test_generate_exits_with_2_on_an_option_the_format_refuses(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = PostgresqlCMD().exec(
        ["generate", "--component", "broker", "--format", "django", "--option", "a=b"],
    )

    captured = capsys.readouterr()
    assert (code, captured.out) == (2, "")
    assert "unknown option: a" in captured.err


def test_plan_writes_only_the_pending_steps_under_the_plan_header(
    new_database: Callable[[], str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    dsn = new_database()
    broker = table_schema("broker")
    command = ["plan", "--component", "broker", "--dsn", dsn, "--format", "django"]

    creating = PostgresqlCMD().exec(command)
    created = capsys.readouterr()
    asyncio.run(
        run_script(dsn, "asyncpg", BROKER_TABLE_0_4_SQL.format(table=broker.name)),
    )
    completing = PostgresqlCMD().exec(command)
    completed = capsys.readouterr()

    assert (creating, completing) == (0, 0), created.err + completed.err
    header = created.out.splitlines()[0]
    assert re.fullmatch(
        r"# plan for database \S+ \(search_path .+\): valid only for this database",
        header,
    )
    assert created.out == django_format(
        [broker],
        FormatOptions(),
        comments=[header.removeprefix("# ")],
    )
    # On a broker table 0.4.x created, only step 2 is pending.
    assert completed.out.splitlines()[0] == header
    assert run_sql_of(completed.out) == step_statements(
        [replace(broker, steps=broker.steps[1:])],
    )


def test_the_format_without_django_says_what_to_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block(monkeypatch, "django")
    for name in [name for name in sys.modules if name.startswith(APP)]:
        monkeypatch.delitem(sys.modules, name)

    with pytest.raises(ImportError) as missing:
        migrations.load_format("django")

    assert "pip install 'taskiq-postgresql[django]'" in str(missing.value)
