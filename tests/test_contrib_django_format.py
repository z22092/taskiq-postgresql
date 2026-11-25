"""
The ``django`` migration format: ``taskiq postgresql generate --format django``.

It writes one Django migration that holds every step of the tables, its SQL
written out, for a project that keeps the tables in a migration of its own
instead of installing the app. ``plan --format django`` writes only the steps a
database still lacks. The format imports no Django, so it works without the
``django`` extra; running the migrations it writes needs Django.

The format is imported when a test calls ``render``, never when this module is
collected, so that without it each test fails on its own.
"""

from __future__ import annotations

import ast
import asyncio
import random
import re
import string
import subprocess
import sys
from dataclasses import replace
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

import pytest
from django.db import migrations as django_migrations
from django.test.utils import override_settings

from taskiq_postgresql.cli import PostgresqlCMD
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
from tests.django_support import APP, migrate
from tests.helpers import (
    BROKER_TABLE_0_4_SQL,
    block,
    bounded,
    fetch_on_server,
    in_database,
    lock_of,
    new_database,
    plan_of,
    run_script,
    tables_in,
)

FORMAT_MODULE = "taskiq_postgresql.contrib.django_format"
IMPORTS = ["from django.db import migrations"]
TIMEOUT = 60
# What Python reads as an encoding declaration on the first two lines of a
# file (PEP 263), matched without regard to case.
ENCODING_DECLARATION = re.compile(r"coding[:=]\s*[-\w.]+", re.IGNORECASE)
# Runs taskiq's own entry point in an interpreter where Django cannot be
# imported, as on an install without the django extra.
WITHOUT_DJANGO = (
    "import runpy, sys\n"
    "sys.modules['django'] = None\n"
    "sys.argv = ['taskiq', *sys.argv[1:]]\n"
    "runpy.run_module('taskiq', run_name='__main__', alter_sys=True)\n"
)


@pytest.fixture
def scratch_dsn(postgresql_dsn: str) -> Iterator[str]:
    """DSN of a database created empty for one test and dropped after it."""
    yield from new_database(postgresql_dsn)


def render(*args: Any, **kwargs: Any) -> str:
    """Render the django format, imported on call."""
    from taskiq_postgresql.contrib.django_format import (  # noqa: PLC0415
        django_format,
    )

    return django_format(*args, **kwargs)


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


def expected_steps(
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


def run_python(*args: str) -> subprocess.CompletedProcess[str]:
    """Run ``python -c`` with ``args``, this interpreter, as a user would."""
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", *args],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=False,
    )


def test_the_django_format_is_registered_by_the_package() -> None:
    (entry_point,) = entry_points(group=MIGRATION_FORMATS_GROUP, name="django")

    assert entry_point.value == f"{FORMAT_MODULE}:django_format"
    assert entry_point.dist is not None
    assert entry_point.dist.name == "taskiq-postgresql"
    assert "django" in available_formats()
    assert load_format("django").__module__ == FORMAT_MODULE


def test_the_migration_runs_each_step_and_imports_only_django() -> None:
    schemas = default_schemas()

    source = render(schemas, FormatOptions())

    assert run_sql_of(source) == expected_steps(schemas)
    assert migration_of(source).dependencies == []
    # The SQL is written out: the file never imports this package.
    assert imports_of(source) == IMPORTS
    # The same tables give the same file; Django runs each migration in a
    # transaction of its own, so BEGIN and COMMIT never appear.
    assert render(schemas, FormatOptions()) == source
    assert render(schemas, FormatOptions(transaction=False)) == source
    assert "BEGIN" not in source
    ast.parse(source)


def test_the_migration_takes_the_options() -> None:
    schemas = [table_schema("broker"), table_schema("result_backend")]

    source = render(
        schemas,
        FormatOptions(
            lock=False,
            lock_timeout=3,
            extra={"dependencies": "shop.0004_orders,billing.0001_initial"},
        ),
    )
    unbounded = render(schemas, FormatOptions(lock_timeout=0))

    assert migration_of(source).dependencies == [
        ("shop", "0004_orders"),
        ("billing", "0001_initial"),
    ]
    assert run_sql_of(source) == expected_steps(schemas, lock=False, lock_timeout=3)
    assert run_sql_of(unbounded) == expected_steps(schemas, lock_timeout=0)
    assert "lock_timeout" not in unbounded


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        pytest.param({"author": "me"}, "unknown option: author", id="unknown"),
        pytest.param({"downgrade": "drop"}, "unknown option: downgrade", id="no-drop"),
        pytest.param({"dependencies": ""}, "dependencies must be", id="empty"),
        pytest.param(
            {"dependencies": "shop"}, "dependencies must be", id="no-migration"
        ),
        pytest.param(
            {"dependencies": "shop.0004_orders,"},
            "dependencies must be",
            id="trailing-comma",
        ),
        pytest.param(
            {"dependencies": "1shop.0004_orders"},
            "dependencies must be",
            id="label-not-an-identifier",
        ),
        pytest.param(
            {"dependencies": "shop.0004-orders"},
            "dependencies must be",
            id="dash",
        ),
        pytest.param(
            {"dependencies": 'shop.0004")\nimport os'},
            "dependencies must be",
            id="injection",
        ),
    ],
)
def test_the_format_refuses_an_option_it_cannot_honour(
    extra: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(MigrationFormatOptionError, match=re.escape(message)):
        render([table_schema("broker")], FormatOptions(extra=extra))
    # Checked with no table too: plan checks the options with none.
    with pytest.raises(MigrationFormatOptionError, match=re.escape(message)):
        render([], FormatOptions(extra=extra))


@pytest.mark.parametrize(
    "table_name",
    [
        pytest.param("jobs, users", id="comma"),
        pytest.param('jobs"', id="quote"),
        pytest.param("1jobs", id="leading-digit"),
        pytest.param("jobs\u2028x", id="unicode-line-separator"),
    ],
)
def test_the_format_refuses_a_table_name_that_is_not_an_identifier(
    table_name: str,
) -> None:
    with pytest.raises(ValueError, match="plain SQL identifier"):
        render([table_schema("broker", table_name=table_name)], FormatOptions())


def broker_with_step_number(number: object) -> TableSchema:
    broker = table_schema("broker")
    return replace(broker, steps=(replace(broker.steps[0], number=number),))


@pytest.mark.parametrize(
    ("schema", "error", "message"),
    [
        pytest.param(
            replace(table_schema("broker"), kind="queue"),
            ValueError,
            "kind 'queue'",
            id="unknown-kind",
        ),
        pytest.param(
            broker_with_step_number("1\nimport os"),
            TypeError,
            "must be an int",
            id="text-step",
        ),
        pytest.param(
            broker_with_step_number(True),
            TypeError,
            "must be an int",
            id="bool-step",
        ),
        pytest.param(
            broker_with_step_number(0),
            ValueError,
            "must be positive",
            id="step-zero",
        ),
    ],
)
def test_the_format_refuses_a_kind_or_step_a_comment_cannot_hold(
    schema: TableSchema,
    error: type[Exception],
    message: str,
) -> None:
    # Both are written into the file's comments.
    with pytest.raises(error, match=re.escape(message)):
        render([schema], FormatOptions())


def test_the_migration_starts_with_its_comments() -> None:
    schemas = [table_schema("broker")]
    plain = render(schemas, FormatOptions())

    source = render(
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
    ("comment", "message"),
    [
        pytest.param("a\nimport os", "line break or a NUL", id="line-feed"),
        pytest.param("a\rb", "line break or a NUL", id="carriage-return"),
        pytest.param("a\0b", "line break or a NUL", id="nul"),
        pytest.param("db coding:utf-7", "encoding declaration", id="pep-263-colon"),
        pytest.param(
            "-*- coding: latin-1 -*-",
            "encoding declaration",
            id="pep-263-emacs",
        ),
        pytest.param(
            "vim: set fileencoding=utf-8 :",
            "encoding declaration",
            id="pep-263-vim",
        ),
        pytest.param("CODING=ascii", "encoding declaration", id="pep-263-upper"),
    ],
)
def test_the_migration_refuses_a_comment_that_would_change_the_file(
    comment: str,
    message: str,
) -> None:
    # A line break would end the comment and run the rest; an encoding
    # declaration on the first two lines changes how Python reads the file.
    with pytest.raises(ValueError, match=message):
        render([table_schema("broker")], FormatOptions(), comments=[comment])


def test_the_generated_migration_runs_with_migrate_without_this_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    django_db: str,
) -> None:
    app = "tkq_django_" + "".join(random.choices(string.ascii_lowercase, k=10))
    (tmp_path / app / "migrations").mkdir(parents=True)
    (tmp_path / app / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / app / "migrations" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / app / "migrations" / "0001_taskiq_tables.py").write_text(
        render(default_schemas(), FormatOptions()),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        with override_settings(INSTALLED_APPS=[app]):
            with monkeypatch.context() as blocked:
                # As on a host where taskiq-postgresql is not installed.
                block(blocked, "taskiq_postgresql")
                migrate(app)
                migrate(app, "zero")
            after_zero = tables_in(django_db)
            migrate(app)
    finally:
        for name in [name for name in sys.modules if name.split(".")[0] == app]:
            del sys.modules[name]

    plan = plan_of(django_db, default_schemas())
    assert plan.is_current, plan.pending
    assert plan.problems == ()
    # Reversed, the migration leaves the tables in place.
    assert after_zero == {
        *(schema.name for schema in default_schemas()),
        "django_migrations",
    }


def test_generate_writes_the_migration(capsys: pytest.CaptureFixture[str]) -> None:
    expected = render(
        [table_schema("broker", table_name="jobs")],
        FormatOptions(extra={"dependencies": "shop.0004_orders"}),
    )

    code = PostgresqlCMD().exec(
        [
            "generate",
            "--component",
            "broker",
            "--table-name",
            "jobs",
            "--format",
            "django",
            "--option",
            "dependencies=shop.0004_orders",
        ],
    )

    captured = capsys.readouterr()
    assert code == 0, captured.err
    assert captured.out == expected


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
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    broker = table_schema("broker")
    command = ["plan", "--component", "broker", "--dsn", scratch_dsn]

    creating = PostgresqlCMD().exec([*command, "--format", "django"])
    created = capsys.readouterr()
    asyncio.run(
        run_script(
            scratch_dsn,
            "asyncpg",
            BROKER_TABLE_0_4_SQL.format(table=broker.name),
        ),
    )
    completing = PostgresqlCMD().exec([*command, "--format", "django"])
    completed = capsys.readouterr()

    assert (creating, completing) == (0, 0), created.err + completed.err
    header = created.out.splitlines()[0]
    assert re.fullmatch(
        r"# plan for database \S+ \(search_path .+\): valid only for this database",
        header,
    )
    assert created.out == render(
        [broker],
        FormatOptions(),
        comments=[header.removeprefix("# ")],
    )
    # On a broker table 0.4.x created, only step 2 is pending.
    assert completed.out.splitlines()[0] == header
    assert run_sql_of(completed.out) == expected_steps(
        [replace(broker, steps=broker.steps[1:])],
    )


def test_plan_writes_a_database_name_that_declares_an_encoding_escaped(
    postgresql_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The name comes from the server, and the header is the file's first line:
    # Python would read it as the file's encoding.
    name = "tkq_" + "".join(random.choices(string.ascii_lowercase, k=8))
    name += "_coding:utf-7"
    asyncio.run(fetch_on_server(postgresql_dsn, f'CREATE DATABASE "{name}"'))
    try:
        code = PostgresqlCMD().exec(
            [
                "plan",
                "--component",
                "broker",
                "--dsn",
                in_database(postgresql_dsn, quote(name, safe="")),
                "--format",
                "django",
            ],
        )
        captured = capsys.readouterr()
    finally:
        asyncio.run(
            fetch_on_server(
                postgresql_dsn,
                f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)',
            ),
        )

    assert code == 0, captured.err
    first, second = captured.out.splitlines()[:2]
    assert ENCODING_DECLARATION.search(first) is None
    assert ENCODING_DECLARATION.search(second) is None
    # Still named, with the ":" escaped as repr() would escape a control
    # character.
    escaped = repr(name).replace(":", "\\x3a")
    assert first.startswith(f"# plan for database {escaped} (search_path ")
    # Read as Python reads a file: from bytes, honouring a declaration.
    compile(captured.out.encode(), "0001_taskiq_tables.py", "exec")


def test_the_format_works_without_django_and_the_app_says_what_to_install() -> None:
    generated = run_python(
        WITHOUT_DJANGO,
        "postgresql",
        "generate",
        "--component",
        "broker",
        "--format",
        "django",
    )
    app = run_python(
        f"import sys\nsys.modules['django'] = None\nimport {APP}\n",
    )

    # Writing a migration file needs no Django; running one does.
    assert generated.returncode == 0, generated.stderr
    assert generated.stdout == render([table_schema("broker")], FormatOptions())
    assert app.returncode == 1
    assert "pip install taskiq-postgresql[django]" in app.stderr
