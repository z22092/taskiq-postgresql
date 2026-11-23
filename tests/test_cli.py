"""
``taskiq postgresql``, the command this package adds to the ``taskiq`` CLI.

The subprocess tests run it the way a user does, through ``python -m taskiq``
and the installed entry point. The in-process tests call ``PostgresqlCMD``
directly for the usage and error paths.
"""

from __future__ import annotations

import asyncio
import os
import random
import string
import subprocess
import sys
from importlib.metadata import EntryPoint, entry_points, version
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Optional
from urllib.parse import urlsplit

import asyncpg
import pytest

from taskiq_postgresql import migrations
from taskiq_postgresql.cli import PostgresqlCMD
from taskiq_postgresql.migrations import (
    FormatOptions,
    TableSchema,
    render_sql,
    table_schema,
)
from tests.helpers import (
    closed_local_port,
    create_database,
    drop_database,
    in_database,
    with_netloc,
)

UNUSED_DSN = "postgresql://localhost/unused"
TIMEOUT = 60
DRIVER_MODULES = ("asyncpg", "psycopg", "psycopg_pool", "psqlpy")
TABLES = ("cli_messages", "cli_results", "cli_schedules")

# Runs taskiq's own entry point in an interpreter where no driver package can
# be imported, like an install without a driver extra.
WITHOUT_DRIVERS = (
    "import runpy, sys\n"
    f"for name in {DRIVER_MODULES!r}:\n"
    "    sys.modules[name] = None\n"
    "sys.argv = ['taskiq', *sys.argv[1:]]\n"
    "runpy.run_module('taskiq', run_name='__main__', alter_sys=True)\n"
)

# Runs taskiq's own entry point with the warnings raised outside this package
# ignored, for the tests that check stderr is empty: on the lowest dependency
# versions, pydantic 2.5 warns about typing_extensions on import, which says
# nothing about this package. A warning this package raises still shows.
WITHOUT_DEPENDENCY_WARNINGS = (
    "import runpy, sys, warnings\n"
    "warnings.filterwarnings('ignore', module=r'(?!taskiq_postgresql(\\.|$))')\n"
    "sys.argv = ['taskiq', *sys.argv[1:]]\n"
    "runpy.run_module('taskiq', run_name='__main__', alter_sys=True)\n"
)

APP = """\
import os

from taskiq import InMemoryBroker, TaskiqScheduler

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

dsn = os.environ["POSTGRESQL_URL"]
driver = os.environ.get("APP_DRIVER", "asyncpg")

result_backend = PostgresqlResultBackend(
    dsn=dsn,
    driver=driver,
    table_name="cli_results",
    field_for_task_id="Text",
)
broker = PostgresqlBroker(
    dsn=dsn,
    driver=driver,
    table_name="cli_messages",
).with_result_backend(result_backend)
source = PostgresqlSchedulerSource(dsn=dsn, driver=driver, table_name="cli_schedules")
scheduler = TaskiqScheduler(broker=broker, sources=[source])


def make_broker():
    return broker


foreign_broker = InMemoryBroker()
"""

GOLDEN_BROKER = (
    "-- taskiq-postgresql {version}\n"
    "-- tables: taskiq_messages\n"
    "BEGIN;\n"
    "SET LOCAL lock_timeout = '10s';\n"
    "SELECT pg_advisory_xact_lock(hashtext('taskiq_postgresql'), "
    "hashtext('taskiq_messages'));\n"
    "CREATE TABLE IF NOT EXISTS taskiq_messages (id SERIAL NOT NULL PRIMARY KEY, "
    "task_id Uuid NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, "
    "labels JSONB NOT NULL, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());\n"
    "CREATE INDEX IF NOT EXISTS taskiq_messages_id_idx ON taskiq_messages "
    "USING HASH (id);\n"
    "ALTER TABLE taskiq_messages\n"
    "  ADD COLUMN IF NOT EXISTS channel VARCHAR,\n"
    "  ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL "
    "DEFAULT NOW();\n"
    "CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx\n"
    "  ON taskiq_messages (channel, not_before, id);\n"
    "COMMIT;\n"
)

GOLDEN_RESULTS = (
    "-- taskiq-postgresql {version}\n"
    "-- tables: jobs_results\n"
    "SET LOCAL lock_timeout = '10s';\n"
    "CREATE TABLE IF NOT EXISTS jobs_results (task_id Text NOT NULL PRIMARY KEY, "
    "result BYTEA NOT NULL, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());\n"
    "CREATE INDEX IF NOT EXISTS jobs_results_task_id_idx ON jobs_results "
    "USING HASH (task_id);\n"
)


def golden(template: str) -> str:
    return template.format(version=version("taskiq-postgresql"))


def expected_app_schemas() -> list[TableSchema]:
    """The tables of ``APP``, described without importing it."""
    return [
        table_schema("broker", table_name="cli_messages"),
        table_schema(
            "result_backend", table_name="cli_results", field_for_task_id="Text"
        ),
        table_schema("scheduler_source", table_name="cli_schedules"),
    ]


def taskiq(
    *args: str,
    cwd: Optional[Path] = None,
    dsn: str = UNUSED_DSN,
    without_drivers: bool = False,
    without_dependency_warnings: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run ``taskiq *args`` with this interpreter, as a user would."""
    if without_drivers:
        command = [sys.executable, "-c", WITHOUT_DRIVERS, *args]
    elif without_dependency_warnings:
        command = [sys.executable, "-c", WITHOUT_DEPENDENCY_WARNINGS, *args]
    else:
        command = [sys.executable, "-m", "taskiq", *args]
    return subprocess.run(  # noqa: S603
        command,
        cwd=cwd,
        env={**os.environ, "POSTGRESQL_URL": dsn},
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=False,
    )


@pytest.fixture
def app_dir(tmp_path: Path) -> Path:
    """A directory holding ``app.py``, apart from the working directory."""
    directory = tmp_path / "project"
    directory.mkdir()
    (directory / "app.py").write_text(APP, encoding="utf-8")
    return directory


@pytest.fixture
def scratch_dsn(postgresql_dsn: str) -> Iterator[str]:
    """DSN of a database created empty for one test and dropped after it."""
    name = "cli_" + "".join(random.choices(string.ascii_lowercase, k=10))
    asyncio.run(create_database(postgresql_dsn, name))
    yield in_database(postgresql_dsn, name)
    asyncio.run(drop_database(postgresql_dsn, name))


@pytest.fixture
def app_module(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    """
    Name of a module holding ``APP``, importable through ``--app-dir``.

    The name is unique, so in-process tests never reuse another test's import.
    """
    name = "tkq_app_" + "".join(random.choices(string.ascii_lowercase, k=10))
    (tmp_path / f"{name}.py").write_text(APP, encoding="utf-8")
    # The command adds --app-dir to sys.path, as taskiq worker does.
    monkeypatch.setattr(sys, "path", list(sys.path))
    yield name
    sys.modules.pop(name, None)


def existing_tables(dsn: str) -> set[str]:
    async def fetch() -> set[str]:
        connection = await asyncpg.connect(dsn)
        try:
            rows = await connection.fetch(
                "SELECT relname FROM pg_class WHERE relkind = 'r' "
                "AND relnamespace = current_schema()::regnamespace",
            )
        finally:
            await connection.close()
        return {row["relname"] for row in rows}

    return asyncio.run(fetch())


def run_in_process(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    code = PostgresqlCMD().exec(args)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# --- as a subprocess, through the taskiq entry point ---------------------------


def test_taskiq_finds_the_command() -> None:
    (entry_point,) = entry_points(group="taskiq_cli", name="postgresql")

    assert entry_point.value == "taskiq_postgresql.cli:PostgresqlCMD"
    assert entry_point.load() is PostgresqlCMD


def test_generate_prints_the_script_of_a_component_without_the_app() -> None:
    result = taskiq(
        "postgresql",
        "generate",
        "--component",
        "broker",
        without_dependency_warnings=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == golden(GOLDEN_BROKER)
    assert result.stderr == ""


def test_generate_takes_the_table_options_and_drops_the_wrapping() -> None:
    result = taskiq(
        "postgresql",
        "generate",
        "--component",
        "result_backend",
        "--table-name",
        "jobs_results",
        "--field-for-task-id",
        "Text",
        "--no-transaction",
        "--no-lock",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == golden(GOLDEN_RESULTS)


def test_generate_reads_the_tables_from_the_application(
    app_dir: Path,
    tmp_path: Path,
) -> None:
    # Run from elsewhere, so only --app-dir can make app importable.
    result = taskiq(
        "postgresql",
        "generate",
        "app:broker",
        "app:scheduler",
        "--app-dir",
        str(app_dir),
        cwd=tmp_path,
        without_dependency_warnings=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == render_sql(expected_app_schemas())
    assert result.stderr == ""


def test_generate_calls_a_factory_function(app_dir: Path) -> None:
    result = taskiq("postgresql", "generate", "app:make_broker", cwd=app_dir)

    assert result.returncode == 0, result.stderr
    assert result.stdout == render_sql(expected_app_schemas()[:2])


def test_generate_fails_on_a_target_without_a_component(app_dir: Path) -> None:
    result = taskiq("postgresql", "generate", "app:foreign_broker", cwd=app_dir)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "holds no taskiq-postgresql component" in result.stderr


def test_generate_rejects_an_unknown_format_with_the_list() -> None:
    result = taskiq(
        "postgresql",
        "generate",
        "--component",
        "broker",
        "--format",
        "nope",
    )

    assert result.returncode == 2  # noqa: PLR2004
    assert result.stdout == ""
    assert "unknown migration format 'nope'; available formats: " in result.stderr
    assert "sql" in result.stderr.split("available formats: ", 1)[1]


def test_migrate_creates_the_tables_whatever_run_migrations_says(
    app_dir: Path,
    scratch_dsn: str,
) -> None:
    # APP leaves run_migrations at its default, False.
    for _ in range(2):
        result = taskiq(
            "postgresql",
            "migrate",
            "app:scheduler",
            cwd=app_dir,
            dsn=scratch_dsn,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout == ""
    assert existing_tables(scratch_dsn) == set(TABLES)


def test_taskiq_help_works_without_any_driver() -> None:
    result = taskiq("--help", without_drivers=True)

    assert result.returncode == 0, result.stderr
    assert "postgresql" in result.stdout
    assert "worker" in result.stdout
    assert "Could not load" not in result.stdout + result.stderr


def test_taskiq_worker_still_parses_its_arguments_without_any_driver() -> None:
    result = taskiq("worker", "--help", without_drivers=True)

    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout


def test_generate_without_the_app_needs_no_driver() -> None:
    result = taskiq(
        "postgresql",
        "generate",
        "--component",
        "broker",
        without_drivers=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == golden(GOLDEN_BROKER)


def test_migrate_without_the_driver_says_what_to_install(app_dir: Path) -> None:
    # Also shows that the drivers really are unimportable in that interpreter.
    result = taskiq(
        "postgresql",
        "migrate",
        "app:broker",
        cwd=app_dir,
        without_drivers=True,
    )

    assert result.returncode == 1
    assert "asyncpg is not installed" in result.stderr
    assert "pip install taskiq-postgresql[asyncpg]" in result.stderr


def test_importing_the_command_loads_no_driver_and_no_migration_code() -> None:
    watched = (*DRIVER_MODULES, "taskiq_postgresql.migrations")
    probe = (
        "import sys\n"
        "import taskiq_postgresql.cli\n"
        f"print(sorted(name for name in {watched!r} if name in sys.modules))\n"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=True,
    )

    assert result.stdout == "[]\n"


# --- in process: usage errors exit with 2 ------------------------------------


@pytest.mark.parametrize(
    ("args", "message"),
    [
        ([], "required: COMMAND"),
        (["generate"], "give one or more TARGETs or --component"),
        (["generate", "app:broker", "--component", "broker"], "not both"),
        (["generate", "app:broker", "--table-name", "jobs"], "need --component"),
        (["generate", "app"], "'module:object'"),
        (["generate", "--component", "worker"], "invalid choice: 'worker'"),
        (
            [
                "generate",
                "--component",
                "scheduler_source",
                "--field-for-task-id",
                "Text",
            ],
            "no task_id column",
        ),
        (["generate", "--component", "broker", "--table-name", "a\nb"], "table name"),
        (["generate", "--component", "broker", "--option", "author"], "KEY=VALUE"),
        (
            ["generate", "--component", "broker", "--option", "author=me"],
            "the sql format takes no --option",
        ),
        (["generate", "--component", "broker", "--format", "nope"], "'nope'"),
        (
            ["generate", "--component", "broker", "--lock-timeout", "-1"],
            (
                "--lock-timeout: lock_timeout must be a whole number of seconds "
                "from 0 to 2147483, got -1"
            ),
        ),
        (
            ["generate", "--component", "broker", "--lock-timeout", "2147484"],
            "from 0 to 2147483, got 2147484",
        ),
        (
            ["generate", "--component", "broker", "--lock-timeout", "1.5"],
            "--lock-timeout: invalid int value: '1.5'",
        ),
        (["migrate"], "TARGET"),
    ],
    ids=[
        "no-command",
        "no-target",
        "target-and-component",
        "table-name-without-component",
        "not-an-import-path",
        "unknown-component",
        "field-on-the-schedule-table",
        "line-break-in-table-name",
        "option-without-value",
        "option-the-format-refuses",
        "unknown-format",
        "negative-lock-timeout",
        "lock-timeout-above-the-server-maximum",
        "lock-timeout-not-whole-seconds",
        "migrate-without-target",
    ],
)
def test_a_usage_error_exits_with_2(
    args: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, out, err = run_in_process(args, capsys)

    assert code == 2  # noqa: PLR2004
    assert out == ""
    assert message in err


def test_help_exits_with_0(capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _ = run_in_process(["generate", "--help"], capsys)

    assert code == 0
    assert "--no-transaction" in out
    assert "--lock-timeout SECONDS" in out
    assert "Exit status: 0" in out


@pytest.mark.parametrize(
    ("args", "lock_timeout"),
    [([], 10), (["--lock-timeout", "3"], 3), (["--lock-timeout", "0"], 0)],
    ids=["default", "three-seconds", "zero-leaves-it-out"],
)
@pytest.mark.parametrize(
    "transaction",
    [True, False],
    ids=["transaction", "no-transaction"],
)
def test_generate_sets_the_lock_timeout(
    args: list[str],
    lock_timeout: int,
    transaction: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    wrapping = [] if transaction else ["--no-transaction"]

    code, out, err = run_in_process(
        ["generate", "--component", "broker", *wrapping, *args],
        capsys,
    )

    assert (code, err) == (0, "")
    assert out == render_sql(
        [table_schema("broker")],
        transaction=transaction,
        lock_timeout=lock_timeout,
    )
    # The first statement after the header and BEGIN, before any lock.
    first_statement = out.splitlines()[3 if transaction else 2]
    if lock_timeout:
        assert first_statement == f"SET LOCAL lock_timeout = '{lock_timeout}s';"
        assert out.count("lock_timeout") == 1
    else:
        assert "lock_timeout" not in out


# --- in process: the commands ------------------------------------------------


def test_generate_in_process_writes_only_the_script(
    app_module: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", UNUSED_DSN)

    code, out, err = run_in_process(
        ["generate", f"{app_module}:scheduler", "--app-dir", str(tmp_path)],
        capsys,
    )

    assert (code, err) == (0, "")
    assert out == render_sql(expected_app_schemas())


def test_generate_imports_the_app_from_the_working_directory(
    app_module: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The `taskiq` console script does not put the working directory on
    # sys.path, so the command adds it while it imports, as taskiq worker does.
    monkeypatch.setenv("POSTGRESQL_URL", UNUSED_DSN)
    monkeypatch.chdir(tmp_path)
    for entry in ("", str(tmp_path)):
        while entry in sys.path:
            sys.path.remove(entry)

    code, out, err = run_in_process(["generate", f"{app_module}:broker"], capsys)

    assert (code, err) == (0, "")
    assert out == render_sql(expected_app_schemas()[:2])
    assert str(tmp_path) not in sys.path


def test_generate_fails_with_1_when_the_format_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failing(schemas: list[TableSchema], options: FormatOptions) -> str:
        raise RuntimeError("the format broke")

    class FailingEntryPoint:
        name = "failing"

        def load(self) -> Any:
            """Return a format that raises."""
            return failing

    monkeypatch.setattr(migrations, "_entry_points", lambda: [FailingEntryPoint()])

    code, out, err = run_in_process(
        ["generate", "--component", "broker", "--format", "failing"],
        capsys,
    )

    assert (code, out) == (1, "")
    assert err == "taskiq postgresql: error: RuntimeError: the format broke\n"


def test_generate_fails_with_1_when_the_app_cannot_be_imported(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, out, err = run_in_process(
        ["generate", "taskiq_postgresql_no_such_app:broker"],
        capsys,
    )

    assert (code, out) == (1, "")
    assert err.startswith("taskiq postgresql: error: ModuleNotFoundError: ")


def test_generate_hands_the_options_to_a_third_party_format(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def shout(schemas: list[TableSchema], options: FormatOptions) -> str:
        assert (options.transaction, options.lock, options.lock_timeout) == (
            False,
            False,
            7,
        )
        return f"{schemas[0].name} {dict(options.extra)}\n"

    class ShoutEntryPoint:
        name = "shout"

        def load(self) -> Any:
            """Return the format, as an installed entry point would."""
            return shout

    monkeypatch.setattr(migrations, "_entry_points", lambda: [ShoutEntryPoint()])

    code, out, _ = run_in_process(
        [
            "generate",
            "--component",
            "broker",
            "--format",
            "shout",
            "--option",
            "author=me",
            "--option",
            "id=a=b",
            "--no-transaction",
            "--no-lock",
            "--lock-timeout",
            "7",
        ],
        capsys,
    )

    assert code == 0
    assert out == "taskiq_messages {'author': 'me', 'id': 'a=b'}\n"


def test_a_format_two_packages_register_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class RegisteredEntryPoint:
        name = "tables"

        def __init__(self, package: str) -> None:
            self.dist = SimpleNamespace(name=package)

        def load(self) -> Any:
            """Fail the test: an ambiguous format must not be loaded."""
            raise AssertionError("loaded an ambiguous format")

    monkeypatch.setattr(
        migrations,
        "_entry_points",
        lambda: [
            RegisteredEntryPoint("acme-tables"),
            RegisteredEntryPoint("bolt-tables"),
        ],
    )

    code, out, err = run_in_process(
        ["generate", "--component", "broker", "--format", "tables"],
        capsys,
    )

    assert (code, out) == (2, "")
    assert (
        "migration format 'tables' is registered by several installed packages: "
        "acme-tables, bolt-tables; uninstall all but one of them"
    ) in err


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (
            "taskiq_postgresql.migrations:MIGRATION_FORMATS_GROUP",
            "TypeError: the migration format 'broken' names str, not a callable",
        ),
        (
            "taskiq_postgresql.migrations:no_such_format",
            (
                "AttributeError: module 'taskiq_postgresql.migrations' has no "
                "attribute 'no_such_format'"
            ),
        ),
    ],
    ids=["not-callable", "missing-attribute"],
)
def test_an_entry_point_that_names_no_format_fails_with_1(
    value: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        migrations,
        "_entry_points",
        lambda: [
            EntryPoint(
                name="broken",
                value=value,
                group=migrations.MIGRATION_FORMATS_GROUP,
            ),
        ],
    )

    code, out, err = run_in_process(
        ["generate", "--component", "broker", "--format", "broken"],
        capsys,
    )

    # One line, as for the other errors in the user's setup: no traceback.
    assert (code, out) == (1, "")
    assert err == f"taskiq postgresql: error: {message}\n"


def test_a_format_that_cannot_load_fails_with_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class BrokenEntryPoint:
        name = "yaml"
        extras = ()
        dist = None

        def load(self) -> Any:
            """Fail the way a format whose extra is missing fails."""
            raise ImportError("No module named 'yaml'")

    monkeypatch.setattr(migrations, "_entry_points", lambda: [BrokenEntryPoint()])

    code, out, err = run_in_process(
        ["generate", "--component", "broker", "--format", "yaml"],
        capsys,
    )

    assert (code, out) == (1, "")
    assert "pip install 'taskiq-postgresql[yaml]'" in err


def test_migrate_in_process_creates_the_tables_and_closes_its_pools(
    driver: str,
    app_module: str,
    tmp_path: Path,
    scratch_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    monkeypatch.setenv("APP_DRIVER", driver)

    code, out, err = run_in_process(
        ["migrate", f"{app_module}:scheduler", "--app-dir", str(tmp_path)],
        capsys,
    )

    assert code == 0, err
    assert out == ""
    assert err == f"taskiq postgresql: tables ready: {', '.join(TABLES)}\n"
    assert existing_tables(scratch_dsn) == set(TABLES)
    app = sys.modules[app_module]
    components = [app.broker, app.result_backend, app.source]
    # It applied the migrations, then put the flag and the pools back.
    assert [component.driver.run_migrations for component in components] == [
        False,
        False,
        False,
    ]
    assert [component.driver.pool for component in components] == [None, None, None]
    # No LISTEN connection was opened: the listen driver starts with None.
    assert getattr(app.broker.listen_driver, "connection", None) is None


def test_migrate_fails_with_1_when_the_database_is_unreachable(
    app_module: str,
    tmp_path: Path,
    postgresql_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parts = urlsplit(postgresql_dsn)
    unreachable = with_netloc(
        parts,
        password=parts.password,
        address=f"127.0.0.1:{closed_local_port()}",
    ).geturl()
    monkeypatch.setenv("POSTGRESQL_URL", unreachable)

    code, out, err = run_in_process(
        ["migrate", f"{app_module}:broker", "--app-dir", str(tmp_path)],
        capsys,
    )

    assert (code, out) == (1, "")
    assert err.startswith("taskiq postgresql: error: DatabaseConnectionError: ")
    assert (parts.password or "\0") not in err
