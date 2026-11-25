"""
``taskiq postgresql plan`` and ``taskiq postgresql check``.

Both read one database's catalog in one read-only query and compare it with
the tables' description. ``plan`` prints, and only on stdout, the SQL of the
steps that database still needs; ``check`` reports on stderr (and as JSON on
stdout with ``--json``), and its exit status says whether a table or column
is missing (3) or something no step fixes needs a person (4).
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from importlib.metadata import version
from pathlib import Path
from typing import Callable, Iterator
from urllib.parse import urlsplit

import asyncpg
import pytest

from taskiq_postgresql.migrations import FormatOptions, render_sql, table_schema
from tests.helpers import (
    closed_local_port,
    fetch_on_server,
    importable_modules,
    new_database,
    run_script,
    with_netloc,
)
from tests.test_cli import (
    APP,
    TABLES,
    UNUSED_DSN,
    expected_app_schemas,
    run_in_process,
    taskiq,
)
from tests.test_migration_plan import ALTER_NOTE, INDEX_NOTE
from tests.test_migration_script import catalog
from tests.test_migrations import BROKER_TABLE_0_4_SQL, role_with

PROG = "taskiq postgresql"
BROKER = table_schema("broker")
OLD_SQL = BROKER_TABLE_0_4_SQL.format(table=BROKER.name)
CURRENT_SQL = render_sql([BROKER])
# What a failed CREATE INDEX CONCURRENTLY leaves (superuser only).
INVALIDATE_SQL = (
    "UPDATE pg_index SET indisvalid = false"
    " WHERE indexrelid = to_regclass('taskiq_messages_not_before_idx');"
)


def taken_by_another_table(index: str) -> str:
    """SQL that gives ``index``'s name to an index of another table first."""
    return f"CREATE TABLE other (id INTEGER);\nCREATE INDEX {index} ON other (id);\n"


# The plan for a broker table 0.4.x created: step 2 only.
PLAN_GOLDEN = (
    "-- taskiq-postgresql {version}\n"
    "-- plan for database {database} (search_path {search_path}): valid only "
    "for this database\n"
    "-- pending: taskiq_messages step 2\n"
    "BEGIN;\n"
    "SET LOCAL lock_timeout = '10s';\n"
    "SELECT pg_advisory_xact_lock(hashtext('taskiq_postgresql'), "
    "hashtext('taskiq_messages'));\n"
    "ALTER TABLE taskiq_messages\n"
    "  ADD COLUMN IF NOT EXISTS channel VARCHAR,\n"
    "  ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL "
    "DEFAULT NOW();\n"
    "CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx\n"
    "  ON taskiq_messages (channel, not_before, id);\n"
    "COMMIT;\n"
)

# The same broker table, as check --json reports it.
CHECK_JSON_GOLDEN = {
    "current": False,
    "pending": [
        {
            "table": "taskiq_messages",
            "kind": "broker",
            "step": 2,
            "severity": "required",
            "missing": {
                "table": False,
                "columns": ["channel", "not_before"],
                "indexes": ["taskiq_messages_not_before_idx"],
            },
        },
    ],
    "problems": [],
    "advisory_only": False,
}

# One app, two connection strings to the same database.
TWO_DSNS_APP = """\
import os
from urllib.parse import urlsplit

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend

dsn = os.environ["POSTGRESQL_URL"]
parts = urlsplit(dsn)
results_dsn = parts._replace(
    query="&".join(filter(None, [parts.query, "application_name=results"])),
).geturl()
broker = PostgresqlBroker(dsn=dsn).with_result_backend(
    PostgresqlResultBackend(dsn=results_dsn),
)
"""


@pytest.fixture
def scratch_dsn(postgresql_dsn: str) -> Iterator[str]:
    """DSN of a database created empty for one test and dropped after it."""
    yield from new_database(postgresql_dsn)


@pytest.fixture
def fresh_dsn(postgresql_dsn: str) -> Iterator[str]:
    """A second empty database, for a new install to compare with."""
    yield from new_database(postgresql_dsn)


@pytest.fixture
def app_module(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[[str], str]]:
    """Write a module into ``tmp_path`` and return its name, unique per call."""
    with importable_modules(tmp_path, monkeypatch) as write:
        yield write


def execute(dsn: str, script: str) -> None:
    """Run ``script``, statements and comments as a tool sends them, on ``dsn``."""
    asyncio.run(run_script(dsn, "asyncpg", script))


def database_of(dsn: str) -> tuple[str, str]:
    """Return the database name and search path a plan header shows for ``dsn``."""
    (row,) = asyncio.run(
        fetch_on_server(
            dsn,
            "SELECT current_database()::text, current_schemas(false)::text",
        ),
    )
    return row[0], row[1]


def broker_options(dsn: str) -> list[str]:
    return ["--component", "broker", "--dsn", dsn]


# --- plan ---------------------------------------------------------------------


def test_plan_prints_only_step_2_for_a_0_4_broker_table(
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    execute(scratch_dsn, OLD_SQL)
    database, search_path = database_of(scratch_dsn)

    code, out, err = run_in_process(["plan", *broker_options(scratch_dsn)], capsys)

    assert code == 0, err
    assert out == PLAN_GOLDEN.format(
        version=version("taskiq-postgresql"),
        database=database,
        search_path=search_path,
    )
    # The locks the step takes go to stderr, never into the script.
    assert err == f"{PROG}: note: {ALTER_NOTE}\n{PROG}: note: {INDEX_NOTE}\n"


@pytest.mark.parametrize(
    ("setup", "code", "problem"),
    [
        pytest.param(CURRENT_SQL, 0, None, id="current"),
        pytest.param(
            taken_by_another_table("taskiq_messages_id_idx") + CURRENT_SQL,
            4,
            "index_on_other_table",
            id="index-on-another-table",
        ),
    ],
)
def test_plan_prints_nothing_when_no_step_is_pending(
    setup: str,
    code: int,
    problem: str | None,
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    execute(scratch_dsn, setup)

    status, out, err = run_in_process(["plan", *broker_options(scratch_dsn)], capsys)

    assert (status, out) == (code, "")
    assert f"{PROG}: tables up to date: taskiq_messages\n" in err
    if problem is not None:
        assert f"{PROG}: problem: {problem}: " in err


def test_plan_prints_the_steps_and_exits_with_4_when_a_person_is_needed(
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    execute(
        scratch_dsn,
        OLD_SQL + taken_by_another_table("taskiq_messages_not_before_idx"),
    )

    code, out, err = run_in_process(["plan", *broker_options(scratch_dsn)], capsys)

    assert code == 4  # noqa: PLR2004
    assert "\n-- pending: taskiq_messages step 2\n" in out
    assert "ALTER TABLE taskiq_messages\n" in out
    assert f"{PROG}: problem: index_on_other_table: " in err


@pytest.mark.parametrize(
    ("options", "present", "absent"),
    [
        (
            ["--lock-timeout", "3"],
            ["BEGIN;", "SET LOCAL lock_timeout = '3s';", "pg_advisory_xact_lock"],
            ["'10s'"],
        ),
        (
            ["--lock-timeout", "0"],
            ["BEGIN;", "pg_advisory_xact_lock"],
            ["lock_timeout"],
        ),
        (
            ["--no-transaction", "--no-lock"],
            ["SET LOCAL lock_timeout = '10s';", "ALTER TABLE"],
            ["BEGIN", "COMMIT", "pg_advisory_xact_lock"],
        ),
    ],
    ids=["three-seconds", "zero-leaves-it-out", "no-transaction-no-lock"],
)
def test_plan_takes_the_options_of_generate(
    options: list[str],
    present: list[str],
    absent: list[str],
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    execute(scratch_dsn, OLD_SQL)

    code, out, err = run_in_process(
        ["plan", *broker_options(scratch_dsn), *options],
        capsys,
    )

    assert code == 0, err
    assert out.splitlines()[2] == "-- pending: taskiq_messages step 2"
    for text in present:
        assert text in out
    for text in absent:
        assert text not in out


@pytest.mark.alembic
def test_plan_writes_a_revision_of_only_the_pending_steps(
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("alembic")
    from taskiq_postgresql.contrib.alembic import alembic_format  # noqa: PLC0415

    execute(scratch_dsn, OLD_SQL)
    database, search_path = database_of(scratch_dsn)

    code, out, err = run_in_process(
        ["plan", *broker_options(scratch_dsn), "--format", "alembic"],
        capsys,
    )

    assert code == 0, err
    # The file itself names the database it was planned for, as the SQL does.
    assert out == (
        f"# plan for database {database} (search_path {search_path}): valid only "
        "for this database\n"
    ) + alembic_format(
        [dataclasses.replace(BROKER, steps=BROKER.steps[1:])],
        FormatOptions(),
    )
    assert "step 2 of the broker table taskiq_messages" in out
    assert "step 1 " not in out
    assert f"{PROG}: note: plan for database {database} " in err


def test_the_plan_brings_a_database_to_what_a_new_install_holds(
    driver: str,
    app_module: Callable[[str], str],
    tmp_path: Path,
    scratch_dsn: str,
    fresh_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Through the app, so each driver reads the catalog.
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    monkeypatch.setenv("APP_DRIVER", driver)
    target = [f"{app_module(APP)}:scheduler", "--app-dir", str(tmp_path)]
    execute(scratch_dsn, BROKER_TABLE_0_4_SQL.format(table="cli_messages"))

    code, script, err = run_in_process(["plan", *target], capsys)
    assert code == 0, err
    assert (
        "\n-- pending: cli_messages step 2, cli_results step 1, cli_schedules step 1\n"
    ) in script
    asyncio.run(run_script(scratch_dsn, driver, script))

    code, out, err = run_in_process(["check", *target], capsys)
    assert (code, out) == (0, "")
    assert err == f"{PROG}: tables up to date: {', '.join(TABLES)}\n"
    assert run_in_process(["plan", *target], capsys)[:2] == (0, "")

    execute(fresh_dsn, render_sql(expected_app_schemas()))

    async def same_catalogs() -> None:
        planned = await asyncpg.connect(scratch_dsn)
        fresh = await asyncpg.connect(fresh_dsn)
        try:
            for table in TABLES:
                assert await catalog(planned, table) == await catalog(fresh, table)
        finally:
            await planned.close()
            await fresh.close()

    asyncio.run(same_catalogs())


def test_plan_reads_one_database_and_check_reads_each(
    app_module: Callable[[str], str],
    tmp_path: Path,
    scratch_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    target = [f"{app_module(TWO_DSNS_APP)}:broker", "--app-dir", str(tmp_path)]

    code, out, err = run_in_process(["plan", *target], capsys)
    assert (code, out) == (1, "")
    assert "2 connection strings" in err

    code, out, err = run_in_process(["plan", *target, "--dsn", scratch_dsn], capsys)
    assert code == 0, err
    assert (
        "\n-- pending: taskiq_messages step 1, taskiq_messages step 2, "
        "taskiq_results step 1\n"
    ) in out

    code, out, err = run_in_process(["check", *target], capsys)
    assert (code, out) == (3, "")
    assert f"{PROG}: pending: taskiq_messages step 1 (required): " in err
    assert f"{PROG}: pending: taskiq_results step 1 (required): " in err


def test_plan_fails_with_1_on_a_target_without_a_component(
    app_module: Callable[[str], str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", UNUSED_DSN)

    code, out, err = run_in_process(
        ["plan", f"{app_module(APP)}:foreign_broker", "--app-dir", str(tmp_path)],
        capsys,
    )

    assert (code, out) == (1, "")
    assert "holds no taskiq-postgresql component" in err


# --- check --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("setup", "code", "advisory_only", "problems"),
    [
        pytest.param("", 3, False, [], id="no-table"),
        pytest.param(OLD_SQL, 3, False, [], id="0.4-table"),
        pytest.param(CURRENT_SQL, 0, False, [], id="current"),
        pytest.param(
            f"{CURRENT_SQL}DROP INDEX taskiq_messages_id_idx;",
            0,
            True,
            [],
            id="only-an-index-missing",
        ),
        pytest.param(
            f"{CURRENT_SQL}{INVALIDATE_SQL}",
            4,
            False,
            ["invalid_index"],
            id="invalid-index",
        ),
        pytest.param(
            taken_by_another_table("taskiq_messages_id_idx") + CURRENT_SQL,
            4,
            False,
            ["index_on_other_table"],
            id="index-on-another-table",
        ),
    ],
)
def test_check_exits_with_the_state_of_the_database(
    setup: str,
    code: int,
    advisory_only: bool,
    problems: list[str],
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    if setup:
        execute(scratch_dsn, setup)

    status, out, err = run_in_process(
        ["check", *broker_options(scratch_dsn), "--json"],
        capsys,
    )

    assert status == code, err
    report = json.loads(out)
    assert report["advisory_only"] is advisory_only
    assert [problem["code"] for problem in report["problems"]] == problems
    for problem in problems:
        assert f"{PROG}: problem: {problem}: " in err


def test_check_json_is_the_documented_report(
    scratch_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    execute(scratch_dsn, OLD_SQL)

    code, out, err = run_in_process(
        ["check", *broker_options(scratch_dsn), "--json"],
        capsys,
    )

    assert code == 3  # noqa: PLR2004
    assert json.loads(out) == CHECK_JSON_GOLDEN
    assert out.endswith("}\n")
    assert err == (
        f"{PROG}: pending: taskiq_messages step 2 (required): missing columns: "
        "channel, not_before; missing indexes: taskiq_messages_not_before_idx\n"
    )


def test_check_fails_with_1_when_the_database_refuses(
    postgresql_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parts = urlsplit(postgresql_dsn)
    unreachable = with_netloc(
        parts,
        password=parts.password,
        address=f"127.0.0.1:{closed_local_port()}",
    ).geturl()

    code, out, err = run_in_process(["check", *broker_options(unreachable)], capsys)

    assert (code, out) == (1, "")
    assert err.startswith(f"{PROG}: error: DatabaseConnectionError: ")
    assert (parts.password or "\0") not in err


@pytest.mark.anyio
async def test_a_role_without_ddl_privileges_can_plan_and_check(
    postgresql_dsn: str,
    postgres_table: str,
) -> None:
    admin = await asyncpg.connect(postgresql_dsn)
    try:
        await admin.execute(
            render_sql([table_schema("broker", table_name=postgres_table)]),
        )
        async with role_with(
            admin,
            postgresql_dsn,
            postgres_table,
            "reader",
            "SELECT, INSERT, UPDATE, DELETE",
            sequence=True,
        ) as role_dsn:
            options = ["--table-name", postgres_table, *broker_options(role_dsn)]
            checked = taskiq("postgresql", "check", *options)
            planned = taskiq("postgresql", "plan", *options)
    finally:
        await admin.execute(f"DROP TABLE IF EXISTS {postgres_table}")
        await admin.close()

    for result in (checked, planned):
        assert (result.returncode, result.stdout) == (0, ""), result.stderr
        assert f"tables up to date: {postgres_table}" in result.stderr


# --- usage, help and installs without a driver --------------------------------


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["plan"], "give one or more TARGETs or --component"),
        (["check"], "give one or more TARGETs or --component"),
        (["plan", "--component", "broker"], "--component needs --dsn"),
        (["check", "--component", "broker"], "--component needs --dsn"),
        (["check", "app:broker", "--table-name", "jobs"], "need --component"),
        (
            [*broker_options(UNUSED_DSN), "--lock-timeout", "-1"],
            "--lock-timeout: lock_timeout must be a whole number of seconds",
        ),
        (
            [*broker_options(UNUSED_DSN), "--format", "nope"],
            "unknown migration format 'nope'",
        ),
        (
            [*broker_options(UNUSED_DSN), "--option", "author=me"],
            "the sql format takes no --option; got author",
        ),
        (
            [
                "--component",
                "scheduler_source",
                "--field-for-task-id",
                "Text",
                "--dsn",
                UNUSED_DSN,
            ],
            "no task_id column",
        ),
    ],
    ids=[
        "plan-without-target",
        "check-without-target",
        "plan-component-without-dsn",
        "check-component-without-dsn",
        "table-name-without-component",
        "negative-lock-timeout",
        "unknown-format",
        "option-the-sql-format-refuses",
        "field-on-the-schedule-table",
    ],
)
def test_a_usage_error_exits_with_2_before_connecting(
    args: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The options alone name plan's; the commands come first otherwise.
    command = args if args[0] in {"plan", "check"} else ["plan", *args]

    code, out, err = run_in_process(command, capsys)

    assert code == 2  # noqa: PLR2004
    assert out == ""
    assert message in err


@pytest.mark.parametrize("option", ["--format=sql", "--no-lock", "--lock-timeout=3"])
def test_check_takes_no_output_options(
    option: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, _, err = run_in_process(
        ["check", *broker_options(UNUSED_DSN), option],
        capsys,
    )

    assert code == 2  # noqa: PLR2004
    assert f"unrecognized arguments: {option}" in err


@pytest.mark.parametrize(
    ("command", "options"),
    [
        ("plan", ["--dsn DSN", "--lock-timeout SECONDS", "--no-transaction"]),
        ("check", ["--dsn DSN", "--json"]),
    ],
)
def test_plan_and_check_help_exit_with_0(
    command: str,
    options: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, out, _ = run_in_process([command, "--help"], capsys)

    assert code == 0
    assert "Exit status: 0" in out
    for option in options:
        assert option in out


def test_check_without_any_driver_helps_and_says_what_to_install() -> None:
    helped = taskiq("postgresql", "check", "--help", without_drivers=True)
    failed = taskiq(
        "postgresql",
        "check",
        *broker_options(UNUSED_DSN),
        without_drivers=True,
    )

    assert helped.returncode == 0, helped.stderr
    assert "--json" in helped.stdout
    assert failed.returncode == 1
    assert "pip install taskiq-postgresql[asyncpg]" in failed.stderr
