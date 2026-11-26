"""
The Django app, ``taskiq_postgresql.contrib.django``.

Django is configured once per session (the ``django_configured`` fixture,
``tests/django_support.py``): the app is installed, and ``django_db`` points
the default database at a database created for one test and dropped after it.
The app is imported by Django itself, or inside a test, never when this module
is collected, so that without the app each test fails on its own.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import sys
import time
from pathlib import Path
from typing import Any, Optional

import psycopg
import pytest
from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.db import NotSupportedError, OperationalError, connections
from django.db.migrations.loader import MigrationLoader

from taskiq_postgresql.migrations import (
    COMPONENT_KINDS,
    DEFAULT_LOCK_TIMEOUT,
    TableSchema,
    migration_statements,
    table_schema,
)
from tests.django_support import (
    APP,
    LABEL,
    LOCK_TIMEOUT_SETTING,
    MISSING,
    SQLITE,
    TARGETS,
    applied,
    configured,
    migrate,
    migration_names,
)
from tests.helpers import (
    BROKER_TABLE_0_4_SQL,
    LOCK_TIMEOUT,
    UNUSED_DSN,
    WAIT_SECONDS,
    block,
    bounded,
    build_component,
    close_component,
    fetch_on_server,
    importable_modules,
    lock_of,
    plan_of,
    run_script,
    tables_in,
)

pytestmark = pytest.mark.usefixtures("django_configured")

DEFAULT_TARGETS = [{"kind": kind} for kind in COMPONENT_KINDS]
CUSTOM_TARGETS = [
    {"kind": "broker", "table_name": "jobs", "field_for_task_id": "Text"},
    {
        "kind": "result_backend",
        "table_name": "job_results",
        "field_for_task_id": "VarChar",
    },
    {"kind": "scheduler_source", "table_name": "job_schedules"},
]
# An application module: a broker that brings its result backend.
APP_SOURCE = f"""\
from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend

broker = PostgresqlBroker(
    dsn={UNUSED_DSN!r}, table_name="queued", field_for_task_id="Text"
).with_result_backend(PostgresqlResultBackend(dsn={UNUSED_DSN!r}, table_name="done"))
"""
# A broker whose table name the SQL cannot write unquoted.
DASHED_SOURCE = f"""\
from taskiq_postgresql import PostgresqlBroker

broker = PostgresqlBroker(dsn={UNUSED_DSN!r}, table_name="my-jobs")
"""
INSERT_0_4_ROW = (
    "INSERT INTO taskiq_messages (task_id, task_name, message, labels) "
    "VALUES (gen_random_uuid(), 'old_task', 'old', '{}')"
)


def sql_of(shown: str) -> str:
    """What ``sqlmigrate`` shows, without its comment lines."""
    return "\n".join(line for line in shown.splitlines() if not line.startswith("--"))


def transaction_of(statements: list[str]) -> str:
    return "\n".join(
        ["BEGIN;", *(f"{statement};" for statement in statements), "COMMIT;"]
    )


def assert_migrated(dsn: str, expected: list[TableSchema]) -> None:
    """``expected`` exist as ``run_migrations=True`` builds them, and nothing else."""
    plan = plan_of(dsn, expected)
    # The catalog run_migrations=True builds: every step, no problem.
    assert plan.is_current, plan.pending
    assert plan.problems == ()
    assert tables_in(dsn) == {
        *(schema.name for schema in expected),
        "django_migrations",
    }
    assert applied() == migration_names()


async def start(component: Any) -> None:
    """Start ``component``, then close what it opened."""
    try:
        await component.startup()
    finally:
        await close_component(component)


class NoTaskiqRouter:
    """A database router that keeps the app's migrations off every database."""

    def allow_migrate(self, db: str, app_label: str, **hints: Any) -> Optional[bool]:
        """Refuse the app's migrations; no opinion on any other app's."""
        return False if app_label == LABEL else None


# --- the migrations ------------------------------------------------------------


def test_there_is_one_migration_per_migration_step() -> None:
    from taskiq_postgresql.contrib.django.operations import (  # noqa: PLC0415
        RunTaskiqStep,
    )

    highest = max(
        step.number for kind in COMPONENT_KINDS for step in table_schema(kind).steps
    )
    loader = MigrationLoader(None, ignore_no_migrations=True)
    names = migration_names()

    # A release that adds a step must add the migration that runs it.
    assert len(names) == highest
    previous: Optional[str] = None
    for number, name in enumerate(names, start=1):
        migration = loader.disk_migrations[(LABEL, name)]
        assert name.startswith(f"{number:04d}_")
        assert [
            (type(operation), operation.number) for operation in migration.operations
        ] == [(RunTaskiqStep, number)]
        assert migration.dependencies == (
            [] if previous is None else [(LABEL, previous)]
        )
        previous = name


def test_makemigrations_finds_nothing_to_write(django_db: str) -> None:
    with configured(DEFAULT_TARGETS):
        # Exits with 1 if the app had changes to write.
        call_command(
            "makemigrations",
            LABEL,
            check=True,
            dry_run=True,
            verbosity=0,
        )


@pytest.mark.parametrize(
    ("targets", "expected"),
    [
        pytest.param(
            DEFAULT_TARGETS,
            [
                table_schema("broker"),
                table_schema("result_backend"),
                table_schema("scheduler_source"),
            ],
            id="defaults",
        ),
        pytest.param(
            CUSTOM_TARGETS,
            [
                table_schema("broker", table_name="jobs", field_for_task_id="Text"),
                table_schema(
                    "result_backend",
                    table_name="job_results",
                    field_for_task_id="VarChar",
                ),
                table_schema("scheduler_source", table_name="job_schedules"),
            ],
            id="custom-names",
        ),
    ],
)
def test_migrate_creates_the_configured_tables(
    targets: list[dict[str, str]],
    expected: list[TableSchema],
    django_db: str,
) -> None:
    with configured(targets):
        migrate(LABEL)

    assert_migrated(django_db, expected)


def test_migrate_creates_the_tables_of_an_import_path_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    django_db: str,
) -> None:
    with importable_modules(tmp_path, monkeypatch) as write:
        monkeypatch.syspath_prepend(str(tmp_path))
        # The dict describes the module's broker table again: it is created
        # once.
        targets = [
            f"{write(APP_SOURCE)}:broker",
            {"kind": "broker", "table_name": "queued", "field_for_task_id": "Text"},
        ]
        with configured(targets):
            migrate(LABEL)

    assert_migrated(
        django_db,
        [
            table_schema("broker", table_name="queued", field_for_task_id="Text"),
            table_schema("result_backend", table_name="done"),
        ],
    )


def test_a_path_is_not_imported_from_the_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A module that only the current directory holds must not load: a migrate
    # run from a writable directory would otherwise import whatever sits there.
    from taskiq_postgresql.contrib.django.conf import (  # noqa: PLC0415
        configured_schemas,
    )

    with importable_modules(tmp_path, monkeypatch) as write:
        monkeypatch.chdir(tmp_path)
        name = write(APP_SOURCE)
        before = list(sys.path)

        with (
            configured([f"{name}:broker"]),
            pytest.raises(ImproperlyConfigured, match=name),
        ):
            configured_schemas()

        assert sys.path == before
        assert name not in sys.modules


def test_a_0_4_broker_table_gets_step_2_from_the_second_migration(
    django_db: str,
) -> None:
    broker = table_schema("broker")
    asyncio.run(
        run_script(
            django_db, "asyncpg", BROKER_TABLE_0_4_SQL.format(table=broker.name)
        ),
    )
    asyncio.run(fetch_on_server(django_db, INSERT_0_4_ROW))
    first, second = migration_names()

    with configured([{"kind": "broker"}]):
        migrate(LABEL, first)
        after_first = plan_of(django_db, [broker])
        migrate(LABEL, second)

    assert [
        (item.step.number, item.missing_columns) for item in after_first.pending
    ] == [
        (2, ("channel", "not_before")),
    ]
    assert plan_of(django_db, [broker]).is_current
    rows = asyncio.run(
        fetch_on_server(django_db, "SELECT task_name, message FROM taskiq_messages"),
    )
    assert [(row["task_name"], bytes(row["message"])) for row in rows] == [
        ("old_task", b"old"),
    ]


@pytest.mark.parametrize(
    ("lock_timeout", "seconds"),
    [
        pytest.param(None, DEFAULT_LOCK_TIMEOUT, id="default"),
        pytest.param(3, 3, id="configured"),
        pytest.param(0, 0, id="none"),
    ],
)
def test_sqlmigrate_shows_each_step_with_the_lock_and_the_bound(
    lock_timeout: Optional[int],
    seconds: int,
    django_db: str,
) -> None:
    broker, results = table_schema("broker"), table_schema("result_backend")
    others = {} if lock_timeout is None else {LOCK_TIMEOUT_SETTING: lock_timeout}

    with configured([{"kind": "broker"}, {"kind": "result_backend"}], **others):
        shown = [
            call_command("sqlmigrate", LABEL, name, no_color=True, stdout=io.StringIO())
            for name in migration_names()
        ]

    # Each migration bounds its lock waits, the advisory lock's included,
    # then gives the transaction back its own lock_timeout; 0 bounds nothing.
    assert [sql_of(text) for text in shown] == [
        transaction_of(
            bounded(migration_statements([broker, results], through=1), seconds),
        ),
        # Only the broker table has a step 2.
        transaction_of(
            bounded([lock_of(broker), *broker.steps[1].statements], seconds)
        ),
    ]
    assert ("lock_timeout" in "".join(shown)) == bool(seconds)
    # sqlmigrate runs nothing.
    assert tables_in(django_db) == set()


def test_migrating_to_zero_leaves_the_tables_and_migrating_again_reads_the_settings(
    django_db: str,
) -> None:
    broker, results = table_schema("broker"), table_schema("result_backend")
    with configured([{"kind": "broker"}]):
        migrate(LABEL)
    # The reverse runs no SQL, so it needs no settings.
    migrate(LABEL, "zero")
    after_zero = (tables_in(django_db), applied())

    with configured([{"kind": "broker"}, {"kind": "result_backend"}]):
        migrate(LABEL)

    # The steps are additive: going back keeps the tables.
    assert after_zero == ({broker.name, "django_migrations"}, [])
    # Applied again, each migration reads the settings again and runs its
    # step for every table, the one added since included.
    assert plan_of(django_db, [broker, results]).is_current
    assert applied() == migration_names()


def test_a_held_migration_lock_makes_migrate_fail_after_the_lock_timeout(
    django_db: str,
) -> None:
    broker = table_schema("broker")
    # Another migration, or a worker's run_migrations=True, holds the lock: a
    # transaction of its own, which only the rollback below ends.
    holder = psycopg.connect(
        django_db,
        connect_timeout=WAIT_SECONDS,
        options=f"-c statement_timeout={WAIT_SECONDS}s",
    )
    try:
        holder.execute(lock_of(broker))
        started = time.monotonic()
        with (
            configured([{"kind": "broker"}], **{LOCK_TIMEOUT_SETTING: LOCK_TIMEOUT}),
            pytest.raises(OperationalError, match="lock timeout"),
        ):
            migrate(LABEL)
        waited = time.monotonic() - started
    finally:
        holder.rollback()
        holder.close()

    assert LOCK_TIMEOUT <= waited < WAIT_SECONDS
    # The migration's transaction rolled back.
    assert tables_in(django_db) == {"django_migrations"}
    assert applied() == []


def test_a_router_keeps_the_steps_off_a_database(django_db: str) -> None:
    with configured(
        DEFAULT_TARGETS,
        DATABASE_ROUTERS=[f"{__name__}.{NoTaskiqRouter.__name__}"],
    ):
        migrate(LABEL)

    # As for any app: Django records the migrations, and nothing runs.
    assert tables_in(django_db) == {"django_migrations"}
    assert applied() == migration_names()


def test_the_steps_refuse_a_database_that_is_not_postgresql() -> None:
    try:
        with (
            configured(DEFAULT_TARGETS),
            pytest.raises(NotSupportedError, match="PostgreSQL"),
        ):
            migrate(LABEL, database=SQLITE)
    finally:
        connections[SQLITE].close()


def test_after_migrate_a_component_that_does_not_migrate_starts(
    driver: str,
    django_db: str,
) -> None:
    with configured(DEFAULT_TARGETS):
        migrate(LABEL)

    for kind in COMPONENT_KINDS:
        # Its startup check finds every step: no SchemaNotReadyError.
        asyncio.run(
            start(
                build_component(kind, django_db, driver=driver, run_migrations=False)
            ),
        )


# --- settings ------------------------------------------------------------------


@pytest.mark.parametrize(
    "targets",
    [
        pytest.param(MISSING, id="missing"),
        pytest.param([], id="empty-list"),
        pytest.param((), id="empty-tuple"),
    ],
)
def test_migrate_refuses_missing_or_empty_targets(
    targets: object,
    django_db: str,
) -> None:
    with (
        configured(targets),
        pytest.raises(ImproperlyConfigured, match=TARGETS),
    ):
        migrate(LABEL)

    # Never a silent no-op: nothing is recorded as applied.
    assert applied() == []


@pytest.mark.parametrize(
    ("targets", "message"),
    [
        pytest.param(
            ["tkq_not_installed:broker"],
            "tkq_not_installed",
            id="import-error",
        ),
        pytest.param(
            [
                {"kind": "broker"},
                {"kind": "result_backend", "table_name": "taskiq_messages"},
            ],
            "the table 'taskiq_messages' is described twice",
            id="one-table-two-shapes",
        ),
    ],
)
def test_migrate_refuses_targets_it_cannot_describe(
    targets: list[Any],
    message: str,
    django_db: str,
) -> None:
    with (
        configured(targets),
        pytest.raises(ImproperlyConfigured, match=message) as raised,
    ):
        migrate(LABEL)

    assert TARGETS in str(raised.value)
    assert applied() == []


def assert_refused_before_any_statement(dsn: str, targets: list[Any]) -> None:
    """``migrate`` refuses ``targets`` for a table name: no table, no record."""
    with (
        configured(targets),
        pytest.raises(ImproperlyConfigured, match="plain SQL identifier") as raised,
    ):
        migrate(LABEL)

    assert TARGETS in str(raised.value)
    assert tables_in(dsn) <= {"django_migrations"}
    assert applied() == []


@pytest.mark.parametrize(
    "table_name",
    [
        pytest.param("my-jobs", id="dash"),
        pytest.param("jobs; DROP TABLE users", id="statement"),
    ],
)
def test_migrate_refuses_a_dict_table_name_the_sql_cannot_write(
    table_name: str,
    django_db: str,
) -> None:
    assert_refused_before_any_statement(
        django_db,
        [{"kind": "broker", "table_name": table_name}],
    )


def test_migrate_refuses_a_component_table_name_the_sql_cannot_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    django_db: str,
) -> None:
    with importable_modules(tmp_path, monkeypatch) as write:
        monkeypatch.syspath_prepend(str(tmp_path))
        # Only migrate imports the path, so only migrate can read the name.
        assert_refused_before_any_statement(
            django_db,
            [f"{write(DASHED_SOURCE)}:broker"],
        )


@pytest.mark.parametrize(
    ("setting", "value", "check_id", "message"),
    [
        pytest.param(TARGETS, MISSING, "E001", "is not set", id="missing"),
        pytest.param(TARGETS, [], "E001", "is empty", id="empty"),
        pytest.param(TARGETS, "app:broker", "E001", "must be a list", id="not-a-list"),
        pytest.param(
            TARGETS,
            ["app.broker"],
            "E001",
            "is not a 'module:object' path",
            id="not-a-path",
        ),
        pytest.param(TARGETS, [42], "E001", "got int", id="not-a-path-or-dict"),
        pytest.param(
            TARGETS,
            [{"table_name": "jobs"}],
            "E001",
            "needs a 'kind'",
            id="no-kind",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "queue"}],
            "E001",
            "unknown component kind 'queue'",
            id="unknown-kind",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "broker", "table": "jobs"}],
            "E001",
            "unknown key 'table'",
            id="unknown-key",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "scheduler_source", "field_for_task_id": "Text"}],
            "E001",
            "field_for_task_id does not apply",
            id="field-for-the-scheduler",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "broker", "table_name": 7}],
            "E001",
            "table_name must be a string",
            id="table-name-not-text",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "broker", "table_name": "my-jobs"}],
            "E001",
            "'my-jobs' is not a plain SQL identifier",
            id="table-name-with-a-dash",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "broker", "table_name": "jobs; DROP TABLE users"}],
            "E001",
            "is not a plain SQL identifier",
            id="table-name-with-a-statement",
        ),
        pytest.param(
            TARGETS,
            [{"kind": "broker", "table_name": ""}],
            "E001",
            "table name '' cannot be written",
            id="table-name-empty",
        ),
        pytest.param(
            LOCK_TIMEOUT_SETTING,
            -1,
            "E002",
            "whole number of seconds",
            id="negative-lock-timeout",
        ),
        pytest.param(
            LOCK_TIMEOUT_SETTING,
            "10",
            "E002",
            "whole number of seconds",
            id="text-lock-timeout",
        ),
        pytest.param(
            LOCK_TIMEOUT_SETTING,
            True,
            "E002",
            "whole number of seconds",
            id="bool-lock-timeout",
        ),
    ],
)
def test_the_system_check_reports_a_setting_the_app_cannot_use(
    setting: str,
    value: object,
    check_id: str,
    message: str,
) -> None:
    others: dict[str, Any] = {} if value is MISSING else {setting: value}
    targets = DEFAULT_TARGETS if setting == LOCK_TIMEOUT_SETTING else MISSING

    with (
        configured(targets, **others),
        pytest.raises(SystemCheckError) as raised,
    ):
        call_command("check", stdout=io.StringIO())

    reported = str(raised.value)
    assert f"(taskiq_postgresql.{check_id})" in reported
    assert setting in reported
    assert message in reported


def test_the_system_check_passes_without_importing_the_targets() -> None:
    # The module does not exist: the check passes because only migrate
    # imports the paths, not every manage.py command.
    with configured(
        ["tkq_not_installed:broker", {"kind": "broker", "table_name": "jobs"}],
        **{LOCK_TIMEOUT_SETTING: 0},
    ):
        call_command("check", stdout=io.StringIO())


def test_the_app_has_its_label_and_no_models() -> None:
    config = apps.get_app_config(LABEL)

    assert config.name == APP
    assert list(config.get_models()) == []


def test_the_app_without_django_says_what_to_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block(monkeypatch, "django")
    for name in [name for name in sys.modules if name.startswith(APP)]:
        monkeypatch.delitem(sys.modules, name)

    with pytest.raises(ImportError, match=r"pip install taskiq-postgresql\[django\]"):
        importlib.import_module(APP)


def test_a_reserved_word_as_a_table_name_is_refused_by_the_setting() -> None:
    from taskiq_postgresql.contrib.django.conf import (  # noqa: PLC0415
        configured_schemas,
    )

    with (
        configured([{"kind": "broker", "table_name": "order"}]),
        pytest.raises(ImproperlyConfigured, match="reserved word"),
    ):
        configured_schemas()
