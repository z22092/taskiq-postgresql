"""
The Django app, ``taskiq_postgresql.contrib.django``.

Django is configured once per session with ``settings.configure()``, without
pytest-django: the app is installed, and the default database is pointed at a
database created for one test and dropped after it. Its connection settings
come from the ``postgresql_dsn`` fixture at run time, with a server-side
``statement_timeout``, so no statement can wait for ever. The commands run
through ``call_command``, as ``manage.py`` runs them.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import io
import random
import string
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qsl, unquote, urlsplit

import django
import psycopg
import pytest
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.db import DEFAULT_DB_ALIAS, NotSupportedError, OperationalError, connections
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.recorder import MigrationRecorder
from django.test.utils import override_settings

from taskiq_postgresql.contrib.django.operations import RunTaskiqStep
from taskiq_postgresql.migrations import (
    COMPONENT_KINDS,
    DEFAULT_LOCK_TIMEOUT,
    MigrationPlan,
    TableSchema,
    collect_schemas,
    migration_statements,
    plan_migration,
    read_catalog,
    table_schema,
)
from tests.helpers import (
    UNUSED_DSN,
    build_component,
    create_database,
    drop_database,
    fetch_on_server,
    importable_modules,
    in_database,
    run_script,
)
from tests.test_contrib_alembic import bounded
from tests.test_contrib_sqlalchemy import block, tables_in
from tests.test_migration_script import LOCK_TIMEOUT, WAIT_SECONDS
from tests.test_migrations import BROKER_TABLE_0_4_SQL, close_component

pytestmark = pytest.mark.usefixtures("django_configured")

APP = "taskiq_postgresql.contrib.django"
LABEL = "taskiq_postgresql"
TARGETS = "TASKIQ_POSTGRESQL_TARGETS"
LOCK_TIMEOUT_SETTING = "TASKIQ_POSTGRESQL_LOCK_TIMEOUT"
# The alias of a database that is not PostgreSQL; it lives in memory.
SQLITE = "sqlite"
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
INSERT_0_4_ROW = (
    "INSERT INTO taskiq_messages (task_id, task_name, message, labels) "
    "VALUES (gen_random_uuid(), 'old_task', 'old', '{}')"
)
# Stands for a setting the project does not define.
MISSING = object()


def configure_django() -> None:
    """
    Configure Django for this process, once: the app installed, no database yet.

    The default database names nothing until ``django_database`` points it at
    one. Django leaves logging alone, so pytest's capture keeps working.
    """
    if settings.configured:
        return
    settings.configure(
        DATABASES={
            DEFAULT_DB_ALIAS: {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": "taskiq_unused",
            },
            SQLITE: {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        },
        INSTALLED_APPS=[APP],
        USE_TZ=True,
        LOGGING_CONFIG=None,
    )
    django.setup()


def database_settings(dsn: str) -> dict[str, Any]:
    """Return Django's settings of the database ``dsn`` names, read at run time."""
    parts = urlsplit(dsn)
    options: dict[str, Any] = dict(parse_qsl(parts.query))
    # Server-side, so a statement waiting on a lock ends even if nothing
    # else bounds it.
    options["options"] = f"-c statement_timeout={WAIT_SECONDS}s"
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": unquote(parts.path.lstrip("/")),
        "USER": unquote(parts.username or ""),
        "PASSWORD": unquote(parts.password or ""),
        "HOST": parts.hostname or "",
        "PORT": str(parts.port or ""),
        "OPTIONS": options,
    }


@contextlib.contextmanager
def django_database(dsn: str) -> Iterator[None]:
    """Point Django's default database at ``dsn``, then close and restore it."""
    connection = connections[DEFAULT_DB_ALIAS]
    saved = dict(connection.settings_dict)
    connection.close()
    # The connection of every thread reads this dict when it connects.
    connection.settings_dict.update(database_settings(dsn))
    try:
        yield
    finally:
        connection.close()
        connection.settings_dict.clear()
        connection.settings_dict.update(saved)


@pytest.fixture(scope="session")
def django_configured() -> None:
    configure_django()


@pytest.fixture
def django_db(postgresql_dsn: str) -> Iterator[str]:
    """DSN of a database created for one test, Django's default database."""
    name = "django_" + "".join(random.choices(string.ascii_lowercase, k=10))
    asyncio.run(create_database(postgresql_dsn, name))
    dsn = in_database(postgresql_dsn, name)
    try:
        with django_database(dsn):
            yield dsn
    finally:
        asyncio.run(drop_database(postgresql_dsn, name))


def configured(targets: object = MISSING, **others: Any) -> Any:
    """Override the app's settings; ``MISSING`` leaves the targets undefined."""
    if targets is not MISSING:
        others[TARGETS] = targets
    return override_settings(**others)


def migrate(*args: str, **options: Any) -> None:
    call_command("migrate", *args, verbosity=0, **options)


def migration_names() -> list[str]:
    """The app's migrations, in order."""
    loader = MigrationLoader(None, ignore_no_migrations=True)
    return sorted(name for app, name in loader.disk_migrations if app == LABEL)


def applied() -> list[str]:
    """The app's migrations Django recorded as applied to the default database."""
    recorder = MigrationRecorder(connections[DEFAULT_DB_ALIAS])
    return sorted(name for app, name in recorder.applied_migrations() if app == LABEL)


def schemas_of(targets: list[Any]) -> list[TableSchema]:
    """Describe ``targets`` as the setting names them, each table once."""
    described: dict[str, TableSchema] = {}
    for target in targets:
        found = (
            collect_schemas(target)
            if isinstance(target, str)
            else [
                table_schema(
                    target["kind"],
                    table_name=target.get("table_name"),
                    field_for_task_id=target.get("field_for_task_id"),
                ),
            ]
        )
        for schema in found:
            described.setdefault(schema.name, schema)
    return list(described.values())


def plan_of(dsn: str, schemas: list[TableSchema]) -> MigrationPlan:
    """Compare the database ``dsn`` names with ``schemas``."""

    async def fetch_value(query: str) -> Any:
        (row,) = await fetch_on_server(dsn, query)
        return row[0]

    return plan_migration(schemas, asyncio.run(read_catalog(schemas, fetch_value)))


def lock_of(schema: TableSchema) -> str:
    return migration_statements([schema])[0]


def sql_of(shown: str) -> str:
    """What ``sqlmigrate`` shows, without its comment lines."""
    return "\n".join(line for line in shown.splitlines() if not line.startswith("--"))


def transaction_of(statements: list[str]) -> str:
    return "\n".join(
        ["BEGIN;", *(f"{statement};" for statement in statements), "COMMIT;"]
    )


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


@pytest.mark.parametrize("case", ["defaults", "custom-names", "import-path"])
def test_migrate_creates_the_configured_tables(
    case: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    django_db: str,
) -> None:
    with importable_modules(tmp_path, monkeypatch) as write:
        monkeypatch.syspath_prepend(str(tmp_path))
        targets: list[Any] = {
            "defaults": lambda: DEFAULT_TARGETS,
            "custom-names": lambda: CUSTOM_TARGETS,
            # The dict describes the module's broker table again: it is
            # created once.
            "import-path": lambda: [
                f"{write(APP_SOURCE)}:broker",
                {"kind": "broker", "table_name": "queued", "field_for_task_id": "Text"},
            ],
        }[case]()
        expected = schemas_of(targets)
        with configured(targets):
            migrate(LABEL)

    plan = plan_of(django_db, expected)
    # The catalog run_migrations=True builds: every step, no problem.
    assert plan.is_current, plan.pending
    assert plan.problems == ()
    assert tables_in(django_db) == {
        *(schema.name for schema in expected),
        "django_migrations",
    }
    assert applied() == migration_names()
    if case == "import-path":
        assert [schema.name for schema in expected] == ["queued", "done"]


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
    "lock_timeout",
    [None, 3, 0],
    ids=["default", "configured", "none"],
)
def test_sqlmigrate_shows_each_step_with_the_lock_and_the_bound(
    lock_timeout: Optional[int],
    django_db: str,
) -> None:
    broker, results = table_schema("broker"), table_schema("result_backend")
    others = {} if lock_timeout is None else {LOCK_TIMEOUT_SETTING: lock_timeout}
    seconds = DEFAULT_LOCK_TIMEOUT if lock_timeout is None else lock_timeout

    with configured([{"kind": "broker"}, {"kind": "result_backend"}], **others):
        shown = [
            call_command("sqlmigrate", LABEL, name, no_color=True, stdout=io.StringIO())
            for name in migration_names()
        ]

    # Each migration bounds its lock waits, the advisory lock's included,
    # then gives the transaction back its own lock_timeout.
    assert [sql_of(text) for text in shown] == [
        transaction_of(
            bounded(migration_statements([broker, results], through=1), seconds),
        ),
        # Only the broker table has a step 2.
        transaction_of(
            bounded([lock_of(broker), *broker.steps[1].statements], seconds)
        ),
    ]
    if lock_timeout == 0:
        assert "lock_timeout" not in "".join(shown)
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

    # The steps are additive: going back keeps the tables and their rows.
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
    [MISSING, [], ()],
    ids=["missing", "empty-list", "empty-tuple"],
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
        (["tkq_not_installed:broker"], "tkq_not_installed"),
        (
            [
                {"kind": "broker"},
                {"kind": "result_backend", "table_name": "taskiq_messages"},
            ],
            "two entries use the table 'taskiq_messages'",
        ),
    ],
    ids=["import-error", "one-table-two-shapes"],
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


@pytest.mark.parametrize(
    ("setting", "value", "check_id", "message"),
    [
        (TARGETS, MISSING, "E001", "is not set"),
        (TARGETS, [], "E001", "is empty"),
        (TARGETS, "app:broker", "E001", "must be a list"),
        (TARGETS, ["app.broker"], "E001", "is not a 'module:object' path"),
        (TARGETS, [42], "E001", "got int"),
        (TARGETS, [{"table_name": "jobs"}], "E001", "needs a 'kind'"),
        (TARGETS, [{"kind": "queue"}], "E001", "unknown component kind 'queue'"),
        (TARGETS, [{"kind": "broker", "table": "jobs"}], "E001", "unknown key 'table'"),
        (
            TARGETS,
            [{"kind": "scheduler_source", "field_for_task_id": "Text"}],
            "E001",
            "field_for_task_id does not apply",
        ),
        (
            TARGETS,
            [{"kind": "broker", "table_name": 7}],
            "E001",
            "table_name must be a string",
        ),
        (LOCK_TIMEOUT_SETTING, -1, "E002", "whole number of seconds"),
        (LOCK_TIMEOUT_SETTING, "10", "E002", "whole number of seconds"),
        (LOCK_TIMEOUT_SETTING, True, "E002", "whole number of seconds"),
    ],
    ids=[
        "missing",
        "empty",
        "not-a-list",
        "not-a-path",
        "not-a-path-or-dict",
        "no-kind",
        "unknown-kind",
        "unknown-key",
        "field-for-the-scheduler",
        "table-name-not-text",
        "negative-lock-timeout",
        "text-lock-timeout",
        "bool-lock-timeout",
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
