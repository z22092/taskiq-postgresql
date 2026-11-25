"""
Django for the tests, configured without pytest-django.

Django is configured once per process with ``settings.configure()``: the app
``taskiq_postgresql.contrib.django`` installed, and a default database that
names nothing until ``django_database`` points it at a database a test
created. Its connection settings come from a DSN at run time, with a
server-side ``statement_timeout``, so no statement can wait for ever. The
commands run through ``call_command``, as ``manage.py`` runs them.

Only the tests that use Django import this module.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

import django
from django.conf import settings
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.recorder import MigrationRecorder
from django.test.utils import override_settings

from tests.helpers import WAIT_SECONDS

APP = "taskiq_postgresql.contrib.django"
LABEL = "taskiq_postgresql"
TARGETS = "TASKIQ_POSTGRESQL_TARGETS"
LOCK_TIMEOUT_SETTING = "TASKIQ_POSTGRESQL_LOCK_TIMEOUT"
# The alias of a database that is not PostgreSQL; it lives in memory.
SQLITE = "sqlite"
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


def configured(targets: object = MISSING, **others: Any) -> Any:
    """Override the app's settings; ``MISSING`` leaves the targets undefined."""
    if targets is not MISSING:
        others[TARGETS] = targets
    return override_settings(**others)


def migrate(*args: str, **options: Any) -> None:
    """Run ``manage.py migrate`` with ``args``, quietly."""
    call_command("migrate", *args, verbosity=0, **options)


def migration_names() -> list[str]:
    """The app's migrations, in order."""
    loader = MigrationLoader(None, ignore_no_migrations=True)
    return sorted(name for app, name in loader.disk_migrations if app == LABEL)


def applied() -> list[str]:
    """The app's migrations Django recorded as applied to the default database."""
    recorder = MigrationRecorder(connections[DEFAULT_DB_ALIAS])
    return sorted(name for app, name in recorder.applied_migrations() if app == LABEL)
