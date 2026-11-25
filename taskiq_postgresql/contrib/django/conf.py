"""
The settings of the Django app, and the system check that reads them.

``TASKIQ_POSTGRESQL_TARGETS`` lists the components whose tables the app's
migrations create: ``"module:object"`` paths, resolved as
``taskiq postgresql`` resolves its targets, and dicts that describe a table by
kind, as ``table_schema`` does, without importing the application::

    TASKIQ_POSTGRESQL_TARGETS = [
        "myproject.tasks:broker",
        {"kind": "scheduler_source", "table_name": "schedules"},
    ]

``TASKIQ_POSTGRESQL_LOCK_TIMEOUT`` is the seconds a statement of a step waits
for a lock before the migration fails, ``DEFAULT_LOCK_TIMEOUT`` by default,
``0`` for no bound of the step's own.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured

from taskiq_postgresql.migrations import (
    COMPONENT_KINDS,
    DEFAULT_LOCK_TIMEOUT,
    TableSchema,
    check_lock_timeout,
    check_plain_identifier,
    collect_schemas,
    parse_target,
    table_schema,
)

if TYPE_CHECKING:
    from django.apps import AppConfig

__all__ = [
    "APP_LABEL",
    "LOCK_TIMEOUT_SETTING",
    "TARGETS_SETTING",
    "check_settings",
    "configured_lock_timeout",
    "configured_schemas",
]

APP_LABEL: Final = "taskiq_postgresql"
"""The label of the app, which ``manage.py migrate`` takes."""

TARGETS_SETTING: Final = "TASKIQ_POSTGRESQL_TARGETS"
"""The setting that names the components whose tables the app migrates."""

LOCK_TIMEOUT_SETTING: Final = "TASKIQ_POSTGRESQL_LOCK_TIMEOUT"
"""The setting that bounds, in seconds, the lock waits of a step."""

_TARGET_KEYS: Final = ("kind", "table_name", "field_for_task_id")


def configured_schemas() -> list[TableSchema]:
    """
    Describe the tables ``TASKIQ_POSTGRESQL_TARGETS`` names.

    Paths are imported here, so a module that only Django's settings make
    importable resolves. A table named twice with the same columns is
    described once.

    Returns:
        The tables, in the order the setting names them.

    Raises:
        ImproperlyConfigured: if the setting is missing, empty or malformed,
            a path cannot be imported or holds no component, two entries
            describe one table with different columns, or a table name is not
            a plain SQL identifier.
    """
    targets = _targets()
    try:
        schemas = collect_schemas(*targets)
        for schema in schemas:
            # The steps write the name unquoted, and run without parameters.
            check_plain_identifier(schema.name)
    except (ImportError, AttributeError, LookupError, ValueError) as error:
        raise ImproperlyConfigured(f"{TARGETS_SETTING}: {error}") from error
    return schemas


def configured_lock_timeout() -> int:
    """
    Return the seconds a statement of a step waits for a lock.

    Returns:
        ``TASKIQ_POSTGRESQL_LOCK_TIMEOUT``, or ``DEFAULT_LOCK_TIMEOUT`` when it
        is not set; ``0`` means no bound of the step's own.

    Raises:
        ImproperlyConfigured: if it is not a whole number of seconds from 0 to
            2147483.
    """
    seconds = getattr(settings, LOCK_TIMEOUT_SETTING, DEFAULT_LOCK_TIMEOUT)
    try:
        return check_lock_timeout(seconds)
    except ValueError as error:
        raise ImproperlyConfigured(f"{LOCK_TIMEOUT_SETTING}: {error}") from error


def check_settings(
    app_configs: Optional[Sequence[AppConfig]],
    **kwargs: Any,
) -> list[checks.CheckMessage]:
    """
    Report the app's settings that ``migrate`` would refuse.

    Registered as a system check when the app is ready, so ``manage.py
    check``, and every command that runs the checks, reports them:
    ``taskiq_postgresql.E001`` for ``TASKIQ_POSTGRESQL_TARGETS`` and
    ``taskiq_postgresql.E002`` for ``TASKIQ_POSTGRESQL_LOCK_TIMEOUT``. Paths
    are only checked for their form: importing them is left to ``migrate``.

    Args:
        app_configs: The apps to check, or ``None`` for every app.
        **kwargs: What Django passes to every check.

    Returns:
        The errors, none when the settings are usable.
    """
    if app_configs is not None and all(
        config.label != APP_LABEL for config in app_configs
    ):
        return []
    errors: list[checks.CheckMessage] = []
    for read, check_id in (
        (_targets, "taskiq_postgresql.E001"),
        (configured_lock_timeout, "taskiq_postgresql.E002"),
    ):
        try:
            read()
        except ImproperlyConfigured as error:
            errors.append(checks.Error(str(error), id=check_id))
    return errors


def _targets() -> list[Union[str, TableSchema]]:
    """Read the targets, paths not yet imported and dicts described."""
    value = getattr(settings, TARGETS_SETTING, None)
    if value is None or (isinstance(value, (list, tuple)) and not value):
        state = "not set" if value is None else "empty"
        # Never a migration that does nothing: the project would believe its
        # tables exist.
        raise ImproperlyConfigured(
            f"{TARGETS_SETTING} is {state}: list the components whose tables "
            "the app creates, as 'module:object' paths or as dicts such as "
            "{'kind': 'broker'}",
        )
    if not isinstance(value, (list, tuple)):
        raise ImproperlyConfigured(
            f"{TARGETS_SETTING} must be a list, got {type(value).__name__}",
        )
    return [_target(entry) for entry in value]


def _target(entry: object) -> Union[str, TableSchema]:
    if isinstance(entry, str):
        try:
            parse_target(entry)
        except ValueError as error:
            raise ImproperlyConfigured(f"{TARGETS_SETTING}: {error}") from error
        return entry
    if not isinstance(entry, Mapping):
        raise ImproperlyConfigured(
            f"{TARGETS_SETTING}: expected a 'module:object' path or a dict, "
            f"got {type(entry).__name__}",
        )
    unknown = [key for key in entry if key not in _TARGET_KEYS]
    if unknown:
        raise ImproperlyConfigured(
            f"{TARGETS_SETTING}: unknown key {', '.join(map(repr, unknown))} in "
            f"{dict(entry)!r}; a dict takes {', '.join(_TARGET_KEYS)}",
        )
    if "kind" not in entry:
        raise ImproperlyConfigured(
            f"{TARGETS_SETTING}: {dict(entry)!r} needs a 'kind', one of "
            f"{', '.join(COMPONENT_KINDS)}",
        )
    for key in _TARGET_KEYS:
        if entry.get(key) is not None and not isinstance(entry[key], str):
            raise ImproperlyConfigured(
                f"{TARGETS_SETTING}: {key} must be a string, got "
                f"{type(entry[key]).__name__} in {dict(entry)!r}",
            )
    try:
        schema = table_schema(
            entry["kind"],
            table_name=entry.get("table_name"),
            field_for_task_id=entry.get("field_for_task_id"),
        )
        # Here rather than at migrate time, so manage.py check reports it.
        check_plain_identifier(schema.name)
    except ValueError as error:
        raise ImproperlyConfigured(
            f"{TARGETS_SETTING}: {dict(entry)!r}: {error}",
        ) from error
    return schema
