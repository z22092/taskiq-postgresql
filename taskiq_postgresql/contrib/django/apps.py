"""The Django app of taskiq-postgresql."""

from __future__ import annotations

from django.apps import AppConfig
from django.core import checks

from taskiq_postgresql.contrib.django.conf import APP_LABEL, check_settings

__all__ = ["TaskiqPostgresqlConfig"]


class TaskiqPostgresqlConfig(AppConfig):
    """
    The app ``taskiq_postgresql.contrib.django``, labelled ``taskiq_postgresql``.

    It has no models: its migrations create the tables of the components
    ``TASKIQ_POSTGRESQL_TARGETS`` names.
    """

    name = "taskiq_postgresql.contrib.django"
    label = APP_LABEL
    verbose_name = "taskiq-postgresql"

    def ready(self) -> None:
        """Register the system check of the app's settings."""
        checks.register(check_settings)
