"""
Django support for the tables of taskiq-postgresql.

Install it with ``pip install 'taskiq-postgresql[django]'``, add
``"taskiq_postgresql.contrib.django"`` to ``INSTALLED_APPS`` and name the
components in ``TASKIQ_POSTGRESQL_TARGETS``: ``manage.py migrate`` then
creates their tables with the SQL ``run_migrations=True`` runs. The app has no
models, only migrations, one per migration step (see
``taskiq_postgresql.contrib.django.operations``), under the label
``taskiq_postgresql``.

``taskiq postgresql generate --format django`` writes a migration file with
the steps' SQL instead, for a project that keeps the tables in its own
migrations; that format lives in ``taskiq_postgresql.contrib.django_format``,
which needs no Django.
"""

try:
    import django  # noqa: F401  (only to fail early without the extra)
except ImportError as error:
    raise ImportError(
        "django is not installed. Please install it with "
        "`pip install taskiq-postgresql[django]`.",
    ) from error
