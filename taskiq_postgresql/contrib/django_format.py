"""
The ``django`` migration format: a Django migration file.

``taskiq postgresql generate --format django`` writes it for a project that
keeps the tables in a migration of its own instead of installing the app
``taskiq_postgresql.contrib.django``. The steps' SQL is written out, so the
file never imports taskiq-postgresql and runs the same statements whatever
release is installed later. Writing the file needs no Django: this module
imports none, so ``generate`` and ``plan`` take ``--format django`` without
the ``django`` extra, which only the app needs.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Final, Optional

from taskiq_postgresql.migrations import (
    FormatOptions,
    MigrationFormatOptionError,
    RenderedStep,
    TableSchema,
    package_version,
    render_steps,
    statements_effect,
)

__all__ = ["django_format"]

_OPTIONS: Final = ("dependencies",)
# app_label.migration_name: an app label is a Python identifier, and a
# migration name is the name of a module of the app's migrations package.
_DEPENDENCY: Final = re.compile(
    r"(?P<app>[A-Za-z_][A-Za-z0-9_]*)\.(?P<name>[A-Za-z0-9_]+)",
)
_INDENT: Final = "    "
_TRANSACTION_NOTE: Final = (
    "The lock and the bound need the migration's transaction: keep it atomic."
)
_MIGRATION: Final = '''\
"""Create or complete the taskiq-postgresql tables.

Written by `taskiq postgresql generate --format django` (taskiq-postgresql
{version}). Each statement {effect}, so the migration is safe
on a database that already has the tables, and reversing it runs nothing: the
tables stay. The SQL is written out, so the migration never imports
taskiq-postgresql.
"""

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = {dependencies}

    operations = {operations}
'''


def django_format(
    schemas: Sequence[TableSchema],
    options: FormatOptions,
    *,
    comments: Sequence[str] = (),
) -> str:
    """
    Render the ``django`` migration format: a migration file.

    The migration holds one ``migrations.RunSQL`` per step of each table, the
    tables by name, each with the step's SQL written out, the advisory lock
    ``run_migrations=True`` takes first. Django runs a migration in a
    transaction of its own, so there is no ``BEGIN`` or ``COMMIT`` and
    ``options.transaction`` changes nothing; ``options.lock`` keeps or leaves
    out the locks. Each step bounds its own lock waits with
    ``options.lock_timeout`` and gives the transaction back the
    ``lock_timeout`` it had. Reversing a step runs nothing
    (``RunSQL.noop``): it only added what an earlier release ignores.
    ``comments`` open the file, one comment line each: ``taskiq postgresql
    plan`` names there the database the migration was planned for.

    Options (``--option KEY=VALUE``):

    - ``dependencies``: the migrations it follows, as
      ``app_label.migration_name``, separated by commas, such as
      ``shop.0004_orders``; usually the latest migration of the app the file
      goes into. None by default.

    Args:
        schemas: The tables.
        options: The options of ``taskiq postgresql generate``.
        comments: Lines written as comments at the top of the file, before
            its docstring.

    Returns:
        The migration file.

    Raises:
        MigrationFormatOptionError: if an option is unknown or its value is
            not one the format accepts.
        ValueError: if a table name is not a plain SQL identifier (the SQL
            writes it unquoted), a comment holds a line break, a NUL or an
            encoding declaration, or as ``RenderedStep`` does.
        TypeError: as ``RenderedStep`` does.
    """
    steps = render_steps(
        schemas,
        options,
        format_name="django",
        accepted=_OPTIONS,
        transaction_note=_TRANSACTION_NOTE,
        comments=comments,
    )
    dependencies = _dependencies(options.extra.get("dependencies"))
    return "".join(f"# {comment}\n" for comment in comments) + _MIGRATION.format(
        version=package_version(),
        effect=statements_effect([step for schema in schemas for step in schema.steps]),
        dependencies=_block([f"({app!r}, {name!r})," for app, name in dependencies]),
        operations=_block([line for step in steps for line in _operation(step)]),
    )


def _dependencies(text: Optional[str]) -> list[tuple[str, str]]:
    if text is None:
        return []
    found = []
    for item in text.split(","):
        match = _DEPENDENCY.fullmatch(item)
        if match is None:
            raise MigrationFormatOptionError(
                "dependencies must be app_label.migration_name pairs separated "
                f"by commas, such as shop.0004_orders; got {text!r}",
            )
        found.append((match["app"], match["name"]))
    return found


def _operation(step: RenderedStep) -> list[str]:
    return [
        *(f"# {comment}" for comment in step.comments),
        "migrations.RunSQL(",
        f"{_INDENT}sql=[",
        # repr() writes any text as one valid literal on one line.
        *(f"{_INDENT * 2}{statement!r}," for statement in step.statements),
        f"{_INDENT}],",
        f"{_INDENT}reverse_sql=migrations.RunSQL.noop,",
        "),",
    ]


def _block(lines: Sequence[str]) -> str:
    """Return a list literal of ``lines``, as the class body indents it."""
    if not lines:
        return "[]"
    body = "".join(f"{_INDENT * 2}{line}\n" for line in lines)
    return f"[\n{body}{_INDENT}]"
