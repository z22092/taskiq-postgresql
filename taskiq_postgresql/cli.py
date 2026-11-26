"""
``taskiq postgresql``: print, plan, check or apply the SQL of the package's tables.

taskiq imports this module and instantiates ``PostgresqlCMD`` on every
``taskiq`` call, ``taskiq worker`` included. So it imports only the standard
library and taskiq at module level: anything heavier would slow every taskiq
command, and anything that can fail to import, such as a driver, would break
them.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import re
import sys
from contextlib import asynccontextmanager, suppress
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Final,
    NamedTuple,
    Sequence,
)
from urllib.parse import unquote, urlsplit

from taskiq.abc.cmd import TaskiqCMD

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from types import ModuleType

    from taskiq_postgresql.core.stores.schema import SchemaStore
    from taskiq_postgresql.migrations import (
        Component,
        FormatOptions,
        MigrationFormat,
        MigrationPlan,
        PendingStep,
        SchemaProblem,
        TableSchema,
    )

_PROG = "taskiq postgresql"
_EXIT_STATUS = "Exit status: 0 success, 1 error or no component found, 2 usage error."
_PLAN_EXIT_STATUS = (
    "Exit status: 0 the plan is printed, or no step is pending; 1 error or no "
    "component found; 2 usage error; 4 a problem no step fixes needs a person "
    "(the plan, if any, is still printed)."
)
_CHECK_EXIT_STATUS = (
    "Exit status: 0 up to date, or only an index is missing; 1 error or no "
    "component found; 2 usage error; 3 a table or column is missing, or a column "
    "is still NOT NULL; 4 a problem no step fixes needs a person."
)
_BEHIND: Final = 3
_NEEDS_A_PERSON: Final = 4
# libpq's password option: a word of the key=value form, quoted or not, or a
# parameter of a URL's query.
_PASSWORD_OPTION: Final = re.compile(
    r"(?:^|[\s?&])password\s*=\s*(?:'((?:[^'\\]|\\.)*)'|([^\s&]+))",
)


class PostgresqlCMD(TaskiqCMD):
    """The ``taskiq postgresql`` command and its subcommands."""

    short_help = "Generate, plan, check or apply the taskiq-postgresql tables' SQL"

    def exec(self, args: Sequence[str]) -> int:
        """
        Run ``taskiq postgresql`` with the arguments after its name.

        Args:
            args: The command line after ``taskiq postgresql``.

        Returns:
            The exit status: 0 on success, 1 on an error or when a target
            holds no component, 2 on a usage error. ``check`` returns 3 when
            a table or a column is missing or a column is still ``NOT NULL``,
            and ``plan`` and ``check`` return 4 when a problem no migration
            step fixes needs a person.
        """
        try:
            return _run(list(args))
        except SystemExit as stop:
            # argparse exits on --help and on usage errors; taskiq expects the
            # status as the return value.
            return stop.code if isinstance(stop.code, int) else int(bool(stop.code))


class _Database(NamedTuple):
    """What ``plan`` and ``check`` read from one database."""

    name: str
    search_path: str
    schemas: list[TableSchema]
    plan: MigrationPlan

    @property
    def header(self) -> str:
        """The line that says which database a plan is for."""
        return (
            f"plan for database {_shown(self.name)} (search_path "
            f"{_shown(self.search_path)}): valid only for this database"
        )


class _ConnectionStringError(Exception):
    """An error raised while a driver read, or connected with, a connection string."""

    def __init__(self, error: Exception, dsn: str) -> None:
        # Only the scrubbed text is kept: an attribute holding the string
        # would reach an error tracker that captures locals or attributes.
        super().__init__(f"{type(error).__name__}: {_without_dsn(str(error), dsn)}")

    def __str__(self) -> str:
        """The driver's error, without the connection string or its password."""
        return str(self.args[0])


def _run(args: list[str]) -> int:
    from taskiq_postgresql import migrations  # noqa: PLC0415

    namespace = _parser(migrations).parse_args(args)
    # The subcommand's own parser, so a usage error shows its usage line.
    parser: argparse.ArgumentParser = namespace.parser
    try:
        return namespace.run(namespace, parser, migrations)
    except (
        migrations.MigrationFormatOptionError,
        migrations.UnknownMigrationFormatError,
        migrations.AmbiguousMigrationFormatError,
    ) as error:
        parser.error(str(error))
    # The application's import, a format's module, a third-party format and
    # the server can raise anything.
    except Exception as error:
        return _fail(error)


def _parser(migrations: ModuleType) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description="Create the tables of taskiq-postgresql, print the SQL that "
        "does, or compare a database with them.",
        epilog=f"{_EXIT_STATUS} plan and check also exit with 3 or 4, see their "
        "--help.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)
    commands: dict[argparse.ArgumentParser, Callable[..., int]] = {
        _generate_parser(subparsers, migrations): _generate,
        _migrate_parser(subparsers): _migrate,
        _plan_parser(subparsers, migrations): _plan,
        _check_parser(subparsers, migrations): _check,
    }
    for command, run in commands.items():
        command.set_defaults(run=run, parser=command)
    return parser


def _generate_parser(subparsers: Any, migrations: ModuleType) -> Any:
    generate = subparsers.add_parser(
        "generate",
        help="print the SQL script that creates the tables",
        description="Print the SQL script that creates the tables, and only the "
        "script, on stdout. Name the components with TARGETs, or one component "
        "with --component without importing the application.",
        epilog=_EXIT_STATUS,
    )
    _add_targets(generate, nargs="*")
    _add_component(generate, migrations)
    _add_output(generate, migrations)
    return generate


def _migrate_parser(subparsers: Any) -> Any:
    migrate = subparsers.add_parser(
        "migrate",
        help="create the tables now, as run_migrations=True does",
        description="Create the tables, and the indexes, with the code "
        "run_migrations=True runs at startup, whatever the components set; "
        "then close the connections. Nothing else of the components starts.",
        epilog=_EXIT_STATUS,
    )
    _add_targets(migrate, nargs="+")
    return migrate


def _plan_parser(subparsers: Any, migrations: ModuleType) -> Any:
    plan = subparsers.add_parser(
        "plan",
        help="print the SQL a database still needs",
        description="Read the catalog of the database, in one read-only query, "
        "and print on stdout the SQL of the migration steps it still needs, and "
        "only that SQL: nothing when every step has run. The plan is right for "
        "that database, at that moment. The locks the steps take, and the "
        "problems no step fixes, go to stderr.",
        epilog=_PLAN_EXIT_STATUS,
    )
    _add_targets(plan, nargs="*")
    _add_component(plan, migrations, needs_dsn=True)
    _add_dsn(plan)
    _add_output(plan, migrations)
    return plan


def _check_parser(subparsers: Any, migrations: ModuleType) -> Any:
    check = subparsers.add_parser(
        "check",
        help="tell whether a database has every table, column and index",
        description="Read the catalog of the database, in one read-only query, "
        "and report on stderr the migration steps it still needs and the "
        "problems no step fixes. It changes nothing: the exit status tells a "
        "deploy whether the workers can start.",
        epilog=_CHECK_EXIT_STATUS,
    )
    _add_targets(check, nargs="*")
    _add_component(check, migrations, needs_dsn=True)
    _add_dsn(check)
    check.add_argument(
        "--json",
        action="store_true",
        help="also print the report on stdout, as one JSON object",
    )
    return check


def _add_targets(parser: argparse.ArgumentParser, nargs: str) -> None:
    parser.add_argument(
        "targets",
        nargs=nargs,
        type=_target,
        metavar="TARGET",
        help="module:object of a broker, a TaskiqScheduler or a component, as "
        "for taskiq worker; a broker brings its result backend, a scheduler its "
        "broker and its sources",
    )
    parser.add_argument(
        "--app-dir",
        help="directory to add to sys.path before importing TARGETs",
    )


def _add_component(
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
    *,
    needs_dsn: bool = False,
) -> None:
    offline = parser.add_argument_group("without the application")
    offline.add_argument(
        "--component",
        choices=migrations.COMPONENT_KINDS,
        help="describe one component of this kind instead of importing TARGETs"
        + ("; needs --dsn" if needs_dsn else ""),
    )
    offline.add_argument("--table-name", help="its table name; default: its own")
    offline.add_argument(
        "--field-for-task-id",
        choices=migrations.TASK_ID_TYPES,
        help="its field_for_task_id (broker and result_backend); default: Uuid",
    )


def _add_dsn(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dsn",
        type=_dsn,
        metavar="DSN",
        help="connection string of the database to read; default: each "
        "component's own. Other users of the machine can read a command line: "
        "leave the password out, and give it with PGPASSWORD or a password "
        "file (~/.pgpass, or the one PGPASSFILE names), which asyncpg and "
        "psycopg read and psqlpy does not",
    )


def _add_output(parser: argparse.ArgumentParser, migrations: ModuleType) -> None:
    output = parser.add_argument_group("output")
    output.add_argument(
        "--format",
        default="sql",
        help="migration format, from the installed ones; default: sql",
    )
    output.add_argument(
        "--option",
        action="append",
        default=[],
        type=_option,
        metavar="KEY=VALUE",
        help="an option for the format; repeat for several",
    )
    output.add_argument(
        "--no-transaction",
        dest="transaction",
        action="store_false",
        help="leave out BEGIN and COMMIT, for a tool that opens its own "
        "transaction (Alembic, Django, Flyway)",
    )
    output.add_argument(
        "--no-lock",
        dest="lock",
        action="store_false",
        help="leave out the advisory lock that run_migrations=True also takes",
    )
    output.add_argument(
        "--lock-timeout",
        type=int,
        default=migrations.DEFAULT_LOCK_TIMEOUT,
        metavar="SECONDS",
        help="seconds a statement waits for a lock before the script fails "
        "(SET LOCAL lock_timeout); 0 leaves it out; default: "
        f"{migrations.DEFAULT_LOCK_TIMEOUT}",
    )


def _target(text: str) -> str:
    from taskiq_postgresql.migrations import parse_target  # noqa: PLC0415

    try:
        parse_target(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    return text


def _dsn(text: str) -> str:
    from taskiq_postgresql.utils.dsn import _is_blank  # noqa: PLC0415

    # libpq and asyncpg read an empty string as "use the local defaults", so
    # it would read a server nobody named. The value is never echoed: a
    # connection string can hold a password.
    if _is_blank(text):
        raise argparse.ArgumentTypeError(
            "the connection string is empty; give the database's own, or "
            "leave out --dsn to read each component's",
        )
    return text


def _option(text: str) -> tuple[str, str]:
    key, separator, value = text.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {text!r}")
    return key, value


def _check_selection(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    *,
    needs_dsn: bool = False,
) -> None:
    if bool(namespace.targets) == (namespace.component is not None):
        parser.error("give one or more TARGETs or --component, not both")
    if namespace.component is None and (
        namespace.table_name is not None or namespace.field_for_task_id is not None
    ):
        parser.error("--table-name and --field-for-task-id need --component")
    if needs_dsn and namespace.component is not None and namespace.dsn is None:
        parser.error(
            "--component needs --dsn: without the application there is no "
            "connection string",
        )


def _format_options(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> FormatOptions:
    try:
        return migrations.FormatOptions(
            transaction=namespace.transaction,
            lock=namespace.lock,
            lock_timeout=namespace.lock_timeout,
            extra=dict(namespace.option),
        )
    except ValueError as error:
        parser.error(f"--lock-timeout: {error}")


def _generate(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    _check_selection(namespace, parser)
    options = _format_options(namespace, parser, migrations)
    render = migrations.load_format(namespace.format)
    sys.stdout.write(render(_schemas(namespace, parser, migrations), options))
    return 0


def _schemas(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> list[TableSchema]:
    if namespace.component is None:
        return migrations.collect_schemas(
            *namespace.targets,
            app_dir=namespace.app_dir,
        )
    try:
        return [
            migrations.table_schema(
                namespace.component,
                table_name=namespace.table_name,
                field_for_task_id=namespace.field_for_task_id,
            ),
        ]
    except ValueError as error:
        parser.error(str(error))


def _plan(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    _check_selection(namespace, parser, needs_dsn=True)
    options = _format_options(namespace, parser, migrations)
    render = migrations.load_format(namespace.format)
    _check_format_options(render, options, migrations)
    connections = _connections(namespace, parser, migrations)
    if len(connections) > 1:
        raise ValueError(
            f"the components use {len(connections)} connection strings, and "
            "a plan is for one database: pass --dsn, or plan one component "
            "at a time with --component",
        )
    (database,) = asyncio.run(_read(connections, migrations))
    plan = database.plan
    _check_downgrade(render, options, plan, migrations)
    for note in plan.notes:
        _say(f"note: {note}")
    _say_problems(plan.problems)
    if plan.is_current:
        _say_up_to_date(database.schemas)
    else:
        sys.stdout.write(_plan_script(database, render, options, migrations))
    return _NEEDS_A_PERSON if plan.problems else 0


def _check_format_options(
    render: MigrationFormat,
    options: FormatOptions,
    migrations: ModuleType,
) -> None:
    # A format checks its options as it renders, and a plan with nothing
    # pending renders nothing: render no table at all first, before anything
    # connects. Any other error is the format's answer to an empty list; the
    # real render reports its own.
    try:
        render([], options)
    except migrations.MigrationFormatOptionError:
        raise
    except Exception:
        return


def _check_downgrade(
    render: MigrationFormat,
    options: FormatOptions,
    plan: MigrationPlan,
    migrations: ModuleType,
) -> None:
    # The alembic format's downgrade=drop drops every table it is given.
    if options.extra.get("downgrade") != "drop" or _alembic_format(render) is None:
        return
    kept = sorted({item.table for item in plan.pending if not item.table_missing})
    if kept:
        raise migrations.MigrationFormatOptionError(
            f"--option downgrade=drop: the revision would drop {', '.join(kept)}, "
            "which it does not create, since only later steps are pending; "
            "leave downgrade at none",
        )


def _plan_script(
    database: _Database,
    render: MigrationFormat,
    options: FormatOptions,
    migrations: ModuleType,
) -> str:
    plan = database.plan
    if render is migrations.sql_format:
        # A comment that keeps the script from being run on another database
        # by mistake.
        return plan.render_sql(
            transaction=options.transaction,
            lock=options.lock,
            lock_timeout=options.lock_timeout,
            comments=[database.header],
        )
    _say(f"note: {database.header}")
    schemas = plan.pending_schemas(database.schemas)
    commented = _commented(render)
    if commented is not None:
        # The file names the database too: the plan is wrong for another
        # database, and an alembic revision's downgrade=drop there would
        # drop a table the revision did not create.
        return commented(schemas, options, comments=[database.header])
    return render(schemas, options)


def _commented(render: MigrationFormat) -> Callable[..., str] | None:
    """Return ``render`` if it takes a keyword-only ``comments``, else ``None``."""
    # A capability the format declares, so any package's format can take the
    # header, whatever its name or module.
    try:
        parameters = inspect.signature(render).parameters
    except (TypeError, ValueError):
        # A callable without a signature Python can read takes no keyword.
        return None
    comments = parameters.get("comments")
    if comments is None or comments.kind is not inspect.Parameter.KEYWORD_ONLY:
        return None
    return render


def _alembic_format(render: MigrationFormat) -> Callable[..., str] | None:
    """Return this package's alembic format if ``render`` is it, else ``None``."""
    # By identity, not by the name after --format: another package can
    # register the same callable under a name of its own.
    try:
        from taskiq_postgresql.contrib.alembic import (  # noqa: PLC0415
            alembic_format,
        )
    except ImportError:
        # Without Alembic installed, no format can be this one.
        return None
    return alembic_format if render is alembic_format else None


def _check(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    _check_selection(namespace, parser, needs_dsn=True)
    databases = asyncio.run(
        _read(_connections(namespace, parser, migrations), migrations),
    )
    pending = [item for database in databases for item in database.plan.pending]
    problems = [problem for database in databases for problem in database.plan.problems]
    for item in pending:
        _say(
            f"pending: {item.table} step {item.step.number} ({item.severity}): "
            f"{item.describe()}",
        )
    _say_problems(problems)
    if not pending:
        _say_up_to_date(
            [schema for database in databases for schema in database.schemas],
        )
    if namespace.json:
        sys.stdout.write(json.dumps(_json_report(pending, problems), indent=2) + "\n")
    if problems:
        return _NEEDS_A_PERSON
    return _BEHIND if any(item.severity == "required" for item in pending) else 0


def _connections(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> dict[str, tuple[SchemaStore | None, list[TableSchema]]]:
    """Group the tables by the connection string their catalog is read with."""
    from taskiq_postgresql.core.stores.schema import schema_store  # noqa: PLC0415

    if namespace.component is not None:
        schemas = _schemas(namespace, parser, migrations)
        return {namespace.dsn: (None, schemas)}
    grouped: dict[str, tuple[SchemaStore | None, list[TableSchema]]] = {}
    for component in migrations.collect_components(
        *namespace.targets,
        app_dir=namespace.app_dir,
    ):
        dsn = namespace.dsn or component.dsn
        _, schemas = grouped.setdefault(dsn, (schema_store(component), []))
        schemas.append(migrations.table_schema(component))
    return grouped


async def _read(
    connections: dict[str, tuple[SchemaStore | None, list[TableSchema]]],
    migrations: ModuleType,
) -> list[_Database]:
    return [
        await _read_database(dsn, store, schemas, migrations)
        for dsn, (store, schemas) in connections.items()
    ]


async def _read_database(
    dsn: str,
    store: SchemaStore | None,
    schemas: list[TableSchema],
    migrations: ModuleType,
) -> _Database:
    # One read: the catalog, and what the plan's header names.
    from taskiq_postgresql.core.stores.schema import read_database  # noqa: PLC0415

    async with _connecting(dsn):
        row = await read_database(dsn, schemas, store)
        found: dict[str, Any] = row or {}
        state = migrations.catalog_state(found.get("catalog"))
    return _Database(
        name=str(found["database"]),
        search_path=str(found["search_path"]),
        schemas=schemas,
        plan=migrations.plan_migration(schemas, state),
    )


def _json_report(
    pending: Sequence[PendingStep],
    problems: Sequence[SchemaProblem],
) -> dict[str, Any]:
    return {
        "current": not pending,
        "pending": [
            {
                "table": item.table,
                "kind": item.kind,
                "step": item.step.number,
                "severity": item.severity,
                "missing": {
                    "table": item.table_missing,
                    "columns": list(item.missing_columns),
                    "indexes": list(item.missing_indexes),
                },
                "not_null_columns": list(item.not_null_columns),
            }
            for item in pending
        ],
        "problems": [
            {
                "table": problem.table,
                "code": problem.code,
                "severity": problem.severity,
                "detail": problem.detail,
            }
            for problem in problems
        ],
        "advisory_only": bool(pending)
        and all(item.severity == "advisory" for item in pending),
    }


def _say_problems(problems: Sequence[SchemaProblem]) -> None:
    for problem in problems:
        _say(f"problem: {problem.code}: {problem.detail}")


def _say_up_to_date(schemas: Sequence[TableSchema]) -> None:
    _say(f"tables up to date: {', '.join(schema.name for schema in schemas)}")


def _shown(text: str) -> str:
    from taskiq_postgresql.migrations import check_comment  # noqa: PLC0415

    # A name from the server goes into a comment line of the script, which a
    # line break would end, and of a migration file, whose first two lines
    # Python reads an encoding declaration from (PEP 263).
    shown = text if text.isprintable() else repr(text)
    try:
        check_comment(shown)
    except ValueError:
        # Without its ":" or "=", "coding: utf-7" declares nothing.
        return repr(text).replace(":", "\\x3a").replace("=", "\\x3d")
    return shown


def _say(text: str) -> None:
    sys.stderr.write(f"{_PROG}: {text}\n")


def _migrate(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    components = migrations.collect_components(
        *namespace.targets,
        app_dir=namespace.app_dir,
    )
    asyncio.run(_apply(components))
    from taskiq_postgresql.core.stores.schema import schema_store  # noqa: PLC0415

    names = ", ".join(
        schema_store(component).declaration.table.configured for component in components
    )
    sys.stderr.write(f"{_PROG}: tables ready: {names}\n")
    return 0


async def _apply(components: Sequence[Component]) -> None:
    from taskiq_postgresql.core.stores.schema import schema_store  # noqa: PLC0415

    for component in components:
        dsn = component.dsn
        async with _connecting(dsn):
            await schema_store(component).migrate(dsn)


@asynccontextmanager
async def _connecting(dsn: str) -> AsyncGenerator[None, None]:
    """Tag an error raised in the block with ``dsn``, which ``_fail`` leaves out."""
    try:
        yield
    except Exception as error:
        raise _ConnectionStringError(error, dsn) from error


def _without_dsn(text: str, dsn: str) -> str:
    """Return ``text`` without ``dsn`` and without the password in it."""
    if dsn:
        text = text.replace(dsn, "<connection string>")
    # The longest first, so a password that holds another goes whole.
    for password in sorted(_passwords(dsn), key=len, reverse=True):
        text = text.replace(password, "***")
    return text


def _passwords(dsn: str) -> set[str]:
    """Return the password in ``dsn``, as written and percent-decoded."""
    written: list[str | None] = []
    with suppress(ValueError):
        # urllib refuses a URL with a malformed IPv6 address.
        written.append(urlsplit(dsn).password)
    for quoted, bare in _PASSWORD_OPTION.findall(dsn):
        written.append(re.sub(r"\\(.)", r"\1", quoted) or bare)
    return {
        form
        for password in written
        if password
        for form in (password, unquote(password))
    }


def _fail(error: Exception) -> int:
    # The message, not a traceback: these are errors in the user's setup. A
    # driver's error comes as a _ConnectionStringError, whose text leaves out the
    # connection string and its password.
    text = (
        str(error)
        if isinstance(error, _ConnectionStringError)
        else f"{type(error).__name__}: {error}"
    )
    sys.stderr.write(f"{_PROG}: error: {text}\n")
    return 1
