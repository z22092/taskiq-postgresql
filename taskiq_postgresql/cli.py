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
import dataclasses
import json
import sys
from typing import TYPE_CHECKING, Any, Final, Literal, NamedTuple, Sequence

from taskiq.abc.cmd import TaskiqCMD

if TYPE_CHECKING:
    from types import ModuleType

    from taskiq_postgresql.abc.driver import QueryDriver
    from taskiq_postgresql.migrations import (
        Component,
        FormatOptions,
        MigrationFormat,
        MigrationPlan,
        MigrationStep,
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
    "component found; 2 usage error; 3 a table or column is missing; 4 a "
    "problem no step fixes needs a person."
)
_BEHIND: Final = 3
_NEEDS_A_PERSON: Final = 4

_DriverName = Literal["asyncpg", "psqlpy", "psycopg"]
# The driver --component reads with, the first one installed: without the
# application, no component says which.
_DRIVERS: Final[tuple[_DriverName, ...]] = ("asyncpg", "psycopg", "psqlpy")


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
            a table or a column is missing, and ``plan`` and ``check`` return 4
            when a problem no migration step fixes needs a person.
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


def _run(args: list[str]) -> int:
    from taskiq_postgresql import migrations  # noqa: PLC0415

    parser, commands = _parser(migrations)
    namespace = parser.parse_args(args)
    command = commands[namespace.command]
    if namespace.command == "generate":
        return _generate(namespace, command, migrations)
    if namespace.command == "plan":
        return _plan(namespace, command, migrations)
    if namespace.command == "check":
        return _check(namespace, command, migrations)
    return _migrate(namespace, migrations)


def _parser(
    migrations: ModuleType,
) -> tuple[argparse.ArgumentParser, dict[str, argparse.ArgumentParser]]:
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description="Create the tables of taskiq-postgresql, print the SQL that "
        "does, or compare a database with them.",
        epilog=f"{_EXIT_STATUS} plan and check also exit with 3 or 4, see their "
        "--help.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)
    commands = {
        "generate": _generate_parser(subparsers, migrations),
        "migrate": _migrate_parser(subparsers),
        "plan": _plan_parser(subparsers, migrations),
        "check": _check_parser(subparsers, migrations),
    }
    return parser, commands


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
        metavar="DSN",
        help="connection string of the database to read; default: each component's own",
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
    module, _, attribute = text.partition(":")
    if not module or not attribute or ":" in attribute:
        raise argparse.ArgumentTypeError(f"{text!r} is not a 'module:object' path")
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


def _load_format(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> MigrationFormat:
    try:
        return migrations.load_format(namespace.format)
    except (
        migrations.UnknownMigrationFormatError,
        migrations.AmbiguousMigrationFormatError,
    ) as error:
        parser.error(str(error))


def _generate(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    _check_selection(namespace, parser)
    options = _format_options(namespace, parser, migrations)
    try:
        render = _load_format(namespace, parser, migrations)
    except Exception as error:  # a format's module and entry point are not ours
        return _fail(error)
    try:
        schemas = _schemas(namespace, parser, migrations)
    except Exception as error:  # the application's own import errors too
        return _fail(error)
    try:
        script = render(schemas, options)
    except migrations.MigrationFormatOptionError as error:
        parser.error(str(error))
    except Exception as error:  # a third-party format can raise anything
        return _fail(error)
    sys.stdout.write(script)
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
    try:
        render = _load_format(namespace, parser, migrations)
    except Exception as error:  # a format's module and entry point are not ours
        return _fail(error)
    if render is migrations.sql_format and options.extra:
        # The plan renders the sql format itself, so it refuses what it would.
        parser.error(
            f"the sql format takes no --option; got {', '.join(sorted(options.extra))}",
        )
    try:
        connections = _connections(namespace, parser, migrations)
        if len(connections) > 1:
            raise ValueError(
                f"the components use {len(connections)} connection strings, and "
                "a plan is for one database: pass --dsn, or plan one component "
                "at a time with --component",
            )
        (database,) = asyncio.run(_read(connections, migrations))
    except Exception as error:  # the application's and the server's errors
        return _fail(error)
    plan = database.plan
    for note in plan.notes:
        _say(f"note: {note}")
    _say_problems(plan.problems)
    if plan.is_current:
        _say_up_to_date(database.schemas)
    else:
        try:
            script = _plan_script(database, render, options, migrations)
        except migrations.MigrationFormatOptionError as error:
            parser.error(str(error))
        except Exception as error:  # a third-party format can raise anything
            return _fail(error)
        sys.stdout.write(script)
    return _NEEDS_A_PERSON if plan.problems else 0


def _plan_script(
    database: _Database,
    render: MigrationFormat,
    options: FormatOptions,
    migrations: ModuleType,
) -> str:
    plan = database.plan
    if render is migrations.sql_format:
        first, rest = plan.render_sql(
            transaction=options.transaction,
            lock=options.lock,
            lock_timeout=options.lock_timeout,
        ).split("\n", 1)
        # After the version line, before the steps: a comment that keeps the
        # script from being run on another database by mistake.
        return f"{first}\n-- {database.header}\n{rest}"
    _say(f"note: {database.header}")
    # Another format gets the tables with only their pending steps.
    steps: dict[str, list[MigrationStep]] = {}
    for item in plan.pending:
        steps.setdefault(item.table, []).append(item.step)
    return render(
        [
            dataclasses.replace(schema, steps=tuple(steps[schema.name]))
            for schema in database.schemas
            if schema.name in steps
        ],
        options,
    )


def _check(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    _check_selection(namespace, parser, needs_dsn=True)
    try:
        databases = asyncio.run(
            _read(_connections(namespace, parser, migrations), migrations),
        )
    except Exception as error:  # the application's and the server's errors
        return _fail(error)
    pending = [item for database in databases for item in database.plan.pending]
    problems = [problem for database in databases for problem in database.plan.problems]
    for item in pending:
        _say(
            f"pending: {item.table} step {item.step.number} ({item.severity}): "
            f"{_missing(item)}",
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
) -> dict[str, tuple[QueryDriver, list[TableSchema]]]:
    """Group the tables by the connection string their catalog is read with."""
    if namespace.component is None:
        components = migrations.collect_components(
            *namespace.targets,
            app_dir=namespace.app_dir,
        )
        tables = [
            (component, migrations.table_schema(component)) for component in components
        ]
    else:
        tables = [
            (_component_of_kind(namespace.component, namespace.dsn), schema)
            for schema in _schemas(namespace, parser, migrations)
        ]
    grouped: dict[str, tuple[QueryDriver, list[TableSchema]]] = {}
    for component, schema in tables:
        dsn = namespace.dsn or component.dsn
        _, schemas = grouped.setdefault(dsn, (component.driver, []))
        schemas.append(schema)
    return grouped


def _component_of_kind(kind: str, dsn: str) -> Component:
    # Its table options do not matter: it only lends its driver.
    from importlib.util import find_spec  # noqa: PLC0415

    from taskiq_postgresql.broker import PostgresqlBroker  # noqa: PLC0415
    from taskiq_postgresql.result_backend import (  # noqa: PLC0415
        PostgresqlResultBackend,
    )
    from taskiq_postgresql.scheduler_source import (  # noqa: PLC0415
        PostgresqlSchedulerSource,
    )

    driver = next((name for name in _DRIVERS if find_spec(name)), _DRIVERS[0])
    if kind == "broker":
        return PostgresqlBroker(dsn, driver=driver)
    if kind == "result_backend":
        return PostgresqlResultBackend(dsn, driver=driver)
    return PostgresqlSchedulerSource(dsn, driver=driver)


async def _read(
    connections: dict[str, tuple[QueryDriver, list[TableSchema]]],
    migrations: ModuleType,
) -> list[_Database]:
    return [
        await _read_database(dsn, driver, schemas, migrations)
        for dsn, (driver, schemas) in connections.items()
    ]


async def _read_database(
    dsn: str,
    driver: QueryDriver,
    schemas: list[TableSchema],
    migrations: ModuleType,
) -> _Database:
    # One SELECT: the catalog, and what the plan's header names.
    query = (
        "SELECT current_database()::text AS database,"  # noqa: S608
        " current_schemas(false)::text AS search_path, catalog FROM ("
        + migrations.catalog_query(schemas)
        + ") AS state"
    )
    driver.connection_string = dsn
    try:
        row = await driver.fetch_row(query)
    finally:
        if getattr(driver, "pool", None) is not None:
            await driver.on_shutdown()
    found = row or {}
    state = migrations.CatalogState.from_json(found.get("catalog") or "")
    return _Database(
        name=str(found.get("database")),
        search_path=str(found.get("search_path")),
        schemas=schemas,
        plan=migrations.plan_migration(schemas, state),
    )


def _missing(item: PendingStep) -> str:
    if item.table_missing:
        return "the table is missing"
    found = []
    if item.missing_columns:
        found.append(f"missing columns: {', '.join(item.missing_columns)}")
    if item.missing_indexes:
        found.append(f"missing indexes: {', '.join(item.missing_indexes)}")
    return "; ".join(found)


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
    # A name from the server goes into a comment line of the script, which a
    # line break would end.
    return text if text.isprintable() else repr(text)


def _say(text: str) -> None:
    sys.stderr.write(f"{_PROG}: {text}\n")


def _migrate(namespace: argparse.Namespace, migrations: ModuleType) -> int:
    try:
        components = migrations.collect_components(
            *namespace.targets,
            app_dir=namespace.app_dir,
        )
        asyncio.run(_apply(components))
    except Exception as error:  # the application's and the server's errors
        return _fail(error)
    names = ", ".join(component.driver.table_name for component in components)
    sys.stderr.write(f"{_PROG}: tables ready: {names}\n")
    return 0


async def _apply(components: Sequence[Component]) -> None:
    for component in components:
        driver = component.driver
        driver.connection_string = component.dsn
        run_migrations = driver.run_migrations
        # The startup path runs its DDL only with the flag set; migrate runs it
        # whatever the component says, then leaves the flag as it found it.
        driver.run_migrations = True
        try:
            await driver.on_startup()
        finally:
            driver.run_migrations = run_migrations
            if getattr(driver, "pool", None) is not None:
                await driver.on_shutdown()


def _fail(error: Exception) -> int:
    # The message, not a traceback: these are errors in the user's setup.
    sys.stderr.write(f"{_PROG}: error: {type(error).__name__}: {error}\n")
    return 1
