"""
``taskiq postgresql``: print or apply the SQL that creates the package's tables.

taskiq imports this module and instantiates ``PostgresqlCMD`` on every
``taskiq`` call, ``taskiq worker`` included. So it imports only the standard
library and taskiq at module level: anything heavier would slow every taskiq
command, and anything that can fail to import, such as a driver, would break
them.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import TYPE_CHECKING, Sequence

from taskiq.abc.cmd import TaskiqCMD

if TYPE_CHECKING:
    from types import ModuleType

    from taskiq_postgresql.migrations import Component, TableSchema

_PROG = "taskiq postgresql"
_EXIT_STATUS = "Exit status: 0 success, 1 error or no component found, 2 usage error."


class PostgresqlCMD(TaskiqCMD):
    """The ``taskiq postgresql`` command: ``generate`` and ``migrate``."""

    short_help = "Generate or apply the SQL for the taskiq-postgresql tables"

    def exec(self, args: Sequence[str]) -> int:
        """
        Run ``taskiq postgresql`` with the arguments after its name.

        Args:
            args: The command line after ``taskiq postgresql``.

        Returns:
            The exit status: 0 on success, 1 on an error or when a target
            holds no component, 2 on a usage error.
        """
        try:
            return _run(list(args))
        except SystemExit as stop:
            # argparse exits on --help and on usage errors; taskiq expects the
            # status as the return value.
            return stop.code if isinstance(stop.code, int) else int(bool(stop.code))


def _run(args: list[str]) -> int:
    from taskiq_postgresql import migrations  # noqa: PLC0415

    parser, generate = _parser(migrations)
    namespace = parser.parse_args(args)
    if namespace.command == "generate":
        return _generate(namespace, generate, migrations)
    return _migrate(namespace, migrations)


def _parser(
    migrations: ModuleType,
) -> tuple[argparse.ArgumentParser, argparse.ArgumentParser]:
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description="Create the tables of taskiq-postgresql, or print the SQL "
        "that does.",
        epilog=_EXIT_STATUS,
    )
    commands = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)
    generate = commands.add_parser(
        "generate",
        help="print the SQL script that creates the tables",
        description="Print the SQL script that creates the tables, and only the "
        "script, on stdout. Name the components with TARGETs, or one component "
        "with --component without importing the application.",
        epilog=_EXIT_STATUS,
    )
    _add_targets(generate, nargs="*")
    offline = generate.add_argument_group("without the application")
    offline.add_argument(
        "--component",
        choices=migrations.COMPONENT_KINDS,
        help="describe one component of this kind instead of importing TARGETs",
    )
    offline.add_argument("--table-name", help="its table name; default: its own")
    offline.add_argument(
        "--field-for-task-id",
        choices=migrations.TASK_ID_TYPES,
        help="its field_for_task_id (broker and result_backend); default: Uuid",
    )
    output = generate.add_argument_group("output")
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
    migrate = commands.add_parser(
        "migrate",
        help="create the tables now, as run_migrations=True does",
        description="Create the tables, and the indexes, with the code "
        "run_migrations=True runs at startup, whatever the components set; "
        "then close the connections. Nothing else of the components starts.",
        epilog=_EXIT_STATUS,
    )
    _add_targets(migrate, nargs="+")
    return parser, generate


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


def _generate(
    namespace: argparse.Namespace,
    parser: argparse.ArgumentParser,
    migrations: ModuleType,
) -> int:
    if bool(namespace.targets) == (namespace.component is not None):
        parser.error("give one or more TARGETs or --component, not both")
    if namespace.component is None and (
        namespace.table_name is not None or namespace.field_for_task_id is not None
    ):
        parser.error("--table-name and --field-for-task-id need --component")
    try:
        options = migrations.FormatOptions(
            transaction=namespace.transaction,
            lock=namespace.lock,
            lock_timeout=namespace.lock_timeout,
            extra=dict(namespace.option),
        )
    except ValueError as error:
        parser.error(f"--lock-timeout: {error}")
    try:
        render = migrations.load_format(namespace.format)
    except migrations.UnknownMigrationFormatError as error:
        parser.error(str(error))
    except ImportError as error:
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
