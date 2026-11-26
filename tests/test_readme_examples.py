"""
Check the README against the code: run its examples, compare its tables.

The README is the PyPI long description, so an example that does not run, or a
table that disagrees with the signatures, is a bug a user hits first. The
quickstart scripts (marked ``# app.py``, ``# kick.py`` and ``# schedule.py`` on
their first line) run as separate processes, with the README's own ``taskiq``
commands, once per installed driver. Every other Python block is executed as
written, except the Integrations examples, which run in a directory that holds
the quickstart's ``app.py`` and are then put to work: their metadata creates
the tables, autogenerate compares them, the revisions run. The shell lines of
the Migrations and Integrations sections run as written too, with the
quickstart's ``app.py``.

Every test that touches a database does so in a database of its own, created
and dropped here, because the README examples use the default table names and
must never run in tables the test did not create.
"""

import ast
import asyncio
import contextlib
import inspect
import io
import os
import queue
import random
import re
import secrets
import signal
import ssl
import string
import subprocess
import sys
import textwrap
import threading
import time
import types
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import alembic as alembic_package
import asyncpg
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlmodel import SQLModel
from taskiq import BrokerMessage, ScheduledTask, TaskiqResult
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers import JSONSerializer, MSGPackSerializer

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend, migrations
from taskiq_postgresql.cli import PostgresqlCMD
from taskiq_postgresql.contrib.alembic import alembic_format
from taskiq_postgresql.contrib.sqlalchemy import add_taskiq_tables
from taskiq_postgresql.migrations import (
    FormatOptions,
    migration_statements,
    render_sql,
    table_schema,
)
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import (
    address_of,
    assert_exited_cleanly,
    create_database,
    drop_database,
    in_database,
    new_database,
    run_script,
)
from tests.test_contrib_alembic import (
    autogenerate,
    bounded,
    offline_sql,
    run_upgrade,
)
from tests.test_contrib_sqlalchemy import sqlalchemy_url
from tests.test_migrations import scram_verifier

README = Path(__file__).resolve().parent.parent / "README.md"
CHANGELOG = README.with_name("CHANGELOG.md")
SCRIPTS = ("app.py", "kick.py", "schedule.py")
CA_PLACEHOLDER = "/path/to/ca.pem"
SCHEDULER_READY = "Starting scheduler"
STARTUP_TIMEOUT = 30
SCRIPT_TIMEOUT = 60
SHUTDOWN_TIMEOUT = 5
SCHEDULER_ALIVE_SECONDS = 2
# The README sections the migration tests read, by the slug of their heading.
SCHEMA_SECTION = "database-schema"
GENERATE_SECTION = "generate-the-sql"
MIGRATE_SECTION = "apply-it-with-migrate"
TOOLS_SECTION = "alembic-django-and-flyway"
FORMAT_SECTION = "writing-a-migration-format"
INTEGRATIONS_SECTION = "integrations"
SQLALCHEMY_SECTION = "sqlalchemy-and-sqlmodel"
ALEMBIC_SECTION = "alembic"
DJANGO_SECTION = "django"
DEFAULT_TABLES = {"taskiq_messages", "taskiq_results", "taskiq_schedulers"}
README_ROLE = "taskiq_worker"

# One pass over headings and fences, so a block knows its section and a "# ..."
# comment inside a fence is never read as a heading.
_TOKEN = re.compile(
    r"^#{2,3} (?P<heading>[^\n]+)$|^```(?P<language>\w*)\n(?P<code>.*?)^```$",
    re.DOTALL | re.MULTILINE,
)


@dataclass(frozen=True)
class Block:
    """A fenced code block of the README and the section it sits in."""

    section: str
    language: str
    source: str
    id: str


def _slug(heading: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")


def _blocks(language: str) -> list[Block]:
    """Return the README's fenced blocks of one language, with readable ids."""
    section = ""
    found: list[tuple[str, str]] = []
    for token in _TOKEN.finditer(README.read_text(encoding="utf-8")):
        if token["heading"]:
            section = _slug(token["heading"])
        elif token["language"] == language:
            found.append((section, token["code"]))
    per_section = Counter(section for section, _ in found)
    position: Counter[str] = Counter()
    blocks = []
    for section, source in found:
        position[section] += 1
        first_line = source.splitlines()[0]
        if first_line.startswith("# ") and first_line.removeprefix("# ") in SCRIPTS:
            block_id = first_line.removeprefix("# ")
        elif per_section[section] == 1:
            block_id = section
        else:
            block_id = f"{section}-{position[section]}"
        blocks.append(Block(section, language, source, block_id))
    return blocks


def _scripts() -> dict[str, str]:
    """Return the quickstart scripts of the README, keyed by file name."""
    return {
        block.id: block.source for block in _blocks("python") if block.id in SCRIPTS
    }


def _other_blocks() -> list[Block]:
    # The Integrations examples import the quickstart's app, which only their
    # own tests provide.
    return [
        block
        for block in _blocks("python")
        if block.id not in SCRIPTS
        and block.section not in {SQLALCHEMY_SECTION, ALEMBIC_SECTION, DJANGO_SECTION}
    ]


def _commands(section: str) -> list[list[str]]:
    """Return the ``taskiq`` commands the shell blocks of ``section`` show."""
    return [
        line.split()
        for block in _blocks("bash")
        if block.section == section
        for line in block.source.splitlines()
        if line.startswith("taskiq ")
    ]


def _shell_lines(*sections: str) -> list[str]:
    """Return every non-empty line of the shell blocks in ``sections``."""
    return [
        line
        for block in _blocks("bash")
        if block.section in sections
        for line in block.source.splitlines()
        if line.strip()
    ]


def _block(language: str, section: str) -> Block:
    (block,) = [block for block in _blocks(language) if block.section == section]
    return block


def _readme_text() -> str:
    """Return the README with every run of whitespace collapsed to one space."""
    return " ".join(README.read_text(encoding="utf-8").split())


def _environment(dsn: str) -> dict[str, str]:
    return {**os.environ, "POSTGRESQL_URL": dsn, "PYTHONUNBUFFERED": "1"}


def _taskiq(command: list[str]) -> list[str]:
    """Run the README's ``taskiq ...`` command with this interpreter's taskiq."""
    assert command[0] == "taskiq"
    return [sys.executable, "-m", "taskiq", *command[1:]]


def _pump(stream: Any, lines: "queue.Queue[str]") -> None:
    for line in stream:
        lines.put(line)


def _wait_for(lines: "queue.Queue[str]", needle: str, seen: list[str]) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        try:
            line = lines.get(timeout=0.5)
        except queue.Empty:
            continue
        seen.append(line)
        if needle in line:
            return
    pytest.fail(f"never saw {needle!r}; output so far:\n{''.join(seen)}")


@pytest.fixture
def scratch_dsn(postgresql_dsn: str) -> Iterator[str]:
    """
    DSN of a database created for one test and dropped after it.

    The README examples use the default table names and the default channel, so
    they run in a database nobody else owns instead of in the session database.
    """
    name = "readme_" + "".join(random.choices(string.ascii_lowercase, k=10))
    asyncio.run(create_database(postgresql_dsn, name))
    yield in_database(postgresql_dsn, name)
    asyncio.run(drop_database(postgresql_dsn, name))


@pytest.fixture(scope="session")
def ca_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    A CA certificate the TLS examples can load.

    Generated at run time (the key is thrown away with the directory), because
    ``ssl.create_default_context(cafile=...)`` needs a real PEM file.
    """
    directory = tmp_path_factory.mktemp("readme-ca")
    certificate = directory / "ca.pem"
    subprocess.run(  # noqa: S603
        [  # noqa: S607
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            "/CN=readme-test-ca",
            "-days",
            "1",
            "-keyout",
            str(directory / "ca.key"),
            "-out",
            str(certificate),
        ],
        check=True,
        capture_output=True,
    )
    return certificate


def _run_block(block: Block, ca_file: Path) -> dict[str, Any]:
    namespace: dict[str, Any] = {"__name__": "readme_block"}
    source = block.source.replace(CA_PLACEHOLDER, str(ca_file))
    exec(compile(source, f"README.md[{block.id}]", "exec"), namespace)  # noqa: S102
    return namespace


def test_when_readme_is_read__then_it_has_the_quickstart_scripts() -> None:
    assert sorted(_scripts()) == sorted(SCRIPTS)
    assert _other_blocks(), "the configuration examples are part of the README"


def test_when_readme_has_relative_links__then_none_is_allowed() -> None:
    """PyPI renders the README outside the repository, so links must be absolute."""
    text = README.read_text(encoding="utf-8")
    relative = re.findall(r"\]\((?!https?://|#|mailto:)[^)]*\)", text)
    assert relative == []


@pytest.mark.parametrize(
    "block",
    _other_blocks(),
    ids=[block.id for block in _other_blocks()],
)
def test_when_a_readme_block_is_executed__then_it_runs(
    block: Block,
    scratch_dsn: str,
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    _run_block(block, ca_file)


def test_when_the_driver_options_block_runs__then_the_options_connect(
    scratch_dsn: str,
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pool and connection options shown in the README are accepted."""
    (block,) = [b for b in _other_blocks() if b.section == "driver-options"]
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    namespace = _run_block(block, ca_file)

    async def connect() -> None:
        backend, broker = namespace["result_backend"], namespace["broker"]
        async with backend.driver:
            assert backend.driver.pool is not None
        await backend.driver.on_shutdown()
        await broker.listen_driver.on_startup()
        await broker.listen_driver.on_shutdown()

    asyncio.run(connect())


# What each driver receives from the TLS blocks: asyncpg and psqlpy take
# keyword arguments, psycopg reads everything from the DSN.
TLS_OPTIONS = {
    "AsyncpgDriver": {"ssl"},
    "PsycopgDriver": set(),
    "PsqlpyDriver": {"ssl_mode", "ca_file"},
}


@pytest.mark.parametrize(
    "block",
    [block for block in _other_blocks() if block.section == "tls"],
    ids=[block.id for block in _other_blocks() if block.section == "tls"],
)
def test_when_a_tls_block_runs__then_all_three_components_hold_its_options(
    block: Block,
    scratch_dsn: str,
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    namespace = _run_block(block, ca_file)
    broker = namespace["broker"]
    drivers = [
        broker.driver,
        broker.listen_driver,
        namespace["result_backend"].driver,
        namespace["scheduler_source"].driver,
    ]
    expected = TLS_OPTIONS[type(broker.driver).__name__]
    for driver in drivers:
        assert set(driver.connection_kwargs) == expected
    if "ssl" in expected:
        assert isinstance(broker.driver.connection_kwargs["ssl"], ssl.SSLContext)
        assert broker.driver.connection_kwargs["ssl"].check_hostname


@pytest.mark.parametrize(
    "serializer",
    [JSONSerializer, MSGPackSerializer],
    ids=["JSONSerializer", "MSGPackSerializer"],
)
@pytest.mark.anyio
async def test_when_a_json_family_serializer_stores_a_result__then_it_raises(
    serializer: type[TaskiqSerializer],
) -> None:
    """The README says these serializers cannot replace pickle for results."""
    assert "`JSONSerializer` and `MSGPackSerializer` raise `TypeError`" in (
        _readme_text()
    )
    backend: PostgresqlResultBackend[int] = PostgresqlResultBackend(
        dsn="postgresql://localhost/unused",
        serializer=serializer(),
    )
    result: TaskiqResult[int] = TaskiqResult(
        is_err=False,
        log=None,
        return_value=3,
        execution_time=0.1,
    )
    with pytest.raises(TypeError):
        await backend.set_result("task", result)


def _readme_table(heading: str) -> list[tuple[str, Any]]:
    """Return the (argument, default) rows of the table under ``heading``."""
    text = README.read_text(encoding="utf-8")
    section = text.split(f"### {heading}\n", 1)[1].split("\n#", 1)[0]
    rows = []
    for line in section.splitlines():
        if not line.startswith("| `"):
            continue
        name, _type, default, *_ = (cell.strip() for cell in line.strip("|").split("|"))
        rows.append(
            (
                name.strip("`"),
                default
                if default in {"required", "none"}
                else ast.literal_eval(
                    default.strip("`"),
                ),
            ),
        )
    return rows


def _signature_rows(component: type) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    for parameter in inspect.signature(component).parameters.values():
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            rows.append((f"**{parameter.name}", "none"))
        elif parameter.default is inspect.Parameter.empty:
            rows.append((parameter.name, "required"))
        else:
            rows.append((parameter.name, parameter.default))
    return rows


def test_when_the_changelog_upgrade_sql_is_read__then_startup_runs_the_same() -> None:
    """The SQL "Upgrading from 0.4.x" has users run is what startup() runs."""
    # The block sits under `[Unreleased]` until the release renames it, and
    # under `[0.5.0]` after it, with an empty `[Unreleased]` above. So it is
    # found by its title, not by the position of the first section, and it ends
    # at the next group or the next section, whichever comes first.
    upgrading = (
        CHANGELOG.read_text()
        .split("Upgrading from 0.4.x:", 1)[1]
        .split("\n### ", 1)[0]
        .split("\n## ", 1)[0]
    )
    blocks = re.findall(
        r"^ *```sql\n(.*?)^ *```$",
        upgrading,
        re.DOTALL | re.MULTILINE,
    )
    concurrently = re.findall(r"`(CREATE INDEX CONCURRENTLY [^`]+)`", upgrading)
    driver = PostgresqlBroker(dsn="unused").driver
    upgrade = driver.add_columns_query.make_query(
        driver.added_columns,
        driver.added_indexes,
    )

    assert [textwrap.dedent(block) for block in blocks] == [f"{upgrade}\n"]
    # The same index, built without blocking writes on a large table.
    assert len(concurrently) == 1
    assert _normalized_sql(concurrently[0].replace(" CONCURRENTLY", "")) in (
        _normalized_sql(upgrade)
    )


@pytest.mark.parametrize(
    "component",
    [PostgresqlBroker, PostgresqlResultBackend, PostgresqlSchedulerSource],
    ids=lambda component: component.__name__,
)
def test_when_a_constructor_table_is_read__then_it_matches_the_signature(
    component: type,
) -> None:
    assert _readme_table(component.__name__) == _signature_rows(component)


UNUSED_ARGUMENTS = ("max_retry_attempts", "pool_kwargs")


def test_when_arguments_are_documented_as_unused__then_the_code_never_reads_them() -> (
    None
):
    """The README calls these two arguments unused; nothing may load them."""
    package = Path(inspect.getfile(PostgresqlBroker)).parent
    reads = [
        f"{path.name}:{node.lineno} {node.attr}"
        for path in package.rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Attribute)
        and isinstance(node.ctx, ast.Load)
        and node.attr in UNUSED_ARGUMENTS
    ]
    assert reads == []
    rows = {
        line.split("|")[1].strip(" `"): line
        for line in README.read_text(encoding="utf-8").splitlines()
        if line.startswith("| `")
    }
    for argument in UNUSED_ARGUMENTS:
        assert "Accepted for compatibility, currently unused." in rows[argument]


def _normalized_sql(statement: str) -> str:
    collapsed = " ".join(statement.split())
    return re.sub(r" ?([(),;]) ?", r"\1", collapsed)


def test_when_the_schema_blocks_are_read__then_they_match_the_generated_sql() -> None:
    """
    The README's DDL is what ``run_migrations=True`` executes.

    It is also what ``taskiq postgresql generate`` prints;
    ``tests/test_migration_script.py`` checks that both build the same tables.
    """
    expected = {}
    for component in (
        PostgresqlBroker(dsn="unused"),
        PostgresqlResultBackend(dsn="unused"),
        PostgresqlSchedulerSource(dsn="unused"),
    ):
        driver = component.driver
        statements = [
            f"{driver.create_table_query.make_query()};",
            driver.create_index_query.make_query(driver.index_columns),
        ]
        if driver.added_columns or driver.added_indexes:
            # The broker's upgrade step, which startup runs after the CREATEs.
            statements.append(
                driver.add_columns_query.make_query(
                    driver.added_columns,
                    driver.added_indexes,
                ),
            )
        expected[driver.table_name] = _normalized_sql(" ".join(statements))
    assert "ALTER TABLE taskiq_messages" in expected["taskiq_messages"]
    generated = {}
    for kind in migrations.COMPONENT_KINDS:
        schema = table_schema(kind)
        script = render_sql([schema], transaction=False, lock=False, lock_timeout=0)
        # Step 2's statements span lines, so the lines are joined with a break.
        generated[schema.name] = _normalized_sql(
            "\n".join(
                line for line in script.splitlines() if not line.startswith("--")
            ),
        )
    documented = {}
    for block in _blocks("sql"):
        if block.section != SCHEMA_SECTION:
            continue
        table = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", block.source)
        assert table, block.source
        documented[table.group(1)] = _normalized_sql(block.source)
    assert documented == expected
    assert documented == generated


@pytest.mark.parametrize(
    "component",
    [PostgresqlBroker, PostgresqlResultBackend, PostgresqlSchedulerSource],
    ids=lambda component: component.__name__,
)
def test_when_dsn_is_missing__then_the_troubleshooting_messages_are_real(
    component: Any,
) -> None:
    text = _readme_text()
    with pytest.raises(TypeError) as missing:
        component()
    assert (
        f"{component.__name__}.__init__() missing 1 required positional argument"
        in str(missing.value)
    )
    with pytest.raises(TypeError) as none:
        component(dsn=None)
    with pytest.raises(TypeError) as empty:
        component(dsn="")
    assert f"{component.__name__} needs a non-empty PostgreSQL connection string" in (
        str(none.value)
    )
    assert "for 'dsn', but got None." in str(none.value)
    assert "for 'dsn', but got ''." in str(empty.value)
    assert (
        "`TypeError: PostgresqlBroker.__init__() missing 1 required positional "
        "argument: 'dsn'`"
    ) in text
    assert (
        "`TypeError: PostgresqlBroker needs a non-empty PostgreSQL connection "
        "string for 'dsn', but got None.`"
    ) in text
    assert "`got ''`" in text


def _stop(process: "subprocess.Popen[str]") -> None:
    """
    Stop a taskiq process and everything it started.

    SIGINT asks for a graceful stop. Whatever is still running afterwards is
    killed with its whole process group, so a worker child that does not stop
    never outlives the test.
    """
    process.send_signal(signal.SIGINT)
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=SHUTDOWN_TIMEOUT)
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    process.wait()


async def _listen_sessions(dsn: str) -> int:
    connection = await asyncpg.connect(dsn)
    try:
        return await connection.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND query ILIKE 'LISTEN %'",
        )
    finally:
        await connection.close()


def _wait_for_listener(
    dsn: str,
    worker: "subprocess.Popen[str]",
    lines: "queue.Queue[str]",
    seen: list[str],
) -> None:
    """
    Wait until a worker process LISTENs on the database.

    The worker's own "Listening started." line is not used: on Python 3.14 and
    newer, taskiq 0.13.0 sets up logging in its worker processes only when
    they start with ``spawn``, and Linux starts them with ``forkserver``.
    """
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        with contextlib.suppress(queue.Empty):
            while True:
                seen.append(lines.get_nowait())
        if worker.poll() is not None:
            pytest.fail(f"the worker exited; output so far:\n{''.join(seen)}")
        if asyncio.run(_listen_sessions(dsn)):
            return
        time.sleep(0.5)
    pytest.fail(f"no worker LISTENs; output so far:\n{''.join(seen)}")


def _start(
    command: list[str], cwd: Path, env: dict[str, str]
) -> tuple[
    "subprocess.Popen[str]",
    "queue.Queue[str]",
]:
    process = subprocess.Popen(  # noqa: S603
        _taskiq(command),
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    lines: queue.Queue[str] = queue.Queue()
    assert process.stdout is not None
    threading.Thread(target=_pump, args=(process.stdout, lines), daemon=True).start()
    return process, lines


@pytest.mark.timeout(180)
def test_when_the_quickstart_is_followed__then_a_task_runs_and_a_schedule_is_stored(
    driver: str,
    scratch_dsn: str,
    tmp_path: Path,
) -> None:
    scripts = _scripts()
    assert scripts["app.py"].count('driver="asyncpg"') == 3  # noqa: PLR2004
    scripts["app.py"] = scripts["app.py"].replace(
        'driver="asyncpg"',
        f'driver="{driver}"',
    )
    for name, source in scripts.items():
        (tmp_path / name).write_text(source, encoding="utf-8")
    env = _environment(scratch_dsn)
    worker_command, scheduler_command = _commands("quickstart")
    assert worker_command == ["taskiq", "worker", "app:broker"]
    assert scheduler_command == ["taskiq", "scheduler", "app:scheduler"]

    worker, lines = _start(worker_command, tmp_path, env)
    seen: list[str] = []
    try:
        _wait_for_listener(scratch_dsn, worker, lines, seen)

        kick = subprocess.run(
            [sys.executable, "kick.py"],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT,
            check=False,
        )
        assert_exited_cleanly(kick, driver)
        assert kick.stdout.strip() == "3"

        schedule = subprocess.run(
            [sys.executable, "schedule.py"],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT,
            check=False,
        )
        assert_exited_cleanly(schedule, driver)

        scheduler, scheduler_lines = _start(scheduler_command, tmp_path, env)
        scheduler_seen: list[str] = []
        try:
            _wait_for(scheduler_lines, SCHEDULER_READY, scheduler_seen)
            time.sleep(SCHEDULER_ALIVE_SECONDS)
            assert scheduler.poll() is None, "".join(scheduler_seen)
        finally:
            _stop(scheduler)
    finally:
        _stop(worker)

    async def stored_schedules() -> list[str]:
        source = PostgresqlSchedulerSource(dsn=scratch_dsn, driver=driver)
        try:
            return [item.task_name for item in await source.get_schedules()]
        finally:
            await source.shutdown()

    assert asyncio.run(stored_schedules()) == ["app:add"]


# --- Migrations ---------------------------------------------------------------

# What the README's psql line does, which the tests do through the driver.
PSQL_LINE = re.compile(r'psql "\$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f (?P<file>\S+)')


def _default_schemas() -> list[migrations.TableSchema]:
    """The tables of the quickstart's ``app:scheduler``, in its order."""
    return [table_schema(kind) for kind in migrations.COMPONENT_KINDS]


def _write_app(directory: Path, driver: str) -> None:
    """Write the quickstart's ``app.py`` with ``driver`` in place of asyncpg."""
    source = _scripts()["app.py"]
    assert source.count('driver="asyncpg"') == 3  # noqa: PLR2004
    (directory / "app.py").write_text(
        source.replace('driver="asyncpg"', f'driver="{driver}"'),
        encoding="utf-8",
    )


async def _tables(dsn: str) -> set[str]:
    connection = await asyncpg.connect(dsn)
    try:
        rows = await connection.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()",
        )
    finally:
        await connection.close()
    return {row["tablename"] for row in rows}


async def _drop_tables(dsn: str, tables: set[str]) -> None:
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(f"DROP TABLE {', '.join(sorted(tables))}")
    finally:
        await connection.close()


@pytest.mark.timeout(180)
def test_when_the_migration_commands_run__then_they_create_the_tables(
    driver: str,
    scratch_dsn: str,
    tmp_path: Path,
) -> None:
    _write_app(tmp_path, driver)
    env = _environment(scratch_dsn)
    ran: Counter[str] = Counter()
    printed: list[str] = []
    for line in _shell_lines(GENERATE_SECTION, MIGRATE_SECTION, TOOLS_SECTION):
        psql = PSQL_LINE.fullmatch(line)
        if psql is not None:
            script = (tmp_path / psql["file"]).read_text(encoding="utf-8")
            asyncio.run(run_script(scratch_dsn, driver, script))
            assert asyncio.run(_tables(scratch_dsn)) == DEFAULT_TABLES
            ran["psql"] += 1
            continue
        assert line.startswith("taskiq postgresql "), f"no runner for {line!r}"
        command, _, target = line.partition(" > ")
        subcommand = command.split()[2]
        if subcommand == "migrate":
            # So that only migrate can have created them.
            asyncio.run(_drop_tables(scratch_dsn, DEFAULT_TABLES))
        result = subprocess.run(  # noqa: S603
            _taskiq(command.split()),
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT,
            check=False,
        )
        assert_exited_cleanly(result, driver)
        ran[subcommand] += 1
        if target:
            output = tmp_path / target
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(result.stdout, encoding="utf-8")
        elif subcommand == "generate":
            printed.append(result.stdout)

    assert ran == Counter({"generate": 3, "psql": 1, "migrate": 1})
    assert asyncio.run(_tables(scratch_dsn)) == DEFAULT_TABLES
    assert (tmp_path / "taskiq_tables.sql").read_text(encoding="utf-8") == (
        render_sql(_default_schemas())
    )
    assert printed == [render_sql([table_schema("broker")])]
    assert (tmp_path / "db/migration/V2__taskiq_tables.sql").read_text(
        encoding="utf-8",
    ) == render_sql(_default_schemas(), transaction=False)


async def _use_the_components(dsn: str, driver: str) -> None:
    """Start the three components on ``dsn`` and write a row with each."""
    for run_migrations in (False, True):
        broker = PostgresqlBroker(dsn=dsn, driver=driver, run_migrations=run_migrations)  # type: ignore[arg-type]
        backend: PostgresqlResultBackend[int] = PostgresqlResultBackend(
            dsn=dsn,
            driver=driver,  # type: ignore[arg-type]
            run_migrations=run_migrations,
        )
        source = PostgresqlSchedulerSource(
            dsn=dsn,
            driver=driver,  # type: ignore[arg-type]
            run_migrations=run_migrations,
        )
        await broker.startup()
        await backend.startup()
        await source.startup()
        try:
            await broker.kick(
                BrokerMessage(
                    task_id=uuid.uuid4().hex,
                    task_name="readme:task",
                    message=b"message",
                    labels={},
                ),
            )
            await backend.set_result(
                uuid.uuid4().hex,
                TaskiqResult(
                    is_err=False,
                    log=None,
                    return_value=1,
                    execution_time=0.1,
                ),
            )
            await source.add_schedule(
                ScheduledTask(
                    task_name="readme:task",
                    labels={},
                    args=[],
                    kwargs={},
                    cron="* * * * *",
                ),
            )
        finally:
            await broker.shutdown()
            # The result backend's shutdown() leaves its pool open.
            await backend.driver.on_shutdown()
            await source.shutdown()


async def _as_a_dml_only_role(dsn: str, driver: str, grants: str) -> None:
    role = "readme_" + "".join(random.choices(string.ascii_lowercase, k=10))
    password = secrets.token_hex(16)
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute(
            f"CREATE ROLE {role} LOGIN PASSWORD '{scram_verifier(password)}'",
        )
        try:
            await admin.execute(grants.replace(README_ROLE, role))
            assert not await admin.fetchval(
                "SELECT has_schema_privilege($1, current_schema(), 'CREATE')",
                role,
            )
            parts = urlsplit(dsn)
            await _use_the_components(
                parts._replace(
                    netloc=f"{role}:{password}@{address_of(parts)}",
                ).geturl(),
                driver,
            )
        finally:
            await admin.execute(f"DROP OWNED BY {role}")
            await admin.execute(f"DROP ROLE {role}")
    finally:
        await admin.close()


def test_when_the_grants_are_applied__then_a_dml_only_role_runs_the_components(
    driver: str,
    scratch_dsn: str,
) -> None:
    grants = _block("sql", MIGRATE_SECTION).source
    assert grants.count(README_ROLE) == 3  # noqa: PLR2004
    asyncio.run(run_script(scratch_dsn, driver, render_sql(_default_schemas())))

    asyncio.run(_as_a_dml_only_role(scratch_dsn, driver, grants))


def test_when_the_format_example_is_installed__then_generate_prints_it(
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    header, entry = _block("toml", FORMAT_SECTION).source.strip().splitlines()
    assert header == (f'[project.entry-points."{migrations.MIGRATION_FORMATS_GROUP}"]')
    name, _, value = (part.strip() for part in entry.partition("="))
    target = re.fullmatch(r'"[\w.]+:(?P<function>\w+)"', value)
    assert target is not None, entry
    render = _run_block(_block("python", FORMAT_SECTION), ca_file)[target["function"]]

    class ExampleEntryPoint:
        """Stands for the entry point the TOML block registers."""

        def __init__(self) -> None:
            self.name = name

        def load(self) -> Any:
            """Return the example's format."""
            return render

    monkeypatch.setattr(migrations, "_entry_points", lambda: [ExampleEntryPoint()])
    command = ["generate", "--component", "result_backend", "--format", name]

    assert PostgresqlCMD().exec([*command, "--option", "title=Results"]) == 0
    assert capsys.readouterr().out == (
        "# Results\n"
        "## taskiq_results (result_backend)\n"
        "- `task_id`: uuid, not null\n"
        "- `result`: bytea, not null\n"
        "- `created_at`: timestamptz, not null\n"
        "- index `taskiq_results_task_id_idx`: hash on task_id\n"
    )
    assert PostgresqlCMD().exec([*command, "--option", "color=red"]) == 2  # noqa: PLR2004
    assert "unknown options: color" in capsys.readouterr().err


# --- Integrations -------------------------------------------------------------


def _section_blocks(section: str) -> list[Block]:
    return [block for block in _blocks("python") if block.section == section]


@pytest.fixture
def project(
    tmp_path: Path,
    scratch_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """
    The working directory of a project that holds the quickstart's ``app.py``.

    The examples name ``"app:scheduler"``, which is imported from the working
    directory, as ``taskiq worker`` imports it; building its components
    connects to nothing.
    """
    _write_app(tmp_path, "asyncpg")
    monkeypatch.setenv("POSTGRESQL_URL", scratch_dsn)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    sys.modules.pop("app", None)
    yield tmp_path
    sys.modules.pop("app", None)


def test_when_the_integration_install_lines_are_read__then_each_extra_exists() -> None:
    lines = _shell_lines(INTEGRATIONS_SECTION)
    installs = [
        re.fullmatch(r'pip install "taskiq-postgresql\[(?P<extras>[\w,]+)\]"', line)
        for line in lines
    ]
    assert all(installs), lines
    named = {extra for line in installs if line for extra in line["extras"].split(",")}
    provided = set(
        importlib_metadata.metadata("taskiq-postgresql").get_all("Provides-Extra") or []
    )
    assert {"sqlalchemy", "sqlmodel", "alembic", "django"} <= named <= provided


def test_when_the_sqlalchemy_examples_run__then_their_metadata_creates_the_tables(
    project: Path,
    scratch_dsn: str,
    ca_file: Path,
) -> None:
    models, with_sqlmodel = _section_blocks(SQLALCHEMY_SECTION)
    assert models.source.startswith("# models.py\n")

    namespace = _run_block(models, ca_file)

    assert [table.name for table in namespace["taskiq_tables"]] == [
        schema.name for schema in _default_schemas()
    ]
    engine = create_engine(sqlalchemy_url(scratch_dsn))
    try:
        namespace["Base"].metadata.create_all(engine)
    finally:
        engine.dispose()
    assert asyncio.run(_tables(scratch_dsn)) == DEFAULT_TABLES

    before = set(SQLModel.metadata.tables)
    try:
        _run_block(with_sqlmodel, ca_file)
        assert set(SQLModel.metadata.tables) - before == DEFAULT_TABLES
    finally:
        for name in set(SQLModel.metadata.tables) - before:
            SQLModel.metadata.remove(SQLModel.metadata.tables[name])


def _steps_diffs(schemas: list[migrations.TableSchema]) -> list[tuple[str, str, int]]:
    """What autogenerate reports for ``schemas`` on an empty database."""
    return [
        ("taskiq_postgresql_step", schema.name, step.number)
        for schema in sorted(schemas, key=lambda table: table.name)
        for step in schema.steps
    ]


@pytest.mark.alembic
def test_when_the_alembic_env_examples_run__then_autogenerate_writes_the_steps(
    project: Path,
    scratch_dsn: str,
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with_metadata, with_hook, with_filter, _ = _section_blocks(ALEMBIC_SECTION)

    class Base(DeclarativeBase):
        """Stands for the application's models: there are none."""

    class TablesBase(DeclarativeBase):
        """The models' base after ``add_taskiq_tables``, as the README shows."""

    add_taskiq_tables(TablesBase.metadata, "app:scheduler")
    models = types.ModuleType("myapp.models")
    monkeypatch.setitem(sys.modules, "myapp", types.ModuleType("myapp"))
    monkeypatch.setitem(sys.modules, "myapp.models", models)

    models.Base = Base  # type: ignore[attr-defined]
    target_metadata = _run_block(with_metadata, ca_file)["target_metadata"]
    include_object = _run_block(with_filter, ca_file)["include_object"]
    models.Base = TablesBase  # type: ignore[attr-defined]
    hooked = _run_block(with_hook, ca_file)["target_metadata"]

    steps = _steps_diffs(_default_schemas())
    assert autogenerate(scratch_dsn, target_metadata) == steps
    assert hooked is TablesBase.metadata
    assert autogenerate(scratch_dsn, hooked) == steps
    asyncio.run(run_script(scratch_dsn, "asyncpg", render_sql(_default_schemas())))
    assert autogenerate(scratch_dsn, target_metadata) == []
    assert autogenerate(scratch_dsn, hooked) == []
    # Left out of target_metadata, the tables would be dropped; the filter
    # keeps them.
    dropped = autogenerate(scratch_dsn, Base.metadata)
    assert {diff[1].name for diff in dropped if diff[0] == "remove_table"} == (
        DEFAULT_TABLES
    )
    assert autogenerate(scratch_dsn, Base.metadata, include_object=include_object) == []


# The env.py the CI test runs: the README's env.py lines, then Alembic's usual
# online run. The URL comes from the environment, never from a file.
_CHECK_ENV_PY = """\
import os

from alembic import context
from sqlalchemy import create_engine, pool

{readme_lines}

engine = create_engine(os.environ["TASKIQ_TEST_ALEMBIC_URL"], poolclass=pool.NullPool)
with engine.connect() as connection:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()
"""


@pytest.mark.alembic
@pytest.mark.timeout(180)
def test_when_the_alembic_check_lines_run__then_a_missing_step_fails_them(
    project: Path,
    scratch_dsn: str,
) -> None:
    with_metadata, *_ = _section_blocks(ALEMBIC_SECTION)
    lines = [
        line.split()
        for line in _shell_lines(ALEMBIC_SECTION)
        if line.startswith("alembic ")
    ]
    assert lines == [["alembic", "upgrade", "head"], ["alembic", "check"]]
    (project / "myapp").mkdir()
    (project / "myapp" / "__init__.py").write_text("", encoding="utf-8")
    (project / "myapp" / "models.py").write_text(
        "from sqlalchemy.orm import DeclarativeBase\n\n\n"
        "class Base(DeclarativeBase):\n    pass\n",
        encoding="utf-8",
    )
    (project / "alembic.ini").write_text(
        "[alembic]\nscript_location = %(here)s/alembic\n",
        encoding="utf-8",
    )
    (project / "alembic" / "versions").mkdir(parents=True)
    (project / "alembic" / "env.py").write_text(
        _CHECK_ENV_PY.format(readme_lines=with_metadata.source),
        encoding="utf-8",
    )
    template = Path(alembic_package.__file__).parent / "templates" / "generic"
    (project / "alembic" / "script.py.mako").write_text(
        (template / "script.py.mako").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    environment = {
        **_environment(scratch_dsn),
        "TASKIQ_TEST_ALEMBIC_URL": sqlalchemy_url(scratch_dsn).render_as_string(
            hide_password=False,
        ),
    }

    def run(arguments: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [sys.executable, "-m", *arguments],
            cwd=project,
            env=environment,
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT,
            check=False,
        )

    behind = [run(line) for line in lines]
    revision = run(["alembic", "revision", "--autogenerate", "-m", "taskiq tables"])
    migrated = [run(line) for line in lines]

    upgraded, checked = behind
    assert upgraded.returncode == 0, upgraded.stderr
    assert checked.returncode != 0
    message = (
        "New upgrade operations detected: "
        "[('taskiq_postgresql_step', 'taskiq_messages', 1)"
    )
    assert message in checked.stdout + checked.stderr
    assert message in _readme_text()
    assert revision.returncode == 0, revision.stderr
    assert [result.returncode for result in migrated] == [0, 0], [
        result.stdout + result.stderr for result in migrated
    ]
    assert asyncio.run(_tables(scratch_dsn)) == {*DEFAULT_TABLES, "alembic_version"}


@pytest.mark.alembic
def test_when_the_alembic_revision_examples_run__then_they_create_the_tables(
    project: Path,
    scratch_dsn: str,
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    *_, revision_block = _section_blocks(ALEMBIC_SECTION)
    settings = types.ModuleType("myapp.settings")
    settings.QUEUE = "staging"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "myapp", types.ModuleType("myapp"))
    monkeypatch.setitem(sys.modules, "myapp.settings", settings)
    broker, *others = [
        table_schema(kind, table_name=f"staging_{suffix}")
        for kind, suffix in zip(
            migrations.COMPONENT_KINDS,
            ("messages", "results", "schedulers"),
            strict=True,
        )
    ]
    staging = {"staging_messages", "staging_results", "staging_schedulers"}

    revision = _run_block(revision_block, ca_file)

    assert (revision["revision"], revision["down_revision"]) == ("taskiq_tables", None)
    assert offline_sql(revision["upgrade"]) == "".join(
        f"{statement};\n\n"
        for statement in [
            *bounded(migration_statements([broker], through=2)),
            *bounded(migration_statements(others, through=1)),
        ]
    )
    for _ in range(2):
        run_upgrade(scratch_dsn, "psycopg", revision["upgrade"])
    assert asyncio.run(_tables(scratch_dsn)) == staging
    asyncio.run(_drop_tables(scratch_dsn, staging))

    (line,) = [
        line for line in _shell_lines(ALEMBIC_SECTION) if line.startswith("taskiq ")
    ]
    command, _, target = line.partition(" > ")
    result = subprocess.run(  # noqa: S603
        _taskiq(command.split()),
        cwd=project,
        env=_environment(scratch_dsn),
        capture_output=True,
        text=True,
        timeout=SCRIPT_TIMEOUT,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == alembic_format(
        _default_schemas(),
        FormatOptions(extra={"revision": "taskiq_tables"}),
    )
    written = project / target
    written.parent.mkdir(parents=True)
    written.write_text(result.stdout, encoding="utf-8")
    generated: dict[str, Any] = {"__name__": "revision"}
    exec(compile(result.stdout, str(written), "exec"), generated)  # noqa: S102
    run_upgrade(scratch_dsn, "psycopg", generated["upgrade"])
    assert asyncio.run(_tables(scratch_dsn)) == DEFAULT_TABLES


def _taskiq_settings(namespace: dict[str, Any]) -> dict[str, Any]:
    """The app's settings a README ``settings.py`` block defines."""
    return {
        name: value
        for name, value in namespace.items()
        if name.startswith("TASKIQ_POSTGRESQL_")
    }


@pytest.mark.timeout(180)
def test_when_the_django_examples_run__then_migrate_creates_the_tables(
    project: Path,
    scratch_dsn: str,
    postgresql_dsn: str,
    ca_file: Path,
    django_configured: None,
) -> None:
    # Imported here: collecting the other README tests needs neither Django
    # nor the app.
    from django.core.management import call_command  # noqa: PLC0415
    from django.test.utils import override_settings  # noqa: PLC0415

    from taskiq_postgresql.contrib.django_format import (  # noqa: PLC0415
        django_format,
    )
    from tests.django_support import django_database  # noqa: PLC0415

    with_paths, with_dicts = _section_blocks(DJANGO_SECTION)
    assert with_paths.source.startswith("# settings.py\n")
    paths = _run_block(with_paths, ca_file)
    dicts = _run_block(with_dicts, ca_file)
    lines = _shell_lines(DJANGO_SECTION)
    manage = [
        line.split()[2:] for line in lines if line.startswith("python manage.py ")
    ]
    assert manage == [
        ["migrate", "taskiq_postgresql"],
        ["migrate", "taskiq_postgresql", "zero"],
    ]
    migrate, zero = manage
    assert "taskiq_postgresql.contrib.django" in paths["INSTALLED_APPS"]
    # `python manage.py` puts the project's directory first on sys.path, and
    # the app imports the paths with that sys.path alone, never from the
    # working directory (the project fixture puts sys.path back afterwards).
    sys.path.insert(0, str(project))

    with django_database(scratch_dsn):
        with override_settings(**_taskiq_settings(paths)):
            call_command(*migrate, verbosity=0)
        created = asyncio.run(_tables(scratch_dsn))
        call_command(*zero, verbosity=0)
        after_zero = asyncio.run(_tables(scratch_dsn))
    # The dict example on a database of its own, as a project that starts
    # with these settings.
    databases = new_database(postgresql_dsn)
    with contextlib.closing(databases):
        dicts_dsn = next(databases)
        with django_database(dicts_dsn), override_settings(**_taskiq_settings(dicts)):
            shown = call_command(
                "sqlmigrate",
                "taskiq_postgresql",
                "0001",
                stdout=io.StringIO(),
            )
            call_command(*migrate, verbosity=0)
        from_dicts = asyncio.run(_tables(dicts_dsn))

    assert created == {*DEFAULT_TABLES, "django_migrations"}
    # The app's own reverse runs no SQL: the tables stay.
    assert after_zero == created
    assert from_dicts == {"jobs", "job_results", "django_migrations"}
    assert "SET LOCAL lock_timeout = '5s'" in shown

    (line,) = [line for line in lines if line.startswith("taskiq ")]
    command, _, target = line.partition(" > ")
    result = subprocess.run(  # noqa: S603
        _taskiq(command.split()),
        cwd=project,
        env=_environment(scratch_dsn),
        capture_output=True,
        text=True,
        timeout=SCRIPT_TIMEOUT,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == django_format(
        _default_schemas(),
        FormatOptions(extra={"dependencies": "shop.0004_orders"}),
    )
    assert target == "shop/migrations/0005_taskiq_tables.py"
