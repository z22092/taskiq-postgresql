"""
Check the README against the code: run its examples, compare its tables.

The README is the PyPI long description, so an example that does not run, or a
table that disagrees with the signatures, is a bug a user hits first. The
quickstart scripts (marked ``# app.py``, ``# kick.py`` and ``# schedule.py`` on
their first line) run as separate processes, with the README's own ``taskiq``
commands, once per installed driver. Every other Python block is executed as
written.

Every test that touches a database does so in a database of its own, created
and dropped here, because the README examples use the default table names and
must never run in tables the test did not create.
"""

import ast
import asyncio
import contextlib
import inspect
import os
import queue
import random
import re
import signal
import ssl
import string
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from taskiq import TaskiqResult
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers import JSONSerializer, MSGPackSerializer

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import create_database, drop_database, in_database

README = Path(__file__).resolve().parent.parent / "README.md"
SCRIPTS = ("app.py", "kick.py", "schedule.py")
CA_PLACEHOLDER = "/path/to/ca.pem"
SCHEDULER_READY = "Starting scheduler"
STARTUP_TIMEOUT = 30
SCRIPT_TIMEOUT = 60
SHUTDOWN_TIMEOUT = 5
SCHEDULER_ALIVE_SECONDS = 2

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
    return [block for block in _blocks("python") if block.id not in SCRIPTS]


def _commands() -> list[list[str]]:
    """Return the ``taskiq`` commands the README's shell blocks show."""
    return [
        line.split()
        for block in _blocks("bash")
        for line in block.source.splitlines()
        if line.startswith("taskiq ")
    ]


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
    """The README's DDL is what ``run_migrations=True`` executes."""
    expected = {}
    for component in (
        PostgresqlBroker(dsn="unused"),
        PostgresqlResultBackend(dsn="unused"),
        PostgresqlSchedulerSource(dsn="unused"),
    ):
        driver = component.driver
        expected[driver.table_name] = _normalized_sql(
            f"{driver.create_table_query.make_query()};"
            f"{driver.create_index_query.make_query(driver.index_columns)}",
        )
    documented = {}
    for block in _blocks("sql"):
        table = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", block.source)
        assert table, block.source
        documented[table.group(1)] = _normalized_sql(block.source)
    assert documented == expected


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


def _assert_exited_cleanly(
    script: subprocess.CompletedProcess[str],
    driver: str,
) -> None:
    """
    Assert that a README script exited with code 0.

    psqlpy 0.13.1 on Python 3.10 occasionally aborts the interpreter at exit,
    after the script's work is done, with ``PyGILState_Release`` while the
    runtime is finalizing; psqlpy alone, without this package, does it too
    (about 1 run in 80). Only that abort is tolerated, and the caller still
    checks the script's output.
    """
    if (
        script.returncode == -signal.SIGABRT
        and driver == "psqlpy"
        and sys.version_info < (3, 11)
        and "Fatal Python error: PyGILState_Release" in script.stderr
        and "finalizing" in script.stderr
    ):
        return
    assert script.returncode == 0, script.stderr


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
    worker_command, scheduler_command = _commands()
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
        _assert_exited_cleanly(kick, driver)
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
        _assert_exited_cleanly(schedule, driver)

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
