"""
Run the README's Python examples against a real PostgreSQL.

The README is the PyPI long description, so an example that does not run is a
bug a user hits first. The quickstart scripts (marked ``# app.py``,
``# kick.py`` and ``# schedule.py`` on their first line) run as separate
processes, with a real ``taskiq worker``, once per installed driver. Every other
Python block is executed as written.
"""

import asyncio
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

README = Path(__file__).resolve().parent.parent / "README.md"
SCRIPTS = ("app.py", "kick.py", "schedule.py")
# The tables the quickstart creates under their default names.
QUICKSTART_TABLES = ("taskiq_messages", "taskiq_results", "taskiq_schedulers")
WORKER_READY = "Listening started"
STARTUP_TIMEOUT = 30
SCRIPT_TIMEOUT = 60
SHUTDOWN_TIMEOUT = 5

_BLOCK = re.compile(r"^```python\n(.*?)^```$", re.DOTALL | re.MULTILINE)


def _python_blocks() -> list[str]:
    return _BLOCK.findall(README.read_text(encoding="utf-8"))


def _scripts() -> dict[str, str]:
    """Return the quickstart scripts of the README, keyed by file name."""
    found = {}
    for block in _python_blocks():
        first_line = block.splitlines()[0]
        name = first_line.removeprefix("# ")
        if first_line.startswith("# ") and name in SCRIPTS:
            found[name] = block
    return found


def _other_blocks() -> list[str]:
    return [
        block
        for block in _python_blocks()
        if block.splitlines()[0].removeprefix("# ") not in SCRIPTS
    ]


def _environment(dsn: str) -> dict[str, str]:
    return {**os.environ, "POSTGRESQL_URL": dsn, "PYTHONUNBUFFERED": "1"}


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
def quickstart_tables(postgresql_dsn: str) -> Iterator[None]:
    """Drop the default-named quickstart tables before and after the test."""

    def drop() -> None:
        async def run() -> None:
            sink = PostgresqlBroker(dsn=postgresql_dsn, driver="asyncpg")
            async with sink.driver, sink.driver.connection() as connection:
                await connection.execute(
                    f"DROP TABLE IF EXISTS {', '.join(QUICKSTART_TABLES)}",
                )
            await sink.driver.on_shutdown()

        asyncio.run(run())

    drop()
    yield
    drop()


def test_when_readme_is_read__then_it_has_the_quickstart_scripts() -> None:
    assert sorted(_scripts()) == sorted(SCRIPTS)
    assert _other_blocks(), "the configuration examples are part of the README"


def test_when_readme_has_relative_links__then_none_is_allowed() -> None:
    """PyPI renders the README outside the repository, so links must be absolute."""
    text = README.read_text(encoding="utf-8")
    relative = re.findall(r"\]\((?!https?://|#|mailto:)[^)]*\)", text)
    assert relative == []


@pytest.mark.parametrize("block", _other_blocks())
def test_when_a_readme_block_is_executed__then_it_runs(
    block: str,
    postgresql_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", postgresql_dsn)
    exec(compile(block, "README.md", "exec"), {"__name__": "readme_block"})  # noqa: S102


def test_when_the_driver_options_block_runs__then_the_options_connect(
    postgresql_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pool and connection options shown in the README are accepted."""
    block = next(block for block in _other_blocks() if "min_size" in block)
    monkeypatch.setenv("POSTGRESQL_URL", postgresql_dsn)
    namespace: dict[str, Any] = {"__name__": "readme_block"}
    exec(compile(block, "README.md", "exec"), namespace)  # noqa: S102

    async def connect() -> None:
        backend, broker = namespace["result_backend"], namespace["broker"]
        async with backend.driver:
            assert backend.driver.pool is not None
        await backend.driver.on_shutdown()
        await broker.listen_driver.on_startup()
        await broker.listen_driver.on_shutdown()

    asyncio.run(connect())


@pytest.mark.timeout(180)
def test_when_the_quickstart_is_followed__then_a_task_runs_and_a_schedule_is_stored(
    driver: str,
    postgresql_dsn: str,
    quickstart_tables: None,
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
    env = _environment(postgresql_dsn)

    worker = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "taskiq",
            "worker",
            "app:broker",
            "--workers",
            "1",
        ],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines: queue.Queue[str] = queue.Queue()
    assert worker.stdout is not None
    threading.Thread(target=_pump, args=(worker.stdout, lines), daemon=True).start()
    seen: list[str] = []
    try:
        _wait_for(lines, WORKER_READY, seen)

        kick = subprocess.run(
            [sys.executable, "kick.py"],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT,
            check=False,
        )
        assert kick.returncode == 0, kick.stderr
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
        assert schedule.returncode == 0, schedule.stderr
    finally:
        # SIGINT is the graceful stop of `taskiq worker`; SIGTERM leaves it hanging.
        worker.send_signal(signal.SIGINT)
        try:
            worker.wait(timeout=SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            worker.kill()

    async def stored_schedules() -> list[str]:
        source = PostgresqlSchedulerSource(dsn=postgresql_dsn, driver=driver)
        try:
            return [item.task_name for item in await source.get_schedules()]
        finally:
            await source.shutdown()

    assert asyncio.run(stored_schedules()) == ["app:add"]
