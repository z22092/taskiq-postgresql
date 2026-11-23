from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

import pytest

from taskiq_postgresql.broker import PostgresqlBroker

pytestmark = pytest.mark.anyio

WORKERS = 2
STARTUP_TIMEOUT = 30.0
EXIT_TIMEOUT = 15.0

# Every process that imports the app records its pid. The workers are the
# processes that must not outlive the main one; with forkserver (the default
# from Python 3.14) they are not even its children.
APP_MODULE = """\
import os
from pathlib import Path

from taskiq_postgresql import PostgresqlBroker

Path(os.environ["WORKER_PIDS_DIR"], str(os.getpid())).touch()

broker = PostgresqlBroker(
    dsn=os.environ["WORKER_DSN"],
    driver=os.environ["WORKER_DRIVER"],
    channel_name=os.environ["WORKER_CHANNEL"],
    table_name=os.environ["WORKER_TABLE"],
)
"""


def recorded_pids(pids_dir: Path) -> set[int]:
    return {int(path.name) for path in pids_dir.iterdir()}


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def listen_sessions(broker: PostgresqlBroker, channel: str) -> int:
    # Inlined because each driver spells parameters differently; the channel
    # is the random lowercase table name plus a suffix.
    rows = await broker.driver.execute(
        f"SELECT pid FROM pg_stat_activity WHERE query ILIKE 'LISTEN %{channel}%'",  # noqa: S608
    )
    return len(rows)


async def test_worker_stops_on_sigint_while_idle(
    broker: PostgresqlBroker,
    driver: str,
    postgresql_dsn: str,
    tmp_path: Path,
) -> None:
    """
    ``taskiq worker`` exits on SIGINT while no message arrives.

    The workers wait on a channel nothing notifies. The worker's shutdown
    signal only sets an ``asyncio.Event``, which runs nothing until the event
    loop wakes up, so a listen that never wakes it keeps the workers, and the
    main process that joins them, running.

    The test waits for the LISTEN sessions instead of the "Listening started."
    log line: from Python 3.14, taskiq configures no logging in the workers.
    """
    channel = f"{broker.table_name}_worker"
    pids_dir = tmp_path / "pids"
    pids_dir.mkdir()
    (tmp_path / "app.py").write_text(APP_MODULE)
    log_path = tmp_path / "worker.log"
    env = {
        **os.environ,
        "WORKER_PIDS_DIR": str(pids_dir),
        "WORKER_DSN": postgresql_dsn,
        "WORKER_DRIVER": driver,
        "WORKER_CHANNEL": channel,
        "WORKER_TABLE": broker.table_name,
    }

    with log_path.open("wb") as log:
        main = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                "taskiq",
                "worker",
                "app:broker",
                "--workers",
                str(WORKERS),
            ],
            cwd=tmp_path,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            # Its own process group: the cleanup kills the group, which
            # holds every process it starts, whatever their parent.
            start_new_session=True,
        )
    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while await listen_sessions(broker, channel) < WORKERS:
            assert main.poll() is None, log_path.read_text()
            assert time.monotonic() < deadline, log_path.read_text()
            await asyncio.sleep(0.2)
        # A signal sent between LISTEN and the wait for a notification stops
        # the worker on any version; give the workers time to start waiting.
        await asyncio.sleep(1)
        workers = recorded_pids(pids_dir)
        assert len(workers) == WORKERS

        main.send_signal(signal.SIGINT)

        deadline = time.monotonic() + EXIT_TIMEOUT
        while main.poll() is None and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        assert main.poll() is not None, (
            f"taskiq worker still running {EXIT_TIMEOUT}s after SIGINT:\n"
            f"{log_path.read_text()}"
        )
        assert not {pid for pid in workers if is_running(pid)}
    finally:
        # Reaches the workers too, even once the main process is gone.
        with suppress(ProcessLookupError):
            os.killpg(main.pid, signal.SIGKILL)
        main.wait()
