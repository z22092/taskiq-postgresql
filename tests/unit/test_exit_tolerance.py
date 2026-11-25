import signal
import subprocess

import pytest

from tests.helpers import assert_exited_cleanly

GIL_ABORT = (
    "Fatal Python error: PyGILState_Release: thread state 0x7f00 must be current"
    " when releasing\nPython runtime state: finalizing (tstate=0x7f00)\n"
)
PYDANTIC_WARNING = (
    "/venv/lib/pydantic/_internal/_generate_schema.py:1: UserWarning: "
    "NoDefault is not a valid default\n"
)
TRACEBACK = 'Traceback (most recent call last):\n  File "kick.py", line 1\nValueError\n'
OTHER_FATAL = "Fatal Python error: Aborted\n\nCurrent thread 0x7f00 (most recent)\n"


def _script(returncode: int, stderr: str = "") -> "subprocess.CompletedProcess[str]":
    return subprocess.CompletedProcess(
        args=["python", "kick.py"],
        returncode=returncode,
        stdout="",
        stderr=stderr,
    )


@pytest.mark.parametrize(
    ("script", "driver", "version"),
    [
        pytest.param(_script(0), "psqlpy", (3, 10), id="exit-0"),
        pytest.param(
            _script(-signal.SIGABRT, PYDANTIC_WARNING),
            "psqlpy",
            (3, 10),
            id="silent-abort-3.10",
        ),
        pytest.param(
            _script(-signal.SIGABRT, PYDANTIC_WARNING),
            "psqlpy",
            (3, 12),
            id="silent-abort-3.12",
        ),
        pytest.param(
            _script(-signal.SIGABRT, GIL_ABORT),
            "psqlpy",
            (3, 11),
            id="gil-abort-3.11",
        ),
        pytest.param(
            _script(-signal.SIGABRT, GIL_ABORT),
            "psqlpy",
            (3, 12),
            id="gil-abort-3.12",
        ),
        pytest.param(
            _script(-signal.SIGABRT, GIL_ABORT),
            "psqlpy",
            (3, 10),
            id="gil-abort-3.10-control",
        ),
    ],
)
def test_when_the_exit_is_tolerated__then_nothing_is_raised(
    script: "subprocess.CompletedProcess[str]",
    driver: str,
    version: tuple[int, ...],
) -> None:
    assert_exited_cleanly(script, driver, version=version)


@pytest.mark.parametrize(
    ("script", "driver", "version"),
    [
        pytest.param(
            _script(-signal.SIGABRT, PYDANTIC_WARNING),
            "psqlpy",
            (3, 13),
            id="abort-3.13",
        ),
        pytest.param(
            _script(-signal.SIGABRT, PYDANTIC_WARNING),
            "asyncpg",
            (3, 10),
            id="abort-asyncpg",
        ),
        pytest.param(
            _script(-signal.SIGABRT, PYDANTIC_WARNING),
            "psycopg",
            (3, 10),
            id="abort-psycopg",
        ),
        pytest.param(
            _script(-signal.SIGABRT, TRACEBACK + PYDANTIC_WARNING),
            "psqlpy",
            (3, 10),
            id="abort-after-traceback",
        ),
        pytest.param(
            _script(-signal.SIGABRT, OTHER_FATAL),
            "psqlpy",
            (3, 10),
            id="abort-other-fatal-error",
        ),
        pytest.param(
            _script(1, PYDANTIC_WARNING),
            "psqlpy",
            (3, 10),
            id="exit-1",
        ),
        pytest.param(
            _script(-signal.SIGSEGV, PYDANTIC_WARNING),
            "psqlpy",
            (3, 10),
            id="segfault",
        ),
    ],
)
def test_when_the_exit_is_not_tolerated__then_it_fails(
    script: "subprocess.CompletedProcess[str]",
    driver: str,
    version: tuple[int, ...],
) -> None:
    with pytest.raises(AssertionError):
        assert_exited_cleanly(script, driver, version=version)
