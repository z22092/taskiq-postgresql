"""
The error ``taskiq postgresql`` prints when a driver fails on a connection string.

A driver that cannot read a connection string can quote any part of it in its
message, the password included, and the command writes its error to standard
error, which CI logs keep. None of these tests reaches a server: the string
fails before a connection is attempted, or the driver's connect is replaced.
"""

from __future__ import annotations

from pathlib import Path
from typing import NoReturn
from urllib.parse import unquote, urlsplit

import pytest

from taskiq_postgresql.drivers import _asyncpg
from tests.helpers import (
    UNUSED_DSN,
    closed_local_port,
    importable_modules,
    with_netloc,
    word,
)
from tests.test_cli import APP, run_in_process

PROG = "taskiq postgresql"
UNREADABLE = "the connection string is not valid"


@pytest.fixture
def halves() -> tuple[str, str]:
    """The two halves of a password, made at run time."""
    return word(), word()


def unreadable_url(halves: tuple[str, str]) -> str:
    """
    A URL whose password holds an unencoded ``/``.

    The ``/`` ends the authority, so asyncpg reads the first half as the port
    number and quotes it in its ``ValueError``.
    """
    malformed = "/".join(halves)
    return with_netloc(
        urlsplit(UNUSED_DSN),
        password=malformed,
        address="localhost",
    ).geturl()


def unreadable_key_value(halves: tuple[str, str]) -> str:
    """
    The ``key=value`` form libpq reads, with an unquoted space in the password.

    The space ends the value, so psqlpy reads the second half as the name of
    an option and quotes it in its error.
    """
    options = (
        ("host", "localhost"),
        ("dbname", "unused"),
        ("password", f"{halves[0]} {halves[1]}=on"),
    )
    return " ".join(f"{key}={value}" for key, value in options)


def assert_no_part_of(halves: tuple[str, str], output: str) -> None:
    for half in halves:
        assert half not in output


@pytest.mark.parametrize("command", ["plan", "check"])
def test_a_connection_string_the_driver_cannot_read_is_not_quoted(
    command: str,
    halves: tuple[str, str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, out, err = run_in_process(
        [command, "--component", "broker", "--dsn", unreadable_url(halves)],
        capsys,
    )

    assert code == 1
    assert_no_part_of(halves, out + err)
    assert err.startswith(f"{PROG}: error: DatabaseConnectionError: {UNREADABLE}")


def test_migrate_does_not_quote_a_connection_string_the_driver_cannot_read(
    halves: tuple[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("POSTGRESQL_URL", unreadable_url(halves))

    with importable_modules(tmp_path, monkeypatch) as write:
        code, out, err = run_in_process(
            ["migrate", f"{write(APP)}:broker", "--app-dir", str(tmp_path)],
            capsys,
        )

    assert code == 1
    assert_no_part_of(halves, out + err)
    assert err.startswith(f"{PROG}: error: DatabaseConnectionError: {UNREADABLE}")


def test_a_connection_string_psqlpy_cannot_read_is_not_quoted(
    halves: tuple[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("psqlpy")
    monkeypatch.setenv("POSTGRESQL_URL", UNUSED_DSN)
    monkeypatch.setenv("APP_DRIVER", "psqlpy")

    with importable_modules(tmp_path, monkeypatch) as write:
        code, out, err = run_in_process(
            [
                "check",
                f"{write(APP)}:broker",
                "--app-dir",
                str(tmp_path),
                "--dsn",
                unreadable_key_value(halves),
            ],
            capsys,
        )

    assert code == 1
    assert_no_part_of(halves, out + err)
    assert err.startswith(f"{PROG}: error: DatabaseConnectionError: {UNREADABLE}")


@pytest.mark.parametrize("value", ["", "  "], ids=["empty", "blank"])
@pytest.mark.parametrize(
    "selection",
    [["--component", "broker"], ["app:broker"]],
    ids=["component", "target"],
)
@pytest.mark.parametrize("command", ["plan", "check"])
def test_an_empty_dsn_is_a_usage_error(
    command: str,
    selection: list[str],
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # libpq and asyncpg read an empty string as "use the local defaults", a
    # server nobody named. Pointed at a closed port, so that even read that
    # way the string reaches no server.
    monkeypatch.setenv("PGHOST", "127.0.0.1")
    monkeypatch.setenv("PGPORT", str(closed_local_port()))

    code, out, err = run_in_process([command, *selection, "--dsn", value], capsys)

    assert (code, out) == (2, "")
    assert f"{PROG} {command}: error: argument --dsn: " in err


@pytest.mark.parametrize(
    "shape",
    ["url", "key-value"],
)
@pytest.mark.parametrize("command", ["plan", "check"])
def test_a_driver_message_that_quotes_the_connection_string_loses_it(
    shape: str,
    command: str,
    halves: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Percent-encoded, so a driver can quote the password as written or decoded.
    encoded = f"{halves[0]}%40{halves[1]}"
    if shape == "url":
        dsn = with_netloc(
            urlsplit(UNUSED_DSN),
            password=encoded,
            address="localhost",
        ).geturl()
    else:
        dsn = " ".join(
            f"{key}={value}"
            for key, value in (("host", "localhost"), ("password", f"'{encoded}'"))
        )

    async def refuse(connection_string: str, **_: object) -> NoReturn:
        # As a library that names what it could not connect with might.
        raise OSError(
            f"cannot connect with {connection_string} as {unquote(encoded)}",
        )

    monkeypatch.setattr(_asyncpg, "create_pool", refuse)

    code, out, err = run_in_process(
        [command, "--component", "broker", "--dsn", dsn],
        capsys,
    )

    assert code == 1
    assert dsn not in out + err
    assert_no_part_of(halves, out + err)
    assert err.startswith(f"{PROG}: error: DatabaseConnectionError: cannot connect ")
