"""
What every driver does with a connection string it cannot read.

A parser quotes part of such a string in its error, password included, and
psycopg's pool logs that error from its workers on every retry. None of these
tests reaches a server: each string fails to parse before a connection is
tried, and the strings that parse are checked against stand-ins that connect
to nothing.
"""

import importlib
import logging
from collections.abc import Callable
from types import ModuleType
from typing import Any

import pytest

from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.exceptions import DatabaseConnectionError
from taskiq_postgresql.utils.dsn import UNREADABLE_DSN, has_ambiguous_userinfo
from tests.helpers import word

Halves = tuple[str, str]


def unquoted_space(a: str, b: str) -> str:
    """A key=value string whose password holds an unquoted space."""
    return f"host=localhost dbname=d user=u password={a} {b}"


def option_name(a: str, b: str) -> str:
    """A key=value string whose password holds a space, then ``<b>=on``."""
    return f"host=localhost dbname=d user=u password={a} {b}=on"


def bad_percent_escape(a: str, b: str) -> str:
    """A URI whose password holds an invalid percent escape."""
    return f"postgresql://u:{a}%zz{b}@localhost/d"


def percent_e9(a: str, b: str) -> str:
    """A URI whose password holds ``%e9``, which is not UTF-8 on its own."""
    return f"postgresql://u:{a}%e9{b}@localhost/d"


def unencoded_slash(a: str, b: str) -> str:
    """A URL whose password holds an unencoded ``/``, which ends the authority."""
    return f"postgresql://u:{a}/{b}@localhost/d"


def two_ats(a: str, b: str) -> str:
    """A URI whose authority holds a second, unencoded ``@``."""
    return f"postgresql://u:{a}@{b}@localhost/d"


def build_query(driver_class: Any, dsn: str) -> Any:
    """A query driver for ``dsn``; nothing connects until it is entered."""
    key = PrimaryKeyColumn()
    return driver_class(dsn, "t", columns=[key, Column("v", "TEXT")], primary_key=key)


def build_listen(driver_class: Any, dsn: str) -> Any:
    """A listen driver for ``dsn``; nothing connects until it starts."""
    return driver_class(dsn, "channel")


# (extra, class, how to build it, the method that reads the string, side).
SIDES = [
    ("asyncpg", "AsyncpgDriver", build_query, "__aenter__", "query"),
    ("asyncpg", "AsyncpgListenDriver", build_listen, "on_startup", "listen"),
    ("psycopg", "PsycopgDriver", build_query, "__aenter__", "query"),
    ("psycopg", "PsycopgListenDriver", build_listen, "on_startup", "listen"),
    ("psqlpy", "PsqlpyDriver", build_query, "__aenter__", "query"),
    ("psqlpy", "PsqlpyListenDriver", build_listen, "on_startup", "listen"),
]

# What each parser refuses: asyncpg reads URLs only and does not decode a
# percent escape strictly; psqlpy reads a URL's password up to the last "@"
# and keeps a "/" in it, so only the strings it refuses are listed.
UNREADABLE = {
    "asyncpg": [unquoted_space, option_name, unencoded_slash, two_ats],
    "psycopg": [
        unquoted_space,
        option_name,
        bad_percent_escape,
        percent_e9,
        unencoded_slash,
        two_ats,
    ],
    "psqlpy": [unquoted_space, option_name, two_ats],
}

READABLE = {
    "multi-host": "postgresql://u@h1,h2/d",
    "multi-host-ports": "postgresql://u@h1:5432,h2:5433/d",
    "ipv6": "postgresql://u@[::1]:5432/d",
    "socket": "postgresql://u@%2Fvar%2Frun%2Fpostgresql/d",
    "at-in-the-query": "postgresql://u@h/d?options=a@b",
}
KEY_VALUE = {
    "key-value": "host=localhost port=5432 dbname=d user=u",
    "key-value-socket": "host=/var/run/postgresql dbname=d",
    "key-value-at": "host=localhost user=u password=a@b@c",
}


def import_driver(extra: str) -> ModuleType:
    """Import the driver module of ``extra``, skipping when the extra is missing."""
    pytest.importorskip(extra)
    return importlib.import_module(f"taskiq_postgresql.drivers._{extra}")


def assert_nothing_leaked(
    error: BaseException,
    caplog: pytest.LogCaptureFixture,
    halves: Halves,
) -> None:
    """No text of ``error``, of the log or of its traceback holds the password."""
    pieces = (*halves, "".join(halves))
    texts = [str(error), repr(error), *map(str, error.args), caplog.text]
    texts.extend(record.getMessage() for record in caplog.records)
    # Not the first frame, the test's own, which built the string: the frames
    # the driver ran in are what an error tracker that captures locals sends.
    traceback = error.__traceback__
    traceback = traceback.tb_next if traceback is not None else None
    while traceback is not None:
        for value in traceback.tb_frame.f_locals.values():
            texts.append(value if isinstance(value, str) else repr(value))
        traceback = traceback.tb_next
    for text in texts:
        assert not any(piece in text for piece in pieces), text


@pytest.mark.parametrize(
    ("extra", "class_name", "build", "method", "make_dsn"),
    [
        pytest.param(
            extra,
            class_name,
            build,
            method,
            make_dsn,
            id=f"{extra}-{side}-{make_dsn.__name__}",
        )
        for extra, class_name, build, method, side in SIDES
        for make_dsn in UNREADABLE[extra]
    ],
)
@pytest.mark.anyio
async def test_when_the_string_cannot_be_read__then_the_error_is_fixed_and_empty(
    extra: str,
    class_name: str,
    build: Callable[[Any, str], Any],
    method: str,
    make_dsn: Callable[[str, str], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    halves = (word(), word())
    driver = build(getattr(import_driver(extra), class_name), make_dsn(*halves))

    with (
        caplog.at_level(logging.DEBUG),
        pytest.raises(DatabaseConnectionError) as raised,
    ):
        await getattr(driver, method)()

    error = raised.value
    assert UNREADABLE_DSN in str(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert_nothing_leaked(error, caplog, halves)
    await driver.on_shutdown()


@pytest.mark.parametrize("name", [*READABLE, *KEY_VALUE])
@pytest.mark.anyio
async def test_when_the_string_is_valid__then_the_query_drivers_build_their_pool(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = {**READABLE, **KEY_VALUE}[name]
    psycopg_module = import_driver("psycopg")
    psycopg_driver = build_query(psycopg_module.PsycopgDriver, dsn)

    async with psycopg_driver:
        # open=False stays: entering the driver never connects.
        assert psycopg_driver.pool is not None
        assert psycopg_driver.pool.closed
    await psycopg_driver.on_shutdown()

    psqlpy_driver = build_query(import_driver("psqlpy").PsqlpyDriver, dsn)
    async with psqlpy_driver:
        assert psqlpy_driver.pool is not None
    await psqlpy_driver.on_shutdown()

    if name in KEY_VALUE:
        return  # asyncpg reads URLs only
    asyncpg_module = import_driver("asyncpg")
    seen: list[str] = []

    async def create_pool(connection_string: str, **_: object) -> object:
        seen.append(connection_string)
        return object()

    monkeypatch.setattr(asyncpg_module, "create_pool", create_pool)
    asyncpg_driver = build_query(asyncpg_module.AsyncpgDriver, dsn)

    async with asyncpg_driver:
        assert seen == [dsn]
    asyncpg_driver.pool = None


@pytest.mark.parametrize("name", READABLE)
def test_when_the_uri_is_valid__then_it_is_not_ambiguous(name: str) -> None:
    assert not has_ambiguous_userinfo(READABLE[name])


@pytest.mark.parametrize("name", KEY_VALUE)
def test_a_key_value_string_is_never_ambiguous(name: str) -> None:
    assert not has_ambiguous_userinfo(KEY_VALUE[name])


@pytest.mark.parametrize("scheme", ["postgresql", "postgres"])
@pytest.mark.parametrize("userinfo", ["u:x@y", "u@x@y", "@u@x"])
def test_when_the_authority_holds_two_ats__then_the_uri_is_ambiguous(
    scheme: str,
    userinfo: str,
) -> None:
    assert has_ambiguous_userinfo(f"{scheme}://{userinfo}@h/d")
