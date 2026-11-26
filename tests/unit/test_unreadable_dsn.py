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
from tests.helpers import word

Halves = tuple[str, str]


def dsn_module() -> ModuleType:
    """
    The module this change adds names to, imported when a test runs.

    Imported on call, so that against code without the change each test fails
    on its own instead of the whole module failing to be collected.
    """
    return importlib.import_module("taskiq_postgresql.utils.dsn")


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


def upper_scheme_two_ats(a: str, b: str) -> str:
    """The same URI as ``two_ats``, with the scheme in upper case."""
    return f"POSTGRESQL://u:{a}@{b}@localhost/d"


def slash_then_at(a: str, b: str) -> str:
    """A password holding ``/``: the first half is read as the port."""
    return f"postgresql://u:{a}/{b}@h/db"


def upper_scheme_slash_then_at(a: str, b: str) -> str:
    """The same URI as ``slash_then_at``, with a mixed-case scheme."""
    return f"Postgres://u:{a}/{b}@h/db"


def non_digit_port(a: str, b: str) -> str:
    """A URI whose port is not made of digits."""
    return f"postgresql://u@h:{a}{b}/d"


def non_digit_port_among_hosts(a: str, b: str) -> str:
    """A multi-host URI whose second port is not made of digits."""
    return f"postgresql://u@h1:5432,h2:{a}{b}/d"


# What the shared check refuses, whichever driver reads the string.
SHARED = [
    two_ats,
    unencoded_slash,
    slash_then_at,
    upper_scheme_two_ats,
    upper_scheme_slash_then_at,
    non_digit_port,
    non_digit_port_among_hosts,
]


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

# What each parser refuses on top of the shared check: asyncpg reads URLs
# only and does not decode a percent escape strictly.
UNREADABLE = {
    "asyncpg": [*SHARED],
    "psycopg": [
        unquoted_space,
        option_name,
        bad_percent_escape,
        percent_e9,
        *SHARED,
    ],
    "psqlpy": [unquoted_space, option_name, *SHARED],
}

READABLE = {
    "multi-host": "postgresql://u@h1,h2/d",
    "multi-host-ports": "postgresql://u@h1:5432,h2:5433/d",
    "ipv6": "postgresql://u@[::1]:5432/d",
    "socket": "postgresql://u@%2Fvar%2Frun%2Fpostgresql/d",
    "empty-port": "postgresql://u@h:/d",
    # A query value may hold an "@", such as an e-mail in application_name.
    "at-in-the-query": "postgresql://u@h/d?application_name=a@b",
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
    assert dsn_module().UNREADABLE_DSN in str(error)
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
def test_when_the_uri_is_valid__then_it_is_not_refused(name: str) -> None:
    assert not dsn_module().is_ambiguous_uri(READABLE[name])


@pytest.mark.parametrize("name", KEY_VALUE)
def test_a_key_value_string_is_never_ambiguous(name: str) -> None:
    assert not dsn_module().is_ambiguous_uri(KEY_VALUE[name])


@pytest.mark.parametrize("scheme", ["postgresql", "postgres"])
@pytest.mark.parametrize("userinfo", ["u:x@y", "u@x@y", "@u@x"])
def test_when_the_authority_holds_two_ats__then_the_uri_is_ambiguous(
    scheme: str,
    userinfo: str,
) -> None:
    assert dsn_module().is_ambiguous_uri(f"{scheme}://{userinfo}@h/d")


@pytest.mark.parametrize("scheme", ["POSTGRESQL", "Postgres"])
def test_the_scheme_is_matched_in_any_case(scheme: str) -> None:
    assert dsn_module().is_ambiguous_uri(f"{scheme}://u:x@y@h/d")


@pytest.mark.parametrize("make_dsn", SHARED)
def test_when_a_uri_is_ambiguous__then_the_shared_check_refuses_it(
    make_dsn: Callable[[str, str], str],
) -> None:
    assert dsn_module().is_ambiguous_uri(make_dsn(word(), word()))


@pytest.mark.anyio
async def test_when_a_connection_option_is_wrong__then_its_own_error_is_kept() -> None:
    # min_size > max_size fails inside asyncpg before it connects, and is not
    # about the string, so it must not read as an unreadable one.
    driver = build_query(
        import_driver("asyncpg").AsyncpgDriver,
        "postgresql://u@localhost/d",
    )
    driver.connection_kwargs.update(min_size=5, max_size=1)

    with pytest.raises(DatabaseConnectionError) as raised:
        await driver.__aenter__()

    assert "min_size is greater than max_size" in str(raised.value)
    assert dsn_module().UNREADABLE_DSN not in str(raised.value)


@pytest.mark.anyio
async def test_when_psqlpy_rejects_a_setting__then_its_own_error_is_kept() -> None:
    driver = build_query(
        import_driver("psqlpy").PsqlpyDriver,
        "postgresql://u@localhost/d",
    )
    driver.connection_kwargs.update(max_db_pool_size=0)

    with pytest.raises(Exception, match="Maximum database pool size") as raised:
        await driver.__aenter__()

    assert dsn_module().UNREADABLE_DSN not in str(raised.value)
