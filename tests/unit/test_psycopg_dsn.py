"""
What the psycopg driver does with a connection string libpq cannot parse.

libpq quotes part of such a string in its error, password included, and
psycopg_pool logs that error from its workers on every retry. None of these
tests reaches a server: the string fails to parse before a connection is tried.
"""

import logging
import random
import string
from collections.abc import Callable
from typing import Optional

import pytest

from taskiq_postgresql.abc.query import Column, PrimaryKeyColumn
from taskiq_postgresql.drivers._psycopg import PsycopgDriver, PsycopgListenDriver
from taskiq_postgresql.exceptions import DatabaseConnectionError


def word() -> str:
    """Letters only, made at run time, so no literal looks like a credential."""
    return "".join(random.choices(string.ascii_lowercase, k=12))


def unquoted_space(secret: str) -> str:
    """A key=value string whose password holds an unquoted space."""
    return f"host=localhost dbname=d user=u password={secret[:6]} {secret[6:]}"


def bad_percent_escape(secret: str) -> str:
    """A URI whose password holds an invalid percent escape."""
    return f"postgresql://u:{secret[:6]}%zz{secret[6:]}@localhost/d"


def unbalanced_quote(secret: str) -> str:
    """A key=value string with a quote that is never closed."""
    return f"host=localhost dbname=d user=u password='{secret}"


MALFORMED = pytest.mark.parametrize(
    "make_dsn",
    [unquoted_space, bad_percent_escape, unbalanced_quote],
)


def query_driver(dsn: str, run_migrations: bool = False) -> PsycopgDriver:
    """A driver whose pool gives up after one second, were it ever to wait."""
    key = PrimaryKeyColumn()
    return PsycopgDriver(
        dsn,
        "t",
        columns=[key, Column("v", "TEXT")],
        primary_key=key,
        run_migrations=run_migrations,
        timeout=1,
    )


def messages(error: Optional[BaseException]) -> list[str]:
    """The text of ``error`` and of every error in its cause/context chain."""
    texts: list[str] = []
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        texts.extend([str(error), repr(error), *map(str, error.args)])
        error = error.__cause__ or error.__context__
    return texts


def assert_nothing_leaked(
    error: BaseException,
    caplog: pytest.LogCaptureFixture,
    secret: str,
) -> None:
    """The error is the driver's fixed one, and no text holds the password."""
    assert isinstance(error, DatabaseConnectionError)
    assert "not valid" in str(error)
    assert "percent-encode" in str(error)
    pieces = (secret, secret[:6], secret[6:])
    for text in [*messages(error), *(r.getMessage() for r in caplog.records)]:
        assert not any(piece in text for piece in pieces), text
    assert error.__cause__ is None


@MALFORMED
@pytest.mark.anyio
async def test_when_the_string_cannot_be_parsed__then_the_pool_is_never_built(
    make_dsn: Callable[[str], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = word()
    driver = query_driver(make_dsn(secret))

    with (
        caplog.at_level(logging.DEBUG),
        pytest.raises(
            DatabaseConnectionError,
        ) as raised,
    ):
        async with driver, driver.connection():
            pass  # pragma: no cover

    assert_nothing_leaked(raised.value, caplog, secret)
    assert driver.pool is None
    await driver.on_shutdown()


@MALFORMED
@pytest.mark.anyio
async def test_when_the_query_driver_starts_on_a_bad_string__then_nothing_leaks(
    make_dsn: Callable[[str], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = word()
    driver = query_driver(make_dsn(secret), run_migrations=True)

    with (
        caplog.at_level(logging.DEBUG),
        pytest.raises(
            DatabaseConnectionError,
        ) as raised,
    ):
        await driver.on_startup()

    assert_nothing_leaked(raised.value, caplog, secret)
    assert driver.pool is None
    await driver.on_shutdown()


@MALFORMED
@pytest.mark.anyio
async def test_when_the_listen_driver_starts_on_a_bad_string__then_nothing_leaks(
    make_dsn: Callable[[str], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = word()
    driver = PsycopgListenDriver(make_dsn(secret), "channel")

    with (
        caplog.at_level(logging.DEBUG),
        pytest.raises(
            DatabaseConnectionError,
        ) as raised,
    ):
        await driver.on_startup()

    assert_nothing_leaked(raised.value, caplog, secret)
    assert driver.connection is None


@pytest.mark.anyio
async def test_when_the_string_is_valid__then_the_pool_is_built_closed() -> None:
    driver = query_driver(f"postgresql://u:{word()}@localhost:1/d")

    async with driver:
        pool = driver.pool

        # open=False stays: building the driver, or entering it, never connects.
        assert pool is not None
        assert pool.closed
    await driver.on_shutdown()
