"""Unit tests for the driver lookup in ``taskiq_postgresql.utils``."""

import sys
from types import ModuleType
from typing import Any, Callable

import pytest

from taskiq_postgresql.utils import get_db_driver, get_db_listen_driver

Lookup = Callable[[Any], type]

LOOKUPS = [
    pytest.param(get_db_driver, id="query"),
    pytest.param(get_db_listen_driver, id="listen"),
]
CLASS_NAMES = {
    (get_db_driver, "asyncpg"): "AsyncpgDriver",
    (get_db_driver, "psqlpy"): "PsqlpyDriver",
    (get_db_driver, "psycopg"): "PsycopgDriver",
    (get_db_listen_driver, "asyncpg"): "AsyncpgListenDriver",
    (get_db_listen_driver, "psqlpy"): "PsqlpyListenDriver",
    (get_db_listen_driver, "psycopg"): "PsycopgListenDriver",
}


@pytest.mark.parametrize("lookup", LOOKUPS)
def test_each_name_returns_its_driver_class(lookup: Lookup, driver: str) -> None:
    assert lookup(driver).__name__ == CLASS_NAMES[lookup, driver]


@pytest.mark.parametrize("lookup", LOOKUPS)
def test_unknown_name_is_rejected(lookup: Lookup) -> None:
    with pytest.raises(ValueError, match="Driver pg8000 is not supported"):
        lookup("pg8000")


@pytest.mark.parametrize("lookup", LOOKUPS)
def test_missing_package_names_the_extra_to_install(
    lookup: Lookup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # taskiq_postgresql.drivers only defines the classes of installed driver
    # packages; an empty module stands in for a machine without psycopg.
    monkeypatch.setitem(
        sys.modules,
        "taskiq_postgresql.drivers",
        ModuleType("taskiq_postgresql.drivers"),
    )

    with pytest.raises(ImportError, match=r"taskiq-postgresql\[psycopg\]"):
        lookup("psycopg")
