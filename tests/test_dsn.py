import re
from typing import Any, Union
from urllib.parse import urlsplit

import pytest

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

Component = Union[
    type[PostgresqlBroker],
    type[PostgresqlResultBackend[Any]],
    type[PostgresqlSchedulerSource],
]

COMPONENTS = [
    pytest.param(PostgresqlBroker, id="broker"),
    pytest.param(PostgresqlResultBackend, id="result_backend"),
    pytest.param(PostgresqlSchedulerSource, id="scheduler_source"),
]

# Construction never connects, so these tests need no database.
DSN = "postgresql://postgres@localhost:5432/postgres"


@pytest.mark.parametrize("component", COMPONENTS)
@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({}, id="omitted"),
        # What ``os.environ.get`` returns for an unset variable.
        pytest.param({"dsn": None}, id="none"),
    ],
)
def test_when_dsn_is_missing__then_construction_fails(
    component: Component,
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(TypeError) as error:
        component(**kwargs)

    message = str(error.value)
    assert component.__name__ in message
    assert "'dsn'" in message
    example = re.search(r"postgresql://[^\s\"']+", message)
    assert example is not None, message
    assert urlsplit(example.group(0)).password is None, message


@pytest.mark.parametrize("component", COMPONENTS)
def test_when_dsn_is_positional__then_it_is_used(component: Component) -> None:
    assert component(DSN).dsn == DSN
