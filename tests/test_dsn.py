import inspect
import re
import uuid
from typing import Any, Callable, Optional, Union
from urllib.parse import urlsplit

import pytest
from taskiq import TaskiqResult

from taskiq_postgresql.broker import PostgresqlBroker
from taskiq_postgresql.exceptions import DatabaseConnectionError
from taskiq_postgresql.result_backend import PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource
from tests.helpers import closed_local_port, drop_and_shutdown

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

# Construction never connects, so the tests that only construct need no
# database.
DSN = "postgresql://postgres@localhost:5432/postgres"


class CountingDsn:
    """A callable DSN that records how often it is resolved."""

    def __init__(self, value: Optional[str]) -> None:
        self.value = value
        self.calls = 0

    def __call__(self) -> Optional[str]:
        """Return the configured value."""
        self.calls += 1
        return self.value


def assert_names_dsn_with_safe_example(message: str, component: Component) -> None:
    assert component.__name__ in message
    assert "'dsn'" in message
    example = re.search(r"postgresql://[^\s\"']+", message)
    assert example is not None, message
    assert urlsplit(example.group(0)).password is None, message


@pytest.fixture
def no_local_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point libpq-style defaults at a closed port, never at a real server."""
    monkeypatch.setenv("PGHOST", "127.0.0.1")
    monkeypatch.setenv("PGPORT", str(closed_local_port()))


@pytest.mark.parametrize("component", COMPONENTS)
def test_dsn_is_the_required_first_parameter(component: Component) -> None:
    parameters = list(inspect.signature(component).parameters.values())

    assert parameters[0].name == "dsn"
    assert parameters[0].default is inspect.Parameter.empty


@pytest.mark.parametrize("component", COMPONENTS)
@pytest.mark.parametrize(
    ("kwargs", "shows_example"),
    [
        # Python itself rejects the call, so the message is Python's.
        pytest.param({}, False, id="omitted"),
        # What ``os.environ.get`` returns for an unset variable.
        pytest.param({"dsn": None}, True, id="none"),
        pytest.param({"dsn": ""}, True, id="empty"),
        pytest.param({"dsn": "  "}, True, id="blank"),
    ],
)
def test_when_dsn_is_missing__then_construction_fails(
    component: Component,
    kwargs: dict[str, Any],
    shows_example: bool,
) -> None:
    with pytest.raises(TypeError) as error:
        component(**kwargs)

    message = str(error.value)
    assert component.__name__ in message
    assert "'dsn'" in message
    if shows_example:
        assert_names_dsn_with_safe_example(message, component)


@pytest.mark.parametrize("component", COMPONENTS)
def test_when_dsn_is_positional__then_it_is_used(component: Component) -> None:
    assert component(DSN).dsn == DSN


@pytest.mark.anyio
@pytest.mark.usefixtures("no_local_fallback")
@pytest.mark.parametrize("component", COMPONENTS)
@pytest.mark.parametrize("value", [None, "", "  "], ids=["none", "empty", "blank"])
async def test_when_dsn_callable_returns_nothing__then_startup_fails(
    component: Component,
    value: Optional[str],
) -> None:
    instance = component(dsn=CountingDsn(value))

    with pytest.raises(TypeError) as error:
        await instance.startup()

    assert_names_dsn_with_safe_example(str(error.value), component)


async def _use_broker(broker: PostgresqlBroker) -> None:
    rows = await broker.driver.execute(
        f"SELECT * FROM {broker.table_name}",  # noqa: S608
    )
    assert rows == []


async def _use_result_backend(backend: PostgresqlResultBackend[Any]) -> None:
    task_id = uuid.uuid4().hex
    result: TaskiqResult[str] = TaskiqResult(
        is_err=False,
        log=None,
        return_value="resolved",
        execution_time=0.1,
    )
    await backend.set_result(task_id, result)
    assert (await backend.get_result(task_id)).return_value == "resolved"


async def _use_scheduler_source(source: PostgresqlSchedulerSource) -> None:
    assert await source.get_schedules() == []


USES: dict[str, Callable[[Any], Any]] = {
    "PostgresqlBroker": _use_broker,
    "PostgresqlResultBackend": _use_result_backend,
    "PostgresqlSchedulerSource": _use_scheduler_source,
}


@pytest.mark.anyio
@pytest.mark.parametrize("component", COMPONENTS)
async def test_when_dsn_is_callable__then_startup_resolves_it_once(
    component: Component,
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    dsn = CountingDsn(postgresql_dsn)
    kwargs: dict[str, Any] = {"table_name": postgres_table, "driver": driver}
    if component is PostgresqlBroker:
        kwargs["channel_name"] = f"{postgres_table}_channel"
    instance = component(dsn=dsn, run_migrations=True, **kwargs)
    assert dsn.calls == 0

    await instance.startup()
    try:
        assert dsn.calls == 1
        assert instance.driver.connection_string == postgresql_dsn
        if isinstance(instance, PostgresqlBroker):
            assert instance.listen_driver.connection_string == postgresql_dsn
        await USES[component.__name__](instance)
    finally:
        await drop_and_shutdown(instance, postgres_table)


@pytest.mark.anyio
async def test_when_dsn_is_callable__then_drivers_do_not_connect_before_startup(
    driver: str,
) -> None:
    dsn = CountingDsn(DSN)
    backend: PostgresqlResultBackend[Any] = PostgresqlResultBackend(
        dsn=dsn,
        driver=driver,
    )

    with pytest.raises(DatabaseConnectionError, match=r"startup\(\)"):
        await backend.is_result_ready(uuid.uuid4().hex)
    assert dsn.calls == 0
