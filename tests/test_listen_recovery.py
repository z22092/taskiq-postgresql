"""
How a worker comes back from a lost or silent connection.

The LISTEN connection is reopened after the server ends it or after it stops
answering a health check; shutdown() and every claim stay bounded when the
connections go silent. ``SilentProxy`` stands in for a network path that drops
packets without a FIN or RST.
"""

import asyncio
import gc
import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from taskiq import BrokerMessage

from taskiq_postgresql import PostgresqlBroker
from taskiq_postgresql.abc.driver import ListenDriver
from taskiq_postgresql.broker import _ReopenWaits
from tests.helpers import (
    NOTIFY_DELIVERS_WITHIN,
    eventually,
    fetch_on_server,
    listen_backends,
    terminate_backends,
    with_sslmode_allow,
)
from tests.silent_proxy import SilentProxy

pytestmark = pytest.mark.anyio

LOGGER = "taskiq.asyncpg_broker"
# How often listen() checks a quiet LISTEN connection.
HEALTH_CHECK = 0.3
# Long enough for a healthy local server to answer on a busy machine, where a
# connect or a claim took over 0.5 s; short enough for a test to wait for a
# silent connection to be given up.
QUERY_TIMEOUT = 2.0
# Room on top of the waits a test adds up: psycopg takes up to 1 s to wake a
# wait it gave up, the connect or claim after a given-up one may take up to
# QUERY_TIMEOUT on a busy machine, and that machine schedules late.
SLACK = QUERY_TIMEOUT + 3.0
# A value no test waits for.
OUT_OF_THE_WAY = 60.0
FAST_SWEEP = 0.2
# How late a 0.05 s sleep may wake up while listen() runs beside it.
RESPONSIVE_WITHIN = 0.5
# The ceiling of each wait before reopening, as failures pile up.
REOPEN_CEILINGS = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0)
# Waits of at least 0.25, 0.5, 1 and 2 s leave room for four attempts in 3 s.
MOST_ATTEMPTS_IN_3_S = 4
MOST_STOPS_IN_5_S = 5
# The LISTENs a connection sends to start: psqlpy sends its own on the first
# read, after the one of on_startup(). Any more came from health checks.
STARTING_LISTENS = {"asyncpg": 1, "psqlpy": 2, "psycopg": 1}


def message_for(payload: bytes) -> BrokerMessage:
    """Build a message with no delay label."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="recovery_test",
        message=payload,
        labels={},
    )


def recovering_broker(
    dsn: str, table: str, driver: str, **kwargs: Any
) -> PostgresqlBroker:
    """A broker of its own channel and table, with the test's timing."""
    kwargs.setdefault("poll_interval", OUT_OF_THE_WAY)
    if driver == "asyncpg":
        # These tests cancel connects, see with_sslmode_allow().
        dsn = with_sslmode_allow(dsn)
    return PostgresqlBroker(
        dsn=dsn,
        channel_name=f"{table}_channel",
        table_name=table,
        driver=driver,
        run_migrations=True,
        **kwargs,
    )


def with_one_pooled_connection(broker: PostgresqlBroker, driver: str) -> None:
    """Keep the query pool small, so silencing its connections silences all of it."""
    sizes = {
        "asyncpg": {"min_size": 1, "max_size": 1},
        "psycopg": {"min_size": 1, "max_size": 1},
    }
    broker.driver.connection_kwargs = {
        **broker.driver.connection_kwargs,
        **sizes[driver],
    }


def messages(caplog: pytest.LogCaptureFixture, level: int, text: str) -> list[str]:
    """The logged messages of ``level`` that contain ``text``."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == level and text in record.getMessage()
    ]


@pytest.fixture
async def listening() -> AsyncGenerator[list[asyncio.Task[None]], None]:
    """Run listen() of brokers in the background; cancel them at the end."""
    tasks: list[asyncio.Task[None]] = []
    yield tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def collect(broker: PostgresqlBroker, received: list[bytes]) -> asyncio.Task[None]:
    """Start reading listen() of ``broker`` into ``received``."""

    async def read() -> None:
        async for delivered in broker.listen():
            received.append(delivered.data)

    return asyncio.create_task(read())


async def close(broker: PostgresqlBroker, dsn: str) -> None:
    """Shut the broker down and drop its table through a direct connection."""
    try:
        await asyncio.wait_for(broker.shutdown(), timeout=2 * QUERY_TIMEOUT + SLACK)
    finally:
        await fetch_on_server(dsn, f"DROP TABLE IF EXISTS {broker.table_name}")


def drawn_under(wait: float, ceiling: float) -> bool:
    """Whether ``wait`` lies between half of ``ceiling`` and all of it."""
    return ceiling / 2 <= wait <= ceiling


def test_when_reopening_keeps_failing__then_each_wait_doubles_up_to_30_s() -> None:
    waits = _ReopenWaits()
    for ceiling in REOPEN_CEILINGS:
        assert drawn_under(waits.next_wait(), ceiling)
        waits.failed()


def test_when_a_reopened_connection_lasts_30_s__then_the_waits_start_over() -> None:
    waits = _ReopenWaits()
    for _ in range(5):
        waits.failed()
    waits.served(30.0)
    assert drawn_under(waits.next_wait(), REOPEN_CEILINGS[0])


def test_when_a_reopened_connection_stops_sooner__then_it_counts_as_a_failure() -> None:
    waits = _ReopenWaits()
    waits.failed()
    waits.served(29.9)
    assert drawn_under(waits.next_wait(), REOPEN_CEILINGS[2])


async def test_when_the_server_ends_the_listen_connection__then_listen_reopens_it(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """The drop itself is noticed (no health check runs) and NOTIFY delivers again."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    worker = recovering_broker(
        postgresql_dsn,
        postgres_table,
        driver,
        health_check_interval=OUT_OF_THE_WAY,
    )
    message = message_for(b"after-reopen")
    received: list[bytes] = []
    await worker.startup()
    try:
        listening.append(collect(worker, received))
        await eventually(
            lambda: _listening(postgresql_dsn, worker.channel_name, 1),
            timeout=3,
        )
        await terminate_backends(
            postgresql_dsn,
            await listen_backends(postgresql_dsn, worker.channel_name),
        )
        await eventually(
            lambda: bool(messages(caplog, logging.INFO, "reopened")),
            timeout=5,
        )
        # The sweep is out of the way, so only a NOTIFY can deliver this.
        await worker.kick(message)
        await eventually(lambda: received == [message.message], NOTIFY_DELIVERS_WITHIN)
        sessions = await listen_backends(postgresql_dsn, worker.channel_name)
    finally:
        await close(worker, postgresql_dsn)

    assert len(sessions) == 1
    assert len(messages(caplog, logging.WARNING, "LISTEN on channel")) == 1
    assert len(messages(caplog, logging.INFO, "reopened")) == 1


async def _listening(dsn: str, channel: str, count: int) -> bool:
    return len(await listen_backends(dsn, channel)) == count


async def test_when_the_listen_path_goes_silent__then_the_health_check_reopens_it(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """A LISTEN socket that never answers is found by the health check and replaced."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    message = message_for(b"after-silence")
    received: list[bytes] = []
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            health_check_interval=HEALTH_CHECK,
            query_timeout=QUERY_TIMEOUT,
        )
        await worker.startup()
        try:
            listening.append(collect(worker, received))
            await eventually(lambda: len(proxy.listen_links) == 1, timeout=3)
            assert proxy.silence(listen=True) == 1
            await eventually(
                lambda: bool(messages(caplog, logging.INFO, "reopened")),
                timeout=HEALTH_CHECK + QUERY_TIMEOUT + SLACK,
            )
            await worker.kick(message)
            await eventually(
                lambda: received == [message.message],
                NOTIFY_DELIVERS_WITHIN,
            )
        finally:
            await close(worker, postgresql_dsn)

    assert len(messages(caplog, logging.WARNING, "LISTEN on channel")) == 1


async def test_when_every_connection_goes_silent__then_shutdown_returns_in_time(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    """shutdown() closes silent sockets by force once query_timeout runs out."""
    loop = asyncio.get_running_loop()
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            query_timeout=QUERY_TIMEOUT,
        )
        await worker.startup()
        try:
            await worker.driver.fetch_row("SELECT 1 AS one")
            proxy.silence(listen=True)
            proxy.silence(listen=False)
            started = loop.time()
            stopping = asyncio.ensure_future(worker.shutdown())
            done, _ = await asyncio.wait({stopping}, timeout=4 * QUERY_TIMEOUT + 2)
            took = loop.time() - started
        finally:
            await fetch_on_server(
                postgresql_dsn,
                f"DROP TABLE IF EXISTS {worker.table_name}",
            )

    assert done == {stopping}, f"shutdown() still running after {took:.1f} s"
    assert stopping.exception() is None


async def test_when_the_pool_goes_silent__then_listen_keeps_its_pace(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """A connection that stops answering, in a claim or its release, is given up."""
    if driver == "psqlpy":
        pytest.skip("psqlpy claims are not bounded by query_timeout, see README")
    loop = asyncio.get_running_loop()
    message = message_for(b"after-pool-silence")
    received: list[bytes] = []
    producer = recovering_broker(postgresql_dsn, postgres_table, driver)
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            poll_interval=FAST_SWEEP,
            query_timeout=QUERY_TIMEOUT,
        )
        with_one_pooled_connection(worker, driver)
        await worker.startup()
        await producer.startup()
        try:
            listening.append(collect(worker, received))
            await asyncio.sleep(0.5)
            assert proxy.silence(listen=False) >= 1
            await producer.kick(message)

            async def slowest_tick() -> float:
                slowest = 0.0
                # Until the stuck claim is given up and the pool is back.
                for _ in range(round((QUERY_TIMEOUT + 1.0) / 0.05)):
                    started = loop.time()
                    await asyncio.sleep(0.05)
                    slowest = max(slowest, loop.time() - started)
                return slowest

            assert await slowest_tick() < RESPONSIVE_WITHIN
            # Silenced while a claim waits for its answer, or after the answer
            # while asyncpg resets the connection for the pool: either way the
            # connection is given up.
            await eventually(
                lambda: bool(
                    messages(caplog, logging.ERROR, "got no answer")
                    or messages(caplog, logging.WARNING, "Releasing a connection"),
                ),
                timeout=2 * QUERY_TIMEOUT + SLACK,
            )
            await eventually(
                lambda: received == [message.message],
                timeout=2 * QUERY_TIMEOUT + SLACK,
            )
        finally:
            await producer.shutdown()
            await close(worker, postgresql_dsn)


async def test_when_the_server_cannot_be_reached__then_reopening_backs_off(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """Failed attempts are spaced out and logged once each; LISTEN comes back after."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    message = message_for(b"after-outage")
    received: list[bytes] = []
    # The worker's own pool may hand out a connection the outage broke (psycopg
    # does not check on checkout), so the message comes from elsewhere and a
    # fast sweep retries a claim that fails on one.
    producer = recovering_broker(postgresql_dsn, postgres_table, driver)
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            poll_interval=FAST_SWEEP,
            health_check_interval=HEALTH_CHECK,
            query_timeout=QUERY_TIMEOUT,
        )
        await worker.startup()
        await producer.startup()
        try:
            listening.append(collect(worker, received))
            await eventually(lambda: len(proxy.listen_links) == 1, timeout=3)
            proxy.refuse()
            await asyncio.sleep(3)
            failures = messages(caplog, logging.WARNING, "Could not reopen")
            proxy.accept()
            await eventually(
                lambda: bool(messages(caplog, logging.INFO, "reopened")),
                timeout=HEALTH_CHECK + QUERY_TIMEOUT + 5,
            )
            await producer.kick(message)
            await eventually(
                lambda: received == [message.message],
                NOTIFY_DELIVERS_WITHIN,
            )
        finally:
            await producer.shutdown()
            await close(worker, postgresql_dsn)

    assert 1 <= len(failures) <= MOST_ATTEMPTS_IN_3_S


async def test_when_listen_dies_right_after_each_reopen__then_the_waits_keep_growing(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """A connection that dies at once does not reset the wait to its shortest."""
    if driver != "asyncpg":
        pytest.skip("the waits are the broker's, alike for every driver")
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            health_check_interval=HEALTH_CHECK,
            query_timeout=QUERY_TIMEOUT,
        )
        await worker.startup()
        try:
            listening.append(collect(worker, []))
            await eventually(lambda: len(proxy.listen_links) == 1, timeout=3)
            # The next health check, and each reopen, sends LISTEN.
            proxy.end_listens()
            await asyncio.sleep(5)
            stops = messages(caplog, logging.WARNING, "LISTEN on channel")
        finally:
            await close(worker, postgresql_dsn)

    # Each stop costs the wait plus the 0.2 s the connection lives: at least
    # 0.45, 0.7, 1.2 and 2.2 s, so at most five stops fit in 5 s. Waits reset
    # to the shortest would fit about ten.
    assert 1 <= len(stops) <= MOST_STOPS_IN_5_S


async def test_when_the_broker_shuts_down_while_reopening__then_nothing_is_left(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """shutdown() stops the reopen loop at once and no LISTEN comes back after it."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    loop = asyncio.get_running_loop()
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            health_check_interval=HEALTH_CHECK,
            query_timeout=QUERY_TIMEOUT,
        )
        await worker.startup()
        try:
            task = collect(worker, [])
            listening.append(task)
            await eventually(lambda: len(proxy.listen_links) == 1, timeout=3)
            proxy.refuse()
            await eventually(
                lambda: bool(messages(caplog, logging.WARNING, "LISTEN on channel")),
                timeout=HEALTH_CHECK + QUERY_TIMEOUT + 3,
            )
            readers = set(worker._listen_readers)
            started = loop.time()
            await asyncio.wait_for(worker.shutdown(), timeout=QUERY_TIMEOUT + 2)
            took = loop.time() - started
            readers_done = [reader.done() for reader in readers]
            proxy.accept()
            await asyncio.wait_for(task, timeout=2)
            await asyncio.sleep(1)
            links_after = len(proxy.listen_links)
        finally:
            await fetch_on_server(
                postgresql_dsn,
                f"DROP TABLE IF EXISTS {worker.table_name}",
            )

    assert took < QUERY_TIMEOUT + 1
    assert readers_done == [True]
    assert links_after == 0
    assert messages(caplog, logging.INFO, "reopened") == []


class StuckListenDriver(ListenDriver):
    """A listen driver whose iterator ends at once and that never reopens."""

    def __init__(self) -> None:
        super().__init__(connection_string="postgresql://unused", channel_name="x")
        self.reopening = asyncio.Event()
        self.cancelled = False
        self._never = asyncio.Event()

    async def on_startup(self) -> None:
        """Wait for good, and record the cancellation that ends the wait."""
        self.reopening.set()
        try:
            await self._never.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    async def on_shutdown(self) -> None:
        """Nothing to close."""

    async def __aiter__(self) -> AsyncGenerator[str, None]:
        """Yield nothing, as a connection that dropped right away."""
        for payload in ():
            yield payload


async def test_when_shutdown_meets_a_reopen_in_progress__then_it_cancels_it(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    """shutdown() cancels an on_startup() that would never return on its own."""
    if driver != "asyncpg":
        pytest.skip("the reopen is the broker's, alike for every driver")
    loop = asyncio.get_running_loop()
    # Out of the way, so that only shutdown() can end the reopen.
    worker = recovering_broker(
        postgresql_dsn,
        postgres_table,
        driver,
        query_timeout=OUT_OF_THE_WAY,
    )
    await worker.startup()
    # The consumer holds the first message and asks for no other, as a worker
    # busy with a task does, so listen() itself never gets to end the reader.
    deliveries = worker.listen()
    try:
        await worker.kick(message_for(b"held"))
        await worker.listen_driver.on_shutdown()
        stuck = StuckListenDriver()
        worker.listen_driver = stuck
        await asyncio.wait_for(deliveries.__anext__(), timeout=3)
        await asyncio.wait_for(stuck.reopening.wait(), timeout=3)
        readers = set(worker._listen_readers)
        started = loop.time()
        await asyncio.wait_for(worker.shutdown(), timeout=QUERY_TIMEOUT + 2)
        took = loop.time() - started
        readers_done = [reader.done() for reader in readers]
    finally:
        await deliveries.aclose()
        await fetch_on_server(
            postgresql_dsn,
            f"DROP TABLE IF EXISTS {worker.table_name}",
        )

    assert took < QUERY_TIMEOUT + 1
    assert stuck.cancelled
    assert readers_done == [True]
    assert worker._listen_readers == set()


async def test_when_a_reopen_hangs__then_it_is_given_up_after_query_timeout(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    caplog: pytest.LogCaptureFixture,
    listening: list[asyncio.Task[None]],
) -> None:
    """An on_startup() that never returns counts as a failed attempt."""
    if driver != "asyncpg":
        pytest.skip("the reopen is the broker's, alike for every driver")
    worker = recovering_broker(
        postgresql_dsn,
        postgres_table,
        driver,
        query_timeout=QUERY_TIMEOUT,
    )
    await worker.startup()
    try:
        await worker.listen_driver.on_shutdown()
        stuck = StuckListenDriver()
        worker.listen_driver = stuck
        listening.append(collect(worker, []))
        await eventually(
            lambda: bool(messages(caplog, logging.WARNING, "Could not reopen")),
            timeout=QUERY_TIMEOUT + SLACK,
        )
    finally:
        await close(worker, postgresql_dsn)

    assert stuck.cancelled


async def test_when_a_started_broker_shuts_down_twice__then_both_succeed(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    """The second shutdown() finds the pool and the LISTEN connection closed."""
    worker = PostgresqlBroker(
        dsn=postgresql_dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
        driver=driver,
        run_migrations=True,
    )
    await worker.startup()
    try:
        await worker.shutdown()
        await worker.shutdown()
    finally:
        await fetch_on_server(postgresql_dsn, f"DROP TABLE IF EXISTS {postgres_table}")

    assert getattr(worker.driver, "pool", None) is None
    assert getattr(worker.listen_driver, "connection", None) is None


async def test_when_listen_is_health_checked__then_each_notify_is_delivered_once(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
    listening: list[asyncio.Task[None]],
) -> None:
    """Health checks between NOTIFYs lose none and keep LISTEN in pg_stat_activity."""
    sent = [message_for(f"probe-{index}".encode()) for index in range(20)]
    received: list[bytes] = []
    async with SilentProxy(postgresql_dsn) as proxy:
        worker = recovering_broker(
            proxy.dsn,
            postgres_table,
            driver,
            health_check_interval=0.05,
            query_timeout=QUERY_TIMEOUT,
        )
        await worker.startup()
        try:
            listening.append(collect(worker, received))
            await eventually(lambda: len(proxy.listen_links) == 1, timeout=3)
            for message in sent:
                await worker.kick(message)
                await asyncio.sleep(0.03)
            await eventually(lambda: len(received) == len(sent), timeout=5)
            await asyncio.sleep(0.3)
            sessions = await listen_backends(postgresql_dsn, worker.channel_name)
            statements = proxy.listen_links[0].listen_statements
        finally:
            await close(worker, postgresql_dsn)

    assert sorted(received) == sorted(message.message for message in sent)
    assert len(sessions) == 1
    assert statements > STARTING_LISTENS[driver] + 2


async def test_when_a_health_check_is_cancelled_twice__then_no_error_is_left_unread(
    postgresql_dsn: str,
    postgres_table: str,
    driver: str,
) -> None:
    """
    A second cancellation during a given-up probe leaves no task error unread.

    shutdown() cancels the reader, and the end of listen() cancels it again,
    often while it still waits for the probe it gave up.
    """
    if driver != "psycopg":
        pytest.skip("only psycopg waits for its probe in a task of its own")
    loop = asyncio.get_running_loop()
    unread: list[str] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _, context: unread.append(
            f"{context['message']}: {context.get('exception')!r}",
        ),
    )
    try:
        async with SilentProxy(postgresql_dsn) as proxy:
            worker = recovering_broker(
                proxy.dsn,
                postgres_table,
                driver,
                health_check_interval=0.05,
                # Out of the way, so that only the cancellations end the probe.
                query_timeout=OUT_OF_THE_WAY,
            )
            await worker.startup()
            try:
                reading = asyncio.ensure_future(anext(aiter(worker.listen_driver)))
                await eventually(lambda: len(proxy.listen_links) == 1, timeout=3)
                link = proxy.listen_links[0]
                proxy.silence(listen=True)
                sent = link.listen_statements
                await eventually(lambda: link.listen_statements > sent, timeout=3)
                reading.cancel()
                # Lets the first cancellation run into _give_up(), so that the
                # second one stops it there; without this both land at once.
                await asyncio.sleep(0)
                reading.cancel()
                await asyncio.gather(reading, return_exceptions=True)
                # The probe ends on the socket shut down by the first cancellation.
                await asyncio.sleep(0.5)
                del reading
                # asyncio reports an unread error once the task is collected,
                # which has to happen while the handler above is installed.
                gc.collect()
            finally:
                await close(worker, postgresql_dsn)
    finally:
        loop.set_exception_handler(previous)

    assert unread == []
