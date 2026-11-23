"""
The health check of a LISTEN connection, shared by the bundled listen drivers.

A NAT or a load balancer can drop a connection's packets without a FIN or RST:
the client still sees an open socket, no termination callback runs, and only a
round trip tells. The probe sends the same LISTEN again, which changes nothing
on the server. It must repeat exactly the statement that registered the
channel, quoting included, since a name spelled differently is another channel
and the probe would start listening on it.
"""

from collections.abc import Awaitable
from typing import Any, Callable

from taskiq_postgresql.exceptions import DatabaseConnectionError


async def check_listen(
    answer: Awaitable[Any],
    give_up: Callable[[], Awaitable[None]],
) -> None:
    """
    Await the probe's round trip; give the connection up if it fails.

    Args:
        answer: the probe, bounded by the driver's ``query_timeout``.
        give_up: closes the connection, so that nothing waits on it any more.

    Raises:
        DatabaseConnectionError: if the probe failed or got no answer in time.
    """
    try:
        await answer
    except Exception as error:
        await give_up()
        msg = f"the LISTEN connection failed its health check: {error!r}"
        raise DatabaseConnectionError(msg) from error
