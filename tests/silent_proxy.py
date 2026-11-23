"""A loopback TCP proxy that stands in for a network path that drops packets."""

import asyncio
import re
from dataclasses import dataclass
from types import TracebackType
from typing import Optional
from urllib.parse import urlsplit

from typing_extensions import Self

from tests.helpers import with_netloc

# LISTEN but not UNLISTEN, which asyncpg sends when a pooled connection returns.
_LISTEN = re.compile(rb"(?<!UN)LISTEN ")


@dataclass
class _Link:
    """One client connection through the proxy and its upstream connection."""

    client: asyncio.StreamWriter
    server: asyncio.StreamWriter
    listen_statements: int = 0
    silent: bool = False
    closed: bool = False


class SilentProxy:
    """
    Loopback TCP proxy in front of PostgreSQL that can make a path go silent.

    It forwards bytes both ways. ``silence()`` makes the chosen connections drop
    every byte without closing them, like a NAT that forgot the flow: the client
    still holds an open socket and never gets an answer, nor a FIN or RST. New
    connections keep working. ``refuse()`` ends every connection and rejects new
    ones until ``accept()``, like a server that cannot be reached.
    ``end_listens()`` ends every connection shortly after its client sends
    ``LISTEN``, like a server that drops each session soon after accepting it.

    A connection counts as a LISTEN connection once its client sent ``LISTEN``.
    """

    def __init__(self, dsn: str) -> None:
        """Proxy to the server ``dsn`` points at; ``self.dsn`` goes through it."""
        self._parts = urlsplit(dsn)
        self._links: list[_Link] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._server: Optional[asyncio.Server] = None
        self._refusing = False
        self._ending_listens = False
        self.dsn = ""

    async def __aenter__(self) -> Self:
        """Start listening on a free loopback port."""
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        self.dsn = with_netloc(
            self._parts,
            password=self._parts.password,
            address=f"127.0.0.1:{port}",
        ).geturl()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Close the port and every connection."""
        if self._server is not None:
            self._server.close()
        self._end_links()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    @property
    def listen_links(self) -> list[_Link]:
        """The open connections that sent LISTEN."""
        return [
            link for link in self._links if link.listen_statements and not link.closed
        ]

    def silence(self, *, listen: bool) -> int:
        """
        Silence the open LISTEN connections, or all the other open ones.

        Returns:
            How many connections went silent.
        """
        chosen = [
            link
            for link in self._links
            if not link.closed and bool(link.listen_statements) == listen
        ]
        for link in chosen:
            link.silent = True
        return len(chosen)

    def refuse(self) -> None:
        """End every connection and reject new ones."""
        self._refusing = True
        self._end_links()

    def accept(self) -> None:
        """Take new connections again."""
        self._refusing = False

    def end_listens(self) -> None:
        """End each connection, from now on, 0.2 s after its client sends LISTEN."""
        self._ending_listens = True

    def _end_links(self) -> None:
        for link in self._links:
            self._end_link(link)

    @staticmethod
    def _end_link(link: _Link) -> None:
        link.closed = True
        link.client.transport.abort()
        link.server.transport.abort()

    async def _accept(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        if self._refusing:
            client_writer.transport.abort()
            return
        hostname = self._parts.hostname or "localhost"
        server_reader, server_writer = await asyncio.open_connection(
            hostname,
            self._parts.port or 5432,
        )
        link = _Link(client=client_writer, server=server_writer)
        self._links.append(link)
        for source, target, upstream in (
            (client_reader, server_writer, True),
            (server_reader, client_writer, False),
        ):
            task = asyncio.create_task(
                self._pump(link, source, target, upstream=upstream),
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _pump(
        self,
        link: _Link,
        source: asyncio.StreamReader,
        target: asyncio.StreamWriter,
        *,
        upstream: bool,
    ) -> None:
        try:
            while data := await source.read(65536):
                if upstream:
                    listens = len(_LISTEN.findall(data))
                    link.listen_statements += listens
                    if listens and self._ending_listens:
                        asyncio.get_running_loop().call_later(
                            0.2,
                            self._end_link,
                            link,
                        )
                if link.silent:
                    continue
                target.write(data)
                await target.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            # The client leaving always closes the link; the server leaving
            # reaches the client only while the link is not silent.
            if upstream or not link.silent:
                link.closed = True
            if not link.silent:
                target.transport.abort()
