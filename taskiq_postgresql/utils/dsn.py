from typing import Callable, Final, Union

Dsn = Union[str, Callable[[], str]]
"""A PostgreSQL connection string, or a callable that returns one."""

_EXAMPLE_DSN = "postgresql://user@localhost:5432/postgres"

UNREADABLE_DSN: Final = (
    "the connection string is not valid: in a URL, percent-encode %, /, ?, # "
    "and @ in the password; in key=value form, quote a value that holds a space"
)
"""
What every driver raises, and the CLI prints, for a string it cannot read.

A parser's own message can quote part of the string, password included, so
the drivers raise this text instead and never chain the parser's error.
"""

_URI_SCHEMES: Final = ("postgresql://", "postgres://")


def _invalid_dsn(component: str, problem: str) -> TypeError:
    return TypeError(
        f"{component} needs a non-empty PostgreSQL connection string for 'dsn', "
        f"but {problem}. Pass the DSN, or a callable that returns it, for "
        f'example {component}(dsn="{_EXAMPLE_DSN}")',
    )


def _is_blank(value: object) -> bool:
    # libpq (psycopg) and asyncpg read an empty DSN as "use the local
    # defaults", so a missing value would silently reach another server.
    return not isinstance(value, str) or not value.strip()


def _describe(value: object) -> str:
    # Only None and a blank string are echoed. Anything else, such as a DSN
    # object or bytes, can carry the password in its repr().
    if value is None or (isinstance(value, str) and not value.strip()):
        return repr(value)
    return f"a {type(value).__name__}"


def is_ambiguous_uri(dsn: str) -> bool:
    """
    Tell whether a connection URI is one libpq and asyncpg would misread.

    A parser reads ``u:pa@ss@host`` as the password ``pa`` and the host
    ``ss@host``, and reads ``u:12/ss@h/db`` as the port ``12`` and the database
    ``ss@h/db``. The server's or the resolver's error then quotes the rest of
    the password, so such a URI is refused before any connection opens. A
    key=value string, a multi-host URI (``h1:5432,h2:5433``), an IPv6 host
    (``[::1]:5432``), an empty port and an ``@`` in the query are never
    refused.

    Args:
        dsn: the connection string.

    Returns:
        ``True`` for a ``postgresql://`` or ``postgres://`` URI (the scheme in
        any case) whose authority holds more than one ``@``, whose path holds
        an ``@``, or whose port is not all ASCII digits.
    """
    lowered = dsn.lower()
    for scheme in _URI_SCHEMES:
        if lowered.startswith(scheme):
            rest = dsn[len(scheme) :]
            break
    else:
        return False
    end = min(
        (i for i in (rest.find("/"), rest.find("?")) if i >= 0), default=len(rest)
    )
    authority, tail = rest[:end], rest[end:]
    # Only the path: the query is read apart, and its values may hold an "@".
    path = tail.partition("?")[0]
    if authority.count("@") > 1 or "@" in path:
        return True
    hosts = authority.rpartition("@")[2]
    return not all(_is_port(host) for host in hosts.split(","))


def _is_port(host: str) -> bool:
    # The port is what follows the last ":" outside an IPv6 literal's brackets.
    port = host.rpartition("]")[2] if "]" in host else host
    port = port.rpartition(":")[2] if ":" in port else ""
    return port == "" or (port.isascii() and port.isdigit())


def require_dsn(dsn: Dsn, component: str) -> Dsn:
    """
    Return ``dsn`` when it can be used, at construction time.

    ``None`` is what ``os.environ.get`` returns for an unset variable, so it
    is rejected like an empty string. A callable is checked by
    :func:`resolve_dsn` when the component starts.

    Args:
        dsn: the DSN the component received.
        component: the component's class name, used in the message.

    Returns:
        The DSN, unchanged.

    Raises:
        TypeError: if ``dsn`` is ``None``, empty, blank, or neither a string
            nor a callable.
    """
    if not callable(dsn) and _is_blank(dsn):
        raise _invalid_dsn(component, f"got {_describe(dsn)}")
    return dsn


def resolve_dsn(dsn: Dsn, component: str) -> str:
    """
    Return the connection string, calling ``dsn`` when it is a callable.

    Args:
        dsn: the DSN the component received.
        component: the component's class name, used in the message.

    Returns:
        A non-empty connection string.

    Raises:
        TypeError: if the callable returns ``None``, a non-string, or an
            empty or blank string.
    """
    # Tested as a str, not as a non-callable: a str subclass could be
    # callable, so only this test tells a type checker the other branch
    # holds the Callable[[], str].
    if isinstance(dsn, str):
        return dsn
    value = dsn()
    if _is_blank(value):
        raise _invalid_dsn(
            component,
            f"the 'dsn' callable returned {_describe(value)}",
        )
    return value


def dsn_before_startup(dsn: Dsn) -> str:
    """
    Return the connection string the drivers hold until startup.

    A callable is resolved only at startup, so until then the drivers hold an
    empty connection string, which they refuse to connect with.
    """
    return "" if callable(dsn) else dsn
