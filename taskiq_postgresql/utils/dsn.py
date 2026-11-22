from typing import Callable, Union

Dsn = Union[str, Callable[[], str]]
"""A PostgreSQL connection string, or a callable that returns one."""

_EXAMPLE_DSN = "postgresql://user@localhost:5432/postgres"


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
        TypeError: if ``dsn`` is ``None``, empty or blank.
    """
    if not callable(dsn) and _is_blank(dsn):
        raise _invalid_dsn(component, f"got {dsn!r}")
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
    if not callable(dsn):
        return dsn
    value = dsn()
    if _is_blank(value):
        raise _invalid_dsn(component, f"the 'dsn' callable returned {value!r}")
    return value


def dsn_before_startup(dsn: Dsn) -> str:
    """
    Return the connection string the drivers hold until startup.

    A callable is resolved only at startup, so until then the drivers hold an
    empty connection string, which they refuse to connect with.
    """
    return "" if callable(dsn) else dsn
