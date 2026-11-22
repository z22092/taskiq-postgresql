from typing import Callable, Optional, Union

Dsn = Union[str, Callable[[], str]]

_EXAMPLE_DSN = "postgresql://user@localhost:5432/postgres"


def require_dsn(dsn: Optional[Dsn], component: str) -> Dsn:
    """
    Return ``dsn``, or fail when the caller did not pass one.

    There is no default DSN: a default would silently point a forgotten
    argument at whatever server answers on localhost. ``None`` is also what
    ``os.environ.get`` returns for an unset variable, so it is rejected the
    same way as an omitted argument.

    Args:
        dsn: the DSN the component received.
        component: the component's class name, used in the message.

    Returns:
        The DSN, unchanged.

    Raises:
        TypeError: if ``dsn`` is ``None``, like Python's own missing-argument
            error.
    """
    if dsn is None:
        msg = (
            f"{component}() missing required argument 'dsn': pass a PostgreSQL "
            "connection string or a callable that returns one, for example "
            f'{component}(dsn="{_EXAMPLE_DSN}")'
        )
        raise TypeError(msg)
    return dsn
