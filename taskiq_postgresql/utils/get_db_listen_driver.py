from typing import TYPE_CHECKING, Literal, Type, Union

if TYPE_CHECKING:
    from taskiq_postgresql.drivers import (
        AsyncpgListenDriver,
        PsqlpyListenDriver,
        PsycopgListenDriver,
    )


def get_db_listen_driver(
    driver: Literal["asyncpg", "psqlpy", "psycopg"],
) -> Type[
    Union[
        "AsyncpgListenDriver",
        "PsqlpyListenDriver",
        "PsycopgListenDriver",
    ]
]:  # type: ignore
    """Get the database driver."""
    try:
        if driver == "asyncpg":
            from taskiq_postgresql.drivers import AsyncpgListenDriver  # noqa: PLC0415

            return AsyncpgListenDriver
        if driver == "psqlpy":
            from taskiq_postgresql.drivers import PsqlpyListenDriver  # noqa: PLC0415

            return PsqlpyListenDriver
        if driver == "psycopg":
            from taskiq_postgresql.drivers import PsycopgListenDriver  # noqa: PLC0415

            return PsycopgListenDriver
    except ImportError:
        raise ImportError(
            f"{driver} is not installed. \
                Please install it with `pip install taskiq-postgresql[{driver}]`.",
        ) from None

    raise ValueError(f"Driver {driver} is not supported.")
