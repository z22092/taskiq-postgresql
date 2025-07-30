from typing import TYPE_CHECKING, Literal, Type, Union

if TYPE_CHECKING:
    from taskiq_postgresql.drivers import (
        AsyncpgDriver,
        PsqlpyDriver,
        PsycopgDriver,
    )


def get_db_driver(
    driver: Literal["asyncpg", "psqlpy", "psycopg"],
) -> Type[Union["AsyncpgDriver", "PsqlpyDriver", "PsycopgDriver"]]:  # type: ignore
    """Get the database driver."""
    try:
        if driver == "asyncpg":
            from taskiq_postgresql.drivers import AsyncpgDriver  # noqa: PLC0415

            return AsyncpgDriver
        if driver == "psqlpy":
            from taskiq_postgresql.drivers import PsqlpyDriver  # noqa: PLC0415

            return PsqlpyDriver
        if driver == "psycopg":
            from taskiq_postgresql.drivers import PsycopgDriver  # noqa: PLC0415

            return PsycopgDriver

    except ImportError:
        raise ImportError(
            f"{driver} is not installed. \
                Please install it with `pip install taskiq-postgresql[{driver}]`.",
        ) from None

    raise ValueError(f"Driver {driver} is not supported.")
