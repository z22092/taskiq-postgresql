from datetime import date, datetime
from typing import Any, Optional, Sequence, Union
from uuid import UUID

from taskiq_postgresql.abc.query import Column
from taskiq_postgresql.drivers._json import dumps


def date_bounds(
    from_date: Union[datetime, date],
    to_date: Optional[Union[datetime, date]],
) -> list[Union[datetime, date]]:
    """
    Return the bind parameters of ``DeleteByDateQuery``.

    Args:
        from_date: the inclusive lower bound.
        to_date: the inclusive upper bound, or ``None`` for none.

    Returns:
        One value without an upper bound, two with it, matching the query text
        built with ``upper_bound=to_date is not None``.
    """
    if to_date is None:
        return [from_date]
    return [from_date, to_date]


def encode_params(
    columns: Optional[Sequence[Column]],
    values: Optional[Sequence[Any]],
) -> Sequence[Any]:
    """
    Shape bind parameters for the asyncpg and psycopg drivers.

    A dict bound to a ``JSONB`` column becomes JSON text and a ``UUID`` bound
    to a ``UUID`` column becomes its hex text; every other value is passed
    through unchanged.

    Args:
        columns: the columns the values are bound to, in order, or ``None``
            to pass the values through.
        values: the values to bind, or ``None``.

    Returns:
        The values to send with the query.

    Raises:
        ValueError: if ``columns`` and ``values`` differ in length.
    """
    if not values:
        return []

    if not columns:
        return values

    new_values = [*values]

    for index, (column, value) in enumerate(zip(columns, values, strict=True)):
        if column.type.upper() == "JSONB" and isinstance(value, dict):
            new_values[index] = dumps(value)

        if column.type.upper() == "UUID" and isinstance(value, UUID):
            new_values[index] = value.hex

    return new_values
