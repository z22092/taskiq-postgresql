"""
The tables of taskiq-postgresql on SQLModel's metadata.

Install it with ``pip install 'taskiq-postgresql[sqlmodel]'``. SQLModel keeps
its ``table=True`` models on ``SQLModel.metadata``, a SQLAlchemy ``MetaData``,
so the tables go there as SQLAlchemy ``Table`` objects, next to the models:
``SQLModel.metadata.create_all(engine)`` creates them, and Alembic sees them.
They are not models; read and write them through the components.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

try:
    from sqlmodel import SQLModel
except ImportError as error:
    raise ImportError(
        "sqlmodel is not installed. Please install it with "
        "`pip install taskiq-postgresql[sqlmodel]`.",
    ) from error

from taskiq_postgresql.contrib import sqlalchemy as sqlalchemy_contrib

if TYPE_CHECKING:
    from sqlalchemy import MetaData, Table

__all__ = ["add_taskiq_tables"]


def add_taskiq_tables(
    *targets: object,
    metadata: Optional[MetaData] = None,
    app_dir: Optional[str] = None,
) -> list[Table]:
    """
    Add the tables of taskiq-postgresql components to SQLModel's metadata.

    Args:
        *targets: As for ``taskiq_postgresql.contrib.sqlalchemy``'s
            ``add_taskiq_tables``: components, brokers, schedulers,
            ``"module:object"`` paths or ``TableSchema`` descriptions.
        metadata: The metadata to add the tables to; ``SQLModel.metadata`` by
            default.
        app_dir: A directory to add to ``sys.path`` before importing a path.

    Returns:
        The tables, in target order, each once.

    Raises:
        TypeError: if no target is given.
        ValueError: if the metadata holds a different table under one of the
            names, or a table name is not a plain SQL identifier.
        LookupError: if a target holds no component of this package.
        ImportError: if a path's module cannot be imported.
        AttributeError: if a path's module has no such object.
    """
    return sqlalchemy_contrib.add_taskiq_tables(
        SQLModel.metadata if metadata is None else metadata,
        *targets,
        app_dir=app_dir,
    )
