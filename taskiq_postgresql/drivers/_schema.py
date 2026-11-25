"""
The table check the bundled query drivers run in ``on_startup``.

One read-only catalog query (``catalog_query``), compared with the table's
description (``plan_migration``), tells what the table lacks. A driver that
migrates skips the lock and the DDL when no step is pending; one that does
not migrate refuses to start without the table or one of its columns, and
logs what only slows the component down or needs a person.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from taskiq_postgresql.exceptions import SchemaNotReadyError
from taskiq_postgresql.migrations import (
    CatalogState,
    MigrationPlan,
    TableSchema,
    _driver_schema,
    catalog_query,
    plan_migration,
)

if TYPE_CHECKING:
    from taskiq_postgresql.abc.driver import QueryDriver

logger = logging.getLogger(__name__)


def described_table(driver: QueryDriver) -> Optional[TableSchema]:
    """
    Describe the table of ``driver`` from its own query builders.

    Args:
        driver: The query driver of a component.

    Returns:
        The description, or ``None`` when no component set the driver's
        ``component_kind``.
    """
    if driver.component_kind is None:
        return None
    return _driver_schema(driver.component_kind, driver)


def compare(schema: TableSchema, catalog: Optional[str]) -> MigrationPlan:
    """
    Compare the table with what ``catalog_query([schema])`` returned.

    Args:
        schema: The table's description.
        catalog: The query's only value; ``None`` if it returned no row.

    Returns:
        The plan of the table.

    Raises:
        ValueError: if ``catalog`` is not what the query returns.
    """
    return plan_migration([schema], CatalogState.from_json(catalog or ""))


async def check_table(driver: QueryDriver) -> None:
    """
    Check, without DDL, that the table of ``driver`` holds what it needs.

    It reads the catalog through ``fetch_row``, on the driver's own pool, and
    logs a warning for each missing index and each problem the component
    works with. Nothing happens without ``component_kind``.

    Args:
        driver: The query driver of a component that does not migrate.

    Raises:
        SchemaNotReadyError: when the table or a column is missing, or a
            problem no step fixes keeps the component from working.
    """
    schema = described_table(driver)
    if schema is None:
        return
    row = await driver.fetch_row(catalog_query([schema]))
    plan = compare(schema, None if row is None else row["catalog"])
    if not plan.is_ready:
        raise SchemaNotReadyError(schema.name, plan.pending, plan.problems)
    # Ready: every pending step lacks indexes only.
    for item in plan.pending:
        logger.warning(
            "taskiq-postgresql: %s: step %s has not fully run, missing indexes: "
            "%s; queries on the table can be slow until they exist. Create them "
            "with `taskiq postgresql migrate <module>:<object>` or with the SQL "
            "`taskiq postgresql plan <module>:<object>` prints",
            schema.name,
            item.step.number,
            ", ".join(item.missing_indexes),
        )
    for problem in plan.problems:
        logger.warning("taskiq-postgresql: %s: %s", problem.code, problem.detail)
