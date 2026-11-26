"""Reading a database's catalog: what the tables and indexes look like now."""

from typing import Sequence

from taskiq_postgresql.core.sql.catalog.base import _sql_literal
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.sql.statement import ParamType, Statement

CatalogTable = tuple[Identifier, Sequence[str]]
"""A table to read, and the names of the indexes on it, as configured."""


def _table_state_sql(table: Identifier, index_names: Sequence[str]) -> str:
    # Names go in as escaped literals: the query has no bind parameters, so
    # every driver runs it the same way.
    name = _sql_literal(table.configured)
    indexes = ", ".join(_index_state_sql(index) for index in index_names)
    return (
        "(SELECT json_build_object("  # noqa: S608
        f"'name', {name}, "
        "'relkind', c.relkind, "
        "'columns', ac.columns, "
        # Beside the columns, not inside them: the pairs keep their shape.
        "'not_null', ac.not_null, "
        f"'indexes', json_build_array({indexes})) "
        f"FROM (SELECT to_regclass({name}) AS oid) AS t "
        "LEFT JOIN pg_class c ON c.oid = t.oid "
        # One scan of pg_attribute for both. An aggregate without GROUP BY
        # yields one row even for none, with NULL, as the subqueries did.
        "LEFT JOIN LATERAL (SELECT json_agg(json_build_array(a.attname,"
        " format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum) AS columns,"
        " json_agg(a.attname ORDER BY a.attnum) FILTER (WHERE a.attnotnull)"
        " AS not_null FROM pg_attribute a WHERE a.attrelid = c.oid"
        " AND a.attnum > 0 AND NOT a.attisdropped) AS ac ON true)"
    )


def _index_state_sql(index_name: str) -> str:
    # Correlated with the table's pg_class row (c) of _table_state_sql.
    name = _sql_literal(index_name)
    return (
        "(SELECT json_build_object("  # noqa: S608
        f"'name', {name}, "
        "'relkind', ic.relkind, "
        "'on_table', i.indrelid IS NOT DISTINCT FROM c.oid, "
        "'valid', i.indisvalid, "
        "'method', am.amname, "
        "'unique', i.indisunique, "
        "'partial', i.indpred IS NOT NULL, "
        "'columns', (SELECT json_agg(a.attname ORDER BY k.n)"
        " FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, n)"
        " LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid"
        " AND a.attnum = k.attnum WHERE k.n <= i.indnkeyatts)) "
        f"FROM (SELECT to_regclass({name}) AS oid) AS r "
        "LEFT JOIN pg_class ic ON ic.oid = r.oid "
        "LEFT JOIN pg_index i ON i.indexrelid = r.oid "
        "LEFT JOIN pg_am am ON am.oid = ic.relam)"
    )


def catalog_read(tables: Sequence[CatalogTable]) -> Statement:
    """
    Return the read-only query that reads what a database holds for ``tables``.

    The query has no parameters: the names are written in it as literals, so
    every driver runs it as it is. It returns one row with one text column,
    ``catalog``. Names resolve through the ``search_path``, as the components'
    own SQL does, and the query reads only the ``pg_catalog`` tables.

    Args:
        tables: Each table, with the names of its indexes as configured.

    Returns:
        The statement.

    Raises:
        ValueError: if ``tables`` is empty.
    """
    if not tables:
        raise ValueError("a catalog query needs at least one table schema")
    return Statement(
        "SELECT json_build_object('tables', json_build_array("
        + ", ".join(_table_state_sql(table, names) for table, names in tables)
        + "))::text AS catalog",
        (),
        (("catalog", ParamType.TEXT),),
    )


def database_read(tables: Sequence[CatalogTable]) -> Statement:
    """
    Return the query that reads the catalog and what names the database.

    Args:
        tables: Each table, with the names of its indexes as configured.

    Returns:
        One row: ``database``, ``search_path`` and ``catalog``, all text.
    """
    catalog = catalog_read(tables)
    return Statement(
        "SELECT current_database()::text AS database,"
        " current_schemas(false)::text AS search_path,"
        f" ({catalog.text}) AS catalog",
        (),
        (
            ("database", ParamType.TEXT),
            ("search_path", ParamType.TEXT),
            ("catalog", ParamType.TEXT),
        ),
    )
