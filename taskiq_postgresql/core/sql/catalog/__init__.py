"""
The SQL catalog: every statement and script the package sends (ADR 0007).

Each builder's ``make_query`` returns its SQL text untyped, so a third-party
driver that passes a column no ``ParamType`` covers keeps working; ``statement``
(or ``script`` for DDL) wraps the same text with the types the server infers
for it. The modules split the catalog by what the SQL does; this package
re-exports every name, so ``taskiq_postgresql.core.sql.catalog`` is the one
place to import them from.
"""

from taskiq_postgresql.core.sql.catalog.base import QueryBase, _sql_literal
from taskiq_postgresql.core.sql.catalog.columns import (
    ClaimColumns,
    Column,
    CreatedAtColumn,
    NotBeforeColumn,
    PrimaryKeyColumn,
    UpdatedAtColumn,
)
from taskiq_postgresql.core.sql.catalog.ddl import (
    AddColumnsQuery,
    AddedIndex,
    CreateIndexQuery,
    CreateTableQuery,
    DropNotNullQuery,
    RelationsExistQuery,
    check_table_objects,
    drop_table_script,
    index_name,
    table_object_names,
)
from taskiq_postgresql.core.sql.catalog.introspection import (
    CatalogTable,
    catalog_read,
    database_read,
)
from taskiq_postgresql.core.sql.catalog.locks import (
    MAX_LOCK_TIMEOUT,
    MIGRATION_LOCK_NAMESPACE,
    SAVED_LOCK_TIMEOUT,
    LockTimeoutScripts,
    MigrationLockQuery,
    check_lock_timeout,
    in_transaction,
    lock_timeout_scripts,
    migration_lock_script,
)
from taskiq_postgresql.core.sql.catalog.rows import (
    ClaimByIdQuery,
    ClaimDueQuery,
    DeleteByDateQuery,
    DeleteQuery,
    ExistsQuery,
    InsertOrUpdateQuery,
    InsertQuery,
    SelectQuery,
    listen_script,
    notify_statement,
)

__all__ = [
    "MAX_LOCK_TIMEOUT",
    "MIGRATION_LOCK_NAMESPACE",
    "SAVED_LOCK_TIMEOUT",
    "AddColumnsQuery",
    "AddedIndex",
    "CatalogTable",
    "ClaimByIdQuery",
    "ClaimColumns",
    "ClaimDueQuery",
    "Column",
    "CreateIndexQuery",
    "CreateTableQuery",
    "CreatedAtColumn",
    "DeleteByDateQuery",
    "DeleteQuery",
    "DropNotNullQuery",
    "ExistsQuery",
    "InsertOrUpdateQuery",
    "InsertQuery",
    "LockTimeoutScripts",
    "MigrationLockQuery",
    "NotBeforeColumn",
    "PrimaryKeyColumn",
    "QueryBase",
    "RelationsExistQuery",
    "SelectQuery",
    "UpdatedAtColumn",
    "_sql_literal",
    "catalog_read",
    "check_lock_timeout",
    "check_table_objects",
    "database_read",
    "drop_table_script",
    "in_transaction",
    "index_name",
    "listen_script",
    "lock_timeout_scripts",
    "migration_lock_script",
    "notify_statement",
    "table_object_names",
]
