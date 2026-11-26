"""
Unit tests for the SQL builders in ``taskiq_postgresql.abc.query``.

They need no database: each builder is a pure function of its table name and
columns, so the expected SQL is written out in full.
"""

import pytest

from taskiq_postgresql.abc.query import (
    ClaimByIdQuery,
    ClaimColumns,
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    DeleteByDateQuery,
    DeleteQuery,
    DropNotNullQuery,
    InsertOrUpdateQuery,
    InsertQuery,
    NotBeforeColumn,
    PrimaryKeyColumn,
    SelectQuery,
    UpdatedAtColumn,
)

TASK_ID = Column("task_id", "UUID")
RESULT = Column("result", "BYTEA")
MESSAGE = Column("message", "BYTEA")
CLAIM_COLUMNS = ClaimColumns(
    channel=Column("channel", "VARCHAR", nullable=True),
    not_before=NotBeforeColumn(),
)


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        pytest.param(Column("name", "VARCHAR"), "name VARCHAR NOT NULL", id="plain"),
        pytest.param(
            Column("labels", "JSONB", nullable=True),
            "labels JSONB",
            id="nullable",
        ),
        pytest.param(
            Column("attempts", "INTEGER", default=0),
            "attempts INTEGER NOT NULL DEFAULT 0",
            id="default",
        ),
        pytest.param(
            Column("id", "UUID", primary_key=True),
            "id UUID NOT NULL PRIMARY KEY",
            id="primary-key",
        ),
        pytest.param(
            PrimaryKeyColumn(),
            "id UUID NOT NULL PRIMARY KEY",
            id="primary-key-column-defaults",
        ),
        pytest.param(
            PrimaryKeyColumn(name="id", type_="SERIAL"),
            "id SERIAL NOT NULL PRIMARY KEY",
            id="primary-key-column-serial",
        ),
        pytest.param(
            CreatedAtColumn(),
            "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
            id="created-at",
        ),
        pytest.param(
            UpdatedAtColumn(),
            "updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
            id="updated-at",
        ),
    ],
)
def test_column_definition(column: Column, expected: str) -> None:
    assert column.make_query() == expected


def test_create_table_lists_columns_in_order() -> None:
    query = CreateTableQuery(
        "messages",
        [PrimaryKeyColumn(type_="SERIAL"), TASK_ID, CreatedAtColumn()],
    )

    assert query.make_query() == (
        "CREATE TABLE IF NOT EXISTS messages ("
        "id SERIAL NOT NULL PRIMARY KEY, "
        "task_id UUID NOT NULL, "
        "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW())"
    )


def test_create_index_emits_one_hash_index_per_column() -> None:
    query = CreateIndexQuery("messages")

    assert query.make_query([PrimaryKeyColumn(), TASK_ID]) == (
        "CREATE INDEX IF NOT EXISTS messages_id_idx "
        "ON messages USING HASH (id);\n"
        "CREATE INDEX IF NOT EXISTS messages_task_id_idx "
        "ON messages USING HASH (task_id);"
    )


def test_insert_numbers_one_placeholder_per_column() -> None:
    query = InsertQuery("messages")

    assert query.make_query([TASK_ID, MESSAGE]) == (
        "INSERT INTO messages (task_id, message) VALUES ($1, $2)"
    )


def test_insert_returning() -> None:
    query = InsertQuery("messages")

    assert query.make_query([TASK_ID, MESSAGE], [PrimaryKeyColumn()]) == (
        "INSERT INTO messages (task_id, message) VALUES ($1, $2) RETURNING id"
    )


def test_upsert_updates_the_given_columns_from_excluded() -> None:
    query = InsertOrUpdateQuery("results")

    sql = query.make_query(
        [TASK_ID, RESULT],
        None,
        [TASK_ID],
        on_conflict_update_columns=[RESULT],
    )

    assert sql == (
        "INSERT INTO results (task_id, result) VALUES ($1, $2) "
        "ON CONFLICT (task_id) DO UPDATE SET result = EXCLUDED.result "
    )


def test_upsert_returning() -> None:
    query = InsertOrUpdateQuery("results")

    sql = query.make_query(
        [TASK_ID, RESULT],
        [TASK_ID],
        [TASK_ID],
        on_conflict_update_columns=[RESULT],
    )

    assert sql.endswith("DO UPDATE SET result = EXCLUDED.result  RETURNING task_id")


def test_upsert_do_nothing_names_the_conflict_columns() -> None:
    query = InsertOrUpdateQuery("results")

    sql = query.make_query(
        [TASK_ID, RESULT],
        [TASK_ID],
        [TASK_ID],
        on_conflict_action="NOTHING",
    )

    assert sql == (
        "INSERT INTO results (task_id, result) VALUES ($1, $2) "
        "ON CONFLICT (task_id) DO NOTHING RETURNING task_id"
    )


def test_upsert_update_without_update_columns_fails() -> None:
    query = InsertOrUpdateQuery("results")

    with pytest.raises(ValueError, match="on_conflict_update_columns is required"):
        query.make_query([TASK_ID, RESULT], None, [TASK_ID])


def test_delete_binds_the_key() -> None:
    assert DeleteQuery("results").make_query(TASK_ID) == (
        "DELETE FROM results WHERE task_id = $1"
    )


def test_claim_by_id_deletes_the_due_row_returning_columns() -> None:
    query = ClaimByIdQuery("messages")

    assert query.make_query(PrimaryKeyColumn(), CLAIM_COLUMNS, [MESSAGE, TASK_ID]) == (
        "DELETE FROM messages WHERE id = $2 AND not_before <= NOW() "
        "AND (channel = $1 OR channel IS NULL) RETURNING message, task_id"
    )


def test_delete_by_date_treats_a_null_upper_bound_as_none() -> None:
    assert DeleteByDateQuery("results").make_query(CreatedAtColumn()) == (
        "DELETE FROM results WHERE created_at >= $1 "
        "AND ($2::TIMESTAMP WITH TIME ZONE IS NULL OR created_at <= $2)"
    )


def test_select_without_filter() -> None:
    assert SelectQuery("schedules").make_query([Column("schedule", "JSONB")]) == (
        "SELECT schedule FROM schedules "
    )


def test_select_numbers_each_filter_column() -> None:
    query = SelectQuery("results")

    assert query.make_query([RESULT, TASK_ID], [TASK_ID, RESULT]) == (
        "SELECT result, task_id FROM results WHERE task_id = $1 AND result = $2"
    )


def test_identifiers_are_emitted_verbatim() -> None:
    # Table and column names come from the component's configuration and are
    # written as given, without double quotes, so PostgreSQL folds them to
    # lower case like any unquoted identifier. Changing that is a schema
    # change for existing users and must be deliberate.
    sql = CreateTableQuery("TaskResults", [Column("TaskId", "UUID")]).make_query()

    assert sql == "CREATE TABLE IF NOT EXISTS TaskResults (TaskId UUID NOT NULL)"


def test_values_never_reach_the_sql_text() -> None:
    # Every builder that takes values binds them as $n parameters; the drivers
    # pass the values separately, so no task data is formatted into SQL.
    statements = [
        InsertQuery("t").make_query([TASK_ID, RESULT]),
        InsertOrUpdateQuery("t").make_query(
            [TASK_ID, RESULT],
            None,
            [TASK_ID],
            on_conflict_update_columns=[RESULT],
        ),
        DeleteQuery("t").make_query(TASK_ID),
        ClaimByIdQuery("t").make_query(TASK_ID, CLAIM_COLUMNS, [RESULT]),
        DeleteByDateQuery("t").make_query(CreatedAtColumn()),
        SelectQuery("t").make_query([RESULT], [TASK_ID]),
    ]

    for sql in statements:
        assert "$1" in sql, sql
        assert "'" not in sql, sql


def test_drop_not_null_writes_one_statement_per_column() -> None:
    query = DropNotNullQuery("results").make_query("result")

    assert query == "ALTER TABLE results ALTER COLUMN result DROP NOT NULL;"
