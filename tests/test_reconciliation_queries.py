from taskiq_postgresql.abc.query import (
    BROKER_CHANNEL_LABEL,
    Column,
    CreatedAtColumn,
    MaxValueQuery,
    PrimaryKeyColumn,
    SelectAvailableIdsQuery,
)


def _normalize(query: str) -> str:
    """Collapse SQL whitespace so tests focus on query semantics."""
    return " ".join(query.split())


def test_max_value_query_uses_the_requested_column() -> None:
    query = MaxValueQuery("taskiq_messages")

    assert query.make_query(PrimaryKeyColumn("message_id")) == (
        "SELECT COALESCE(MAX(message_id), 0) FROM taskiq_messages"
    )


def test_select_available_ids_query_uses_bounded_keyset_pagination() -> None:
    query = SelectAvailableIdsQuery("taskiq_messages").make_query(
        primary_key=PrimaryKeyColumn("message_id"),
        labels=Column("task_labels", "JSONB"),
        created_at=Column("inserted_at", "TIMESTAMP WITH TIME ZONE"),
    )
    normalized = _normalize(query)

    assert normalized.startswith("SELECT message_id FROM taskiq_messages")
    assert "WHERE message_id > $1 AND message_id <= $2" in normalized
    assert f"task_labels->>'{BROKER_CHANNEL_LABEL}' = $3" in normalized
    assert normalized.endswith("ORDER BY message_id LIMIT $4")
    assert [normalized.index(f"${index}") for index in range(1, 5)] == sorted(
        normalized.index(f"${index}") for index in range(1, 5)
    )
    assert normalized.count("$1") == 1
    assert normalized.count("$2") == 1
    assert normalized.count("$3") == 1
    assert normalized.count("$4") == 1


def test_select_available_ids_query_filters_only_due_delayed_rows() -> None:
    query = SelectAvailableIdsQuery("taskiq_messages").make_query(
        primary_key=PrimaryKeyColumn(),
        labels=Column("labels", "JSONB"),
        created_at=CreatedAtColumn(),
    )
    normalized = _normalize(query)

    assert "labels->>'delay' IS NULL" in normalized
    assert "EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at)) >= CASE" in normalized
    assert "jsonb_typeof(labels->'delay') = 'number'" in normalized
    assert "trunc((labels->>'delay')::numeric)" in normalized
    assert "jsonb_typeof(labels->'delay') = 'string'" in normalized
    assert "btrim(labels->>'delay') ~ '^[+-]?[0-9]+$'" in normalized
    assert "btrim(labels->>'delay')::numeric" in normalized
