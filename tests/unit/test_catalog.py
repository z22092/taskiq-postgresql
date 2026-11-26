"""
Unit tests for the SQL catalog (ADR 0007): typed statements and scripts.

No database: each builder's typed ``statement()`` must carry the text its
``make_query()`` returns, with the types the server infers for it.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskiq_postgresql.core.sql import catalog
from taskiq_postgresql.core.sql.catalog import (
    AddColumnsQuery,
    AddedIndex,
    ClaimByIdQuery,
    ClaimColumns,
    ClaimDueQuery,
    Column,
    CreatedAtColumn,
    CreateIndexQuery,
    CreateTableQuery,
    DeleteByDateQuery,
    DeleteQuery,
    DropNotNullQuery,
    InsertOrUpdateQuery,
    InsertQuery,
    MigrationLockQuery,
    NotBeforeColumn,
    PrimaryKeyColumn,
    RelationsExistQuery,
    SelectQuery,
    UpdatedAtColumn,
    _sql_literal,
)
from taskiq_postgresql.core.sql.identifier import Identifier
from taskiq_postgresql.core.sql.statement import ParamType, Script, Statement

TEXT = ParamType.TEXT


def _broker_columns(task_id_type: str) -> dict[str, Column]:
    return {
        "primary_key": PrimaryKeyColumn(name="id", type_="SERIAL"),
        "task_id": Column(name="task_id", type_=task_id_type),
        "task_name": Column(name="task_name", type_="VARCHAR"),
        "message": Column(name="message", type_="BYTEA"),
        "labels": Column(name="labels", type_="JSONB"),
        "created_at": CreatedAtColumn(),
        "channel": Column(name="channel", type_="VARCHAR", nullable=True),
        "not_before": NotBeforeColumn(),
    }


@pytest.mark.parametrize("task_id_type", ["Uuid", "VarChar", "Text"])
def test_broker_statements_carry_the_make_query_text(task_id_type: str) -> None:
    cols = _broker_columns(task_id_type)
    pk = cols["primary_key"]
    claim = ClaimColumns(channel=cols["channel"], not_before=cols["not_before"])
    returning = [pk, cols["task_id"], cols["message"]]
    inserted = [cols["task_id"], cols["task_name"], cols["message"], cols["labels"]]

    insert = InsertQuery("jobs")
    due = ClaimDueQuery("jobs")
    by_id = ClaimByIdQuery("jobs")

    statement = insert.statement(inserted, [pk])
    assert statement.text == insert.make_query(inserted, [pk])
    assert statement.params == (
        cols["task_id"].bind_type,
        TEXT,
        ParamType.BYTEA,
        ParamType.JSONB,
    )
    assert statement.columns == (("id", ParamType.INT4),)

    statement = due.statement(pk, claim, returning)
    assert statement.text == due.make_query(pk, claim, returning)
    assert statement.params == (TEXT,)
    assert [name for name, _ in statement.columns] == ["id", "task_id", "message"]

    statement = by_id.statement(pk, claim, returning)
    assert statement.text == by_id.make_query(pk, claim, returning)
    assert statement.params == (TEXT, ParamType.INT4)


def test_the_delay_column_binds_seconds_and_reads_a_timestamp() -> None:
    not_before = NotBeforeColumn()

    assert not_before.bind_type is ParamType.FLOAT8
    assert not_before.result_type is ParamType.TIMESTAMPTZ
    statement = InsertQuery("jobs").statement([Column("a", "TEXT"), not_before])
    assert statement.params == (TEXT, ParamType.FLOAT8)
    assert "NOW() + make_interval(secs => $2::double precision)" in statement.text


def test_result_backend_statements() -> None:
    pk = PrimaryKeyColumn(name="task_id", type_="Uuid")
    result = Column(name="result", type_="BYTEA")

    upsert = InsertOrUpdateQuery("results")
    statement = upsert.statement([pk, result], None, [pk], "UPDATE", [result])
    assert statement.text == upsert.make_query(
        [pk, result],
        None,
        [pk],
        "UPDATE",
        [result],
    )
    assert statement.params == (ParamType.UUID, ParamType.BYTEA)
    assert statement.columns == ()

    select = SelectQuery("results")
    statement = select.statement([result], [pk])
    assert statement.text == select.make_query([result], [pk])
    assert statement.params == (ParamType.UUID,)
    assert statement.columns == (("result", ParamType.BYTEA),)

    delete = DeleteQuery("results")
    statement = delete.statement(pk)
    assert statement.text == delete.make_query(pk)
    assert statement.params == (ParamType.UUID,)


def test_delete_by_date_binds_the_date_column_type_twice() -> None:
    date = CreatedAtColumn()
    query = DeleteByDateQuery("results")

    statement = query.statement(date)

    assert statement.text == query.make_query(date)
    assert statement.params == (ParamType.TIMESTAMPTZ, ParamType.TIMESTAMPTZ)
    assert statement.columns == ()


def test_scheduler_statements() -> None:
    pk = PrimaryKeyColumn(name="id", type_="UUID")
    name = Column(name="task_name", type_="VARCHAR(100)")
    schedule = Column(name="schedule", type_="JSONB")
    columns = [pk, name, schedule, CreatedAtColumn(), UpdatedAtColumn()]

    select = SelectQuery("schedules")
    statement = select.statement(columns)
    assert statement.text == select.make_query(columns)
    assert statement.params == ()
    assert statement.columns[1] == ("task_name", TEXT)
    assert statement.columns[2] == ("schedule", ParamType.JSONB)

    insert = InsertQuery("schedules")
    statement = insert.statement([pk, name, schedule])
    assert statement.text == insert.make_query([pk, name, schedule])
    assert statement.params == (ParamType.UUID, TEXT, ParamType.JSONB)


def test_relations_exist_types_each_parameter_as_text() -> None:
    query = RelationsExistQuery("jobs")
    index_columns = [Column("task_id", "UUID")]
    added = [Column("channel", "VARCHAR", nullable=True)]
    indexes = [AddedIndex("jobs_channel_idx", added)]

    statement = query.statement(
        index_columns,
        added_columns=added,
        added_indexes=indexes,
    )

    assert statement.text == query.make_query(
        index_columns,
        added_columns=added,
        added_indexes=indexes,
    )
    assert statement.params == (TEXT,) * len(
        query.params(index_columns, added_columns=added, added_indexes=indexes),
    )
    assert statement.columns == (("?column?", ParamType.BOOL),)


def test_migration_lock_statement_has_two_text_params_and_no_columns() -> None:
    query = MigrationLockQuery("Jobs_Mixed")

    statement = query.statement()

    assert statement.text == query.make_query()
    assert statement.text == "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))"
    assert statement.params == (TEXT, TEXT)
    assert statement.columns == ()
    assert query.params()[1] == "Jobs_Mixed"


def test_ddl_builders_return_scripts_with_the_make_query_text() -> None:
    columns = [Column("task_id", "UUID"), Column("channel", "VARCHAR", nullable=True)]
    added = AddedIndex("jobs_channel_idx", columns[1:])

    create = CreateTableQuery("jobs", columns)
    index = CreateIndexQuery("jobs")
    add = AddColumnsQuery("jobs")
    drop = DropNotNullQuery("jobs")

    assert create.script() == Script(create.make_query())
    assert index.script(columns[:1]) == Script(index.make_query(columns[:1]))
    assert add.script(columns[1:], [added]) == Script(
        add.make_query(columns[1:], [added])
    )
    assert drop.script("channel") == Script(drop.make_query("channel"))


@pytest.mark.parametrize("table", ["jobs", "Jobs_Mixed", "jobs$5"])
def test_every_component_table_name_builds_a_valid_statement(table: str) -> None:
    pk = PrimaryKeyColumn(name="id", type_="UUID")
    result = Column(name="result", type_="BYTEA")

    statement = SelectQuery(table).statement([result], [pk])

    assert isinstance(statement, Statement)
    assert statement.text == f"SELECT result FROM {table} WHERE id = $1"  # noqa: S608
    assert DeleteQuery(table).statement(pk).params == (ParamType.UUID,)


def test_a_table_name_the_rule_refuses_is_refused_by_the_query() -> None:
    with pytest.raises(ValueError, match="is not a plain SQL identifier"):
        SelectQuery("o'brien")


def test_query_base_keeps_the_configured_name_and_the_identifier() -> None:
    query = SelectQuery("Jobs_Mixed")

    assert query.table_name == "Jobs_Mixed"
    assert query.table.configured == "Jobs_Mixed"
    assert query.table.name == "jobs_mixed"


def test_make_query_stays_untyped_for_a_third_party_column() -> None:
    integer = Column("n", "NUMERIC")

    assert InsertQuery("jobs").make_query([integer]) == (
        "INSERT INTO jobs (n) VALUES ($1)"
    )
    assert SelectQuery("jobs").make_query([integer], [integer]) == (
        "SELECT n FROM jobs WHERE n = $1"
    )
    with pytest.raises(ValueError, match="no ParamType"):
        InsertQuery("jobs").statement([integer])


def test_make_query_accepts_the_drivers_literal_column() -> None:
    one = Column("1", "")
    key = Column("task_id", "UUID")

    assert (
        SelectQuery("jobs").make_query([one], [key])
        == "SELECT 1 FROM jobs WHERE task_id = $1"
    )


def test_exists_query_statement_is_the_text_the_drivers_build_today() -> None:

    key = Column("id", "SERIAL")

    statement = catalog.ExistsQuery("jobs").statement(key)

    assert statement.text == SelectQuery("jobs").make_query([Column("1", "")], [key])
    assert statement.text == "SELECT 1 FROM jobs WHERE id = $1"
    assert statement.params == (ParamType.INT4,)
    assert statement.columns == (("?column?", ParamType.INT4),)


def test_notify_statement_has_no_result_column() -> None:

    statement = catalog.notify_statement()

    assert statement.text == "SELECT FROM pg_notify($1, $2)"
    assert statement.params == (TEXT, TEXT)
    assert statement.columns == ()


def test_listen_script_names_the_folded_channel() -> None:

    channel = Identifier.parse("Jobs_Mixed", what="channel name", warn=False)

    assert catalog.listen_script(channel) == Script("LISTEN jobs_mixed")


def test_both_lock_forms_come_from_one_template() -> None:

    script = catalog.migration_lock_script(
        Identifier.parse("Jobs_Mixed", what="table name", warn=False),
    )
    statement = MigrationLockQuery("Jobs_Mixed").statement()

    assert script == Script(
        "SELECT pg_advisory_xact_lock(hashtext('taskiq_postgresql'), "
        "hashtext('Jobs_Mixed'))",
    )
    assert "hashtext('Jobs_Mixed')" in script.text
    assert (
        statement.text.replace("$1", "'taskiq_postgresql'").replace(
            "$2",
            "'Jobs_Mixed'",
        )
        == script.text
    )


def test_lock_timeout_scripts_keep_today_s_text() -> None:

    scripts: Any = catalog.lock_timeout_scripts(7)

    assert scripts.set_local == Script("SET LOCAL lock_timeout = '7s'")
    assert scripts.save == Script(
        "SELECT set_config('taskiq_postgresql_step.lock_timeout', "
        "current_setting('lock_timeout'), true)",
    )
    assert scripts.restore == Script(
        "SELECT set_config('lock_timeout', "
        "NULLIF(current_setting('taskiq_postgresql_step.lock_timeout', true), ''), "
        "true)",
    )
    with pytest.raises(ValueError, match="lock_timeout must be a whole number"):
        catalog.lock_timeout_scripts(-1)


def test_drop_table_and_transaction_scripts() -> None:

    assert catalog.drop_table_script("jobs") == Script("DROP TABLE IF EXISTS jobs")
    assert catalog.in_transaction(["SELECT 1"]) == ["BEGIN", "SELECT 1", "COMMIT"]


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        ("plain", "'plain'"),
        ("o'brien", "'o''brien'"),
        ("back\\slash", "E'back\\\\slash'"),
        ("both'\\", "E'both''\\\\'"),
    ],
    ids=["plain", "quote", "backslash", "both"],
)
def test_sql_literal_escapes_quotes_and_backslashes(value: str, literal: str) -> None:
    assert _sql_literal(value) == literal


def test_catalog_and_database_reads_wrap_the_same_text() -> None:

    table = Identifier.parse("jobs", what="table name", warn=False)

    read = catalog.catalog_read([(table, ["jobs_task_id_idx"])])
    database = catalog.database_read([(table, ["jobs_task_id_idx"])])

    assert read.params == ()
    assert read.columns == (("catalog", TEXT),)
    assert "'name', 'jobs'" in read.text
    assert "'name', 'jobs_task_id_idx'" in read.text
    assert database.text == (
        "SELECT current_database()::text AS database,"
        " current_schemas(false)::text AS search_path,"
        f" ({read.text}) AS catalog"
    )
    assert database.columns == (
        ("database", TEXT),
        ("search_path", TEXT),
        ("catalog", TEXT),
    )
    with pytest.raises(ValueError, match="at least one table schema"):
        catalog.catalog_read([])
