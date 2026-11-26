"""``Statement``, ``Script`` and ``param_type``: no database needed."""

import pytest

from taskiq_postgresql.core.sql.statement import (
    ParamType,
    Script,
    Statement,
    param_type,
)


def test_param_type_members() -> None:
    assert {member.name for member in ParamType} == {
        "TEXT",
        "INT4",
        "INT8",
        "FLOAT8",
        "BOOL",
        "JSONB",
        "UUID",
        "BYTEA",
        "TIMESTAMPTZ",
        "INT4_ARRAY",
        "UUID_ARRAY",
        "TEXT_ARRAY",
    }


def test_a_statement_with_matching_placeholders() -> None:
    statement = Statement(
        "SELECT 1 FROM t WHERE a = $1 AND b = $2",
        (ParamType.TEXT, ParamType.INT4),
        (("one", ParamType.INT4),),
    )
    assert statement.params == (ParamType.TEXT, ParamType.INT4)
    assert statement.columns == (("one", ParamType.INT4),)


def test_a_statement_defaults_to_no_params_and_no_columns() -> None:
    statement = Statement("SELECT 1")
    assert statement.params == ()
    assert statement.columns == ()


def test_a_repeated_placeholder_is_one_parameter() -> None:
    Statement(
        "DELETE FROM t WHERE d >= $1 AND ($2 IS NULL OR d <= $2)",
        (ParamType.TIMESTAMPTZ, ParamType.TIMESTAMPTZ),
    )


def test_a_missing_placeholder_is_refused() -> None:
    with pytest.raises(ValueError, match="placeholder"):
        Statement("SELECT $1, $3", (ParamType.TEXT, ParamType.TEXT, ParamType.TEXT))


def test_a_placeholder_above_the_total_is_refused() -> None:
    with pytest.raises(ValueError, match="placeholder"):
        Statement("SELECT $1, $2", (ParamType.TEXT,))


def test_a_param_without_a_placeholder_is_refused() -> None:
    with pytest.raises(ValueError, match="placeholder"):
        Statement("SELECT 1", (ParamType.TEXT,))


def test_a_dollar_inside_a_name_is_not_a_placeholder() -> None:
    statement = Statement(
        "INSERT INTO jobs$5 (a, b) VALUES ($1, $2)",
        (ParamType.TEXT, ParamType.TEXT),
    )
    assert statement.params == (ParamType.TEXT, ParamType.TEXT)


def test_a_name_with_a_dollar_alone_adds_no_parameter() -> None:
    Statement("SELECT 1 FROM jobs$1")


def test_statement_is_frozen() -> None:
    statement = Statement("SELECT 1")
    with pytest.raises(AttributeError):
        statement.text = "SELECT 2"  # type: ignore[misc]


def test_script_holds_text_and_is_frozen() -> None:
    script = Script("LISTEN jobs")
    assert script.text == "LISTEN jobs"
    with pytest.raises(AttributeError):
        script.text = "x"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("sql_type", "expected"),
    [
        ("SERIAL", ParamType.INT4),
        ("UUID", ParamType.UUID),
        ("Uuid", ParamType.UUID),
        ("VARCHAR", ParamType.TEXT),
        ("VarChar", ParamType.TEXT),
        ("VARCHAR(255)", ParamType.TEXT),
        ("varchar(10)", ParamType.TEXT),
        ("TEXT", ParamType.TEXT),
        ("Text", ParamType.TEXT),
        ("BYTEA", ParamType.BYTEA),
        ("JSONB", ParamType.JSONB),
        ("TIMESTAMP WITH TIME ZONE", ParamType.TIMESTAMPTZ),
        ("timestamp with time zone", ParamType.TIMESTAMPTZ),
    ],
)
def test_param_type_maps_the_component_types(
    sql_type: str, expected: ParamType
) -> None:
    assert param_type(sql_type) is expected


def test_param_type_refuses_an_unknown_type_naming_it() -> None:
    with pytest.raises(ValueError, match="INTEGER"):
        param_type("INTEGER")
