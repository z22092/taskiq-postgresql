"""
The rule for a configured table or channel name, and the names derived from it.

Nothing here needs a database: the rule is plain Python, and the derived names
are checked against what PostgreSQL's own parser and ``makeObjectName`` do.
"""

import warnings
from pathlib import Path
from typing import Optional

import pytest

from taskiq_postgresql.core.sql.identifier import (
    RESERVED_WORDS,
    Identifier,
    IdentifierError,
    check_table_names,
    excerpt,
    fold,
    index_relation_name,
    object_name,
    truncate,
)

MAX_BYTES = 63
RESERVED_COUNT = 101  # categories R and T, PostgreSQL 17 and 18
BROKER_NAME_BYTES = 62

# Each component's index suffixes and serial column, as its steps create them.
BROKER = (("id_idx", "not_before_idx"), "id")
RESULT_BACKEND = (("task_id_idx",), None)
SCHEDULER = (("id_idx", "task_name_idx"), None)


def parse(value: object, *, what: str = "table name") -> Identifier:
    return Identifier.parse(value, what=what, warn=False)


def check(
    name: str,
    component: tuple[tuple[str, ...], Optional[str]],
) -> None:
    suffixes, serial = component
    table = parse(name)
    # The package hands index names over with the configured case.
    check_table_names(
        table,
        index_names=[f"{name}_{suffix}" for suffix in suffixes],
        serial_column=serial,
    )


def source_line(lineno: int) -> str:
    return Path(__file__).read_text(encoding="utf-8").splitlines()[lineno - 1]


# R1


@pytest.mark.parametrize("value", [123, None, b"jobs", 1.5])
def test_r1_a_non_string_is_a_type_error_naming_the_type(value: object) -> None:
    with pytest.raises(TypeError, match=type(value).__name__):
        parse(value)


# R2


def test_r2_empty_message() -> None:
    with pytest.raises(IdentifierError) as caught:
        parse("")
    assert str(caught.value) == "table name '' cannot be written in SQL: it is empty"


@pytest.mark.parametrize(
    "value",
    [
        "jobs; DROP TABLE x",
        "tâche",
        "9jobs",
        "$jobs",
        "a\nb",
        "a\n",
        "app.jobs",
        "o'brien",
        "with space",
    ],
)
def test_r2_outside_the_grammar_is_refused_with_the_text(value: str) -> None:
    with pytest.raises(IdentifierError) as caught:
        parse(value, what="channel name")
    assert str(caught.value) == (
        f"channel name {excerpt(value)} is not a plain SQL identifier "
        "(ASCII letters, digits, _ and $, not starting with a digit or $), "
        "and the SQL writes it unquoted"
    )


def test_identifier_error_is_a_value_error() -> None:
    assert issubclass(IdentifierError, ValueError)


# excerpt


def test_excerpt_of_a_short_value_is_its_repr() -> None:
    assert excerpt("abc") == "'abc'"


def test_excerpt_of_a_long_value_is_cut_with_the_total() -> None:
    value = "x" * 100
    assert excerpt(value) == repr("x" * 64) + " (truncated, 100 characters)"


def test_excerpt_of_exactly_64_characters_is_not_marked() -> None:
    assert excerpt("x" * 64) == repr("x" * 64)


def test_excerpt_keeps_a_line_break_escaped() -> None:
    shown = excerpt("a\nFAKE LOG LINE")
    assert "\n" not in shown
    assert shown == "'a\\nFAKE LOG LINE'"


def test_a_refused_long_value_does_not_leak_whole() -> None:
    value = "a\n" + "z" * 500
    with pytest.raises(IdentifierError) as caught:
        parse(value)
    assert "\n" not in str(caught.value)
    assert "(truncated, 502 characters)" in str(caught.value)
    assert "z" * 100 not in str(caught.value)


# R3


def test_r3_64_bytes_is_refused_with_the_size() -> None:
    value = "a" * 64
    with pytest.raises(IdentifierError) as caught:
        parse(value)
    assert str(caught.value) == (
        f"table name {excerpt(value)} is 64 bytes; PostgreSQL keeps 63"
    )


def test_r3_63_bytes_is_accepted() -> None:
    assert parse("a" * 63).name == "a" * 63


# R4


@pytest.mark.parametrize("value", ["order", "User", "left", "SELECT", "table"])
def test_r4_a_reserved_word_is_refused(value: str) -> None:
    with pytest.raises(IdentifierError) as caught:
        parse(value, what="channel name")
    assert str(caught.value) == (
        f"channel name {excerpt(value)} is a reserved word in PostgreSQL"
    )


@pytest.mark.parametrize("value", ["name", "text", "key", "user_", "orders", "int"])
def test_r4_an_unreserved_word_is_accepted(value: str) -> None:
    assert parse(value).name == value


def test_reserved_words_are_lower_case_and_include_both_categories() -> None:
    assert all(word == word.lower() for word in RESERVED_WORDS)
    assert "order" in RESERVED_WORDS  # category R
    # Category T: a type or function name, still no table or channel name.
    assert {"left", "right", "inner", "join", "full"} <= RESERVED_WORDS
    assert len(RESERVED_WORDS) == RESERVED_COUNT


# R5 and R6


def test_r5_mixed_case_is_accepted_and_folded() -> None:
    with pytest.warns(DeprecationWarning, match="jobs_mixed"):
        identifier = Identifier.parse("Jobs_Mixed", what="table name")
    assert identifier.configured == "Jobs_Mixed"
    assert identifier.name == "jobs_mixed"


def test_r5_warns_once_per_parse() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Identifier.parse("Jobs_Mixed", what="table name")
    assert [w.category for w in caught] == [DeprecationWarning]


def test_r5_the_warning_points_at_the_caller_line() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Identifier.parse("Jobs_Mixed", what="table name")  # CALLER-LINE
    assert len(caught) == 1
    assert caught[0].filename == __file__
    assert "CALLER-LINE" in source_line(caught[0].lineno)


def test_r5_stacklevel_moves_the_warning_to_the_callers_caller() -> None:
    def component() -> Identifier:
        return Identifier.parse("Jobs_Mixed", what="table name", stacklevel=3)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        component()  # USER-LINE
    assert "USER-LINE" in source_line(caught[0].lineno)


def test_r5_warn_false_is_silent() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert Identifier.parse("Jobs", what="name", warn=False).name == "jobs"


@pytest.mark.parametrize("value", ["taskiq_messages", "jobs$1", "_x", "a"])
def test_r6_a_plain_name_is_accepted_without_a_warning(value: str) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        identifier = Identifier.parse(value, what="table name")
    assert identifier.configured == value
    assert identifier.name == value


def test_identifier_is_frozen() -> None:
    identifier = parse("jobs")
    with pytest.raises(AttributeError):
        identifier.name = "other"  # type: ignore[misc]


# fold and truncate


def test_fold_lowers_ascii_only() -> None:
    assert fold("Jobs_MIXED$1") == "jobs_mixed$1"
    assert fold("ÉA") == "Éa"


def test_truncate_cuts_at_63_bytes() -> None:
    assert truncate("a" * 63) == "a" * 63
    assert truncate("a" * 64) == "a" * 63


# object_name, against makeObjectName


def test_object_name_without_a_second_name() -> None:
    assert object_name("jobs", None, "pkey") == "jobs_pkey"


def test_object_name_with_a_second_name() -> None:
    assert object_name("jobs", "id", "seq") == "jobs_id_seq"


def test_object_name_cuts_a_63_byte_table_to_58_before_the_label() -> None:
    table = "t" * 63
    assert object_name(table, None, "pkey") == "t" * 58 + "_pkey"


def test_object_name_shortens_the_longer_of_the_two_names() -> None:
    result = object_name("a" * 60, "id", "seq")
    # name2 stays whole; name1 loses what the 63 byte limit asks for.
    assert result == "a" * 56 + "_id_seq"
    assert len(result) == MAX_BYTES


def test_object_name_never_cuts_the_label() -> None:
    result = object_name("a" * 70, "b" * 70, "seq")
    assert result.endswith("_seq")
    assert len(result) <= MAX_BYTES


# index_relation_name


def test_index_relation_name_is_cut_like_the_parser_cuts() -> None:
    assert index_relation_name(parse("jobs"), "id") == "jobs_id_idx"
    assert index_relation_name(parse("a" * 61), "id") == "a" * 61 + "_i"


# M3.derivados


@pytest.mark.parametrize("length", [1, 48, 49, 61])
def test_broker_accepts_up_to_61_bytes(length: int) -> None:
    check("b" * length, BROKER)


@pytest.mark.parametrize("length", [62, 63])
def test_broker_refuses_62_bytes_and_more(length: int) -> None:
    with pytest.raises(IdentifierError):
        check("b" * length, BROKER)


@pytest.mark.parametrize("length", [51, 52, 62])
def test_result_backend_accepts_up_to_62_bytes(length: int) -> None:
    check("r" * length, RESULT_BACKEND)


def test_result_backend_refuses_63_bytes() -> None:
    with pytest.raises(IdentifierError):
        check("r" * 63, RESULT_BACKEND)


@pytest.mark.parametrize("length", [49, 50, 61])
def test_scheduler_accepts_up_to_61_bytes(length: int) -> None:
    check("s" * length, SCHEDULER)


def test_scheduler_refuses_62_bytes() -> None:
    with pytest.raises(IdentifierError):
        check("s" * 62, SCHEDULER)


def test_a_mixed_case_62_byte_broker_name_is_refused() -> None:
    name = "Jobs" + "b" * 58
    assert len(name) == BROKER_NAME_BYTES
    with pytest.raises(IdentifierError):
        check(name, BROKER)


def test_a_mixed_case_61_byte_broker_name_is_accepted() -> None:
    check("Jobs" + "b" * 57, BROKER)


def test_the_error_names_the_colliding_names_in_excerpt_form() -> None:
    name = "b" * 62
    with pytest.raises(IdentifierError) as caught:
        check(name, BROKER)
    message = str(caught.value)
    # Both cut indexes became "<t>_": one pair, named in excerpt form.
    assert excerpt(name + "_") in message
    assert "\n" not in message


def test_a_cut_index_equal_to_the_table_name_is_refused() -> None:
    name = "r" * 63
    with pytest.raises(IdentifierError) as caught:
        check(name, RESULT_BACKEND)
    assert excerpt(name) in str(caught.value)


def test_the_primary_key_name_takes_part() -> None:
    # The cut primary key of a 63 byte table is "t"*58 + "_pkey".
    with pytest.raises(IdentifierError):
        check_table_names(
            parse("t" * 63),
            index_names=["t" * 58 + "_pkey"],
            serial_column=None,
        )


def test_the_serial_sequence_name_takes_part() -> None:
    table = parse("jobs")
    with pytest.raises(IdentifierError):
        check_table_names(table, index_names=["jobs_id_seq"], serial_column="id")
    check_table_names(table, index_names=["jobs_id_seq"], serial_column=None)
