"""
The migration format registry: the entry points ``--format`` chooses from.

A third-party package adds a format by registering a callable under the
``taskiq_postgresql.migration_formats`` entry-point group; these tests swap the
entry-point lookup to stand for such a package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import EntryPoint, entry_points
from typing import Any, Callable, Optional, Sequence

import pytest

from taskiq_postgresql import migrations
from taskiq_postgresql.migrations import (
    MIGRATION_FORMATS_GROUP,
    FormatOptions,
    MigrationFormatOptionError,
    TableSchema,
    UnknownMigrationFormatError,
    available_formats,
    load_format,
    render_sql,
    sql_format,
    table_schema,
)


def shout_format(schemas: Sequence[TableSchema], options: FormatOptions) -> str:
    """Stand for a third-party format: the table names and the extra options."""
    names = " ".join(schema.name.upper() for schema in schemas)
    return f"{names} {sorted(options.extra.items())}\n"


@dataclass
class FakeDistribution:
    name: str


@dataclass
class FakeEntryPoint:
    """The entry-point attributes the registry reads, with a custom loader."""

    name: str
    loader: Callable[[], Any]
    extras: list[str] = field(default_factory=list)
    dist: Optional[FakeDistribution] = None

    def load(self) -> Any:
        """Load the format, as ``EntryPoint.load`` does."""
        return self.loader()


def register(monkeypatch: pytest.MonkeyPatch, *added: object) -> None:
    """Make the registry see the installed entry points plus ``added``."""
    installed = list(migrations._entry_points())
    monkeypatch.setattr(migrations, "_entry_points", lambda: [*installed, *added])


def test_the_sql_format_is_registered_by_the_package() -> None:
    (entry_point,) = entry_points(group=MIGRATION_FORMATS_GROUP, name="sql")

    assert MIGRATION_FORMATS_GROUP == "taskiq_postgresql.migration_formats"
    assert entry_point.value == "taskiq_postgresql.migrations:sql_format"
    assert entry_point.dist is not None
    assert entry_point.dist.name == "taskiq-postgresql"
    assert "sql" in available_formats()
    assert load_format("sql") is sql_format


def test_the_sql_format_renders_the_script_with_the_options() -> None:
    schemas = [table_schema("broker"), table_schema("scheduler_source")]

    assert sql_format(schemas, FormatOptions()) == render_sql(schemas)
    assert sql_format(
        schemas,
        FormatOptions(transaction=False, lock=False),
    ) == render_sql(schemas, transaction=False, lock=False)
    assert sql_format(schemas, FormatOptions(lock_timeout=0)) == render_sql(
        schemas,
        lock_timeout=0,
    )
    assert "lock_timeout = '3s'" in sql_format(schemas, FormatOptions(lock_timeout=3))


def test_the_sql_format_refuses_extra_options() -> None:
    with pytest.raises(MigrationFormatOptionError, match="author"):
        sql_format([table_schema("broker")], FormatOptions(extra={"author": "me"}))


def test_a_third_party_format_is_found_through_its_entry_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register(
        monkeypatch,
        EntryPoint(
            name="shout",
            value="tests.test_migration_formats:shout_format",
            group=MIGRATION_FORMATS_GROUP,
        ),
    )

    render = load_format("shout")

    assert "shout" in available_formats()
    assert render is shout_format
    assert render(
        [table_schema("broker")],
        FormatOptions(extra={"k": "v"}),
    ) == ("TASKIQ_MESSAGES [('k', 'v')]\n")


def test_listing_the_formats_loads_none_of_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def never() -> Any:
        raise AssertionError("listing must not load a format")

    register(monkeypatch, FakeEntryPoint(name="lazy", loader=never))

    assert "lazy" in available_formats()


def test_a_format_whose_import_fails_names_the_extra_to_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register(
        monkeypatch,
        EntryPoint(
            name="yaml",
            value="taskiq_postgresql_yaml_is_not_installed:render [yaml]",
            group=MIGRATION_FORMATS_GROUP,
        ),
    )

    with pytest.raises(ImportError) as missing:
        load_format("yaml")

    assert "Migration format 'yaml' is not installed" in str(missing.value)
    assert "pip install 'taskiq-postgresql[yaml]'" in str(missing.value)
    assert isinstance(missing.value.__cause__, ModuleNotFoundError)


def test_the_install_hint_names_the_package_that_registers_the_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken() -> Any:
        raise ImportError("No module named 'acme_yaml'")

    register(
        monkeypatch,
        FakeEntryPoint(
            name="yaml",
            loader=broken,
            dist=FakeDistribution("acme-taskiq-formats"),
        ),
    )

    # Without extras on the entry point, the extra is named after the format.
    with pytest.raises(ImportError, match=r"pip install 'acme-taskiq-formats\[yaml\]'"):
        load_format("yaml")


def test_an_unknown_format_lists_the_available_ones() -> None:
    with pytest.raises(UnknownMigrationFormatError) as unknown:
        load_format("nope")

    assert unknown.value.format_name == "nope"
    assert "sql" in unknown.value.available
    assert "unknown migration format 'nope'" in str(unknown.value)
    assert "available formats: " in str(unknown.value)
    assert isinstance(unknown.value, LookupError)


def test_an_entry_point_that_is_not_callable_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register(
        monkeypatch,
        EntryPoint(
            name="constant",
            value="taskiq_postgresql.migrations:MIGRATION_FORMATS_GROUP",
            group=MIGRATION_FORMATS_GROUP,
        ),
    )

    with pytest.raises(TypeError, match="'constant'"):
        load_format("constant")
