"""
The migration steps Alembic's autogenerate writes, and ``alembic check``.

Importing ``taskiq_postgresql.contrib.alembic`` registers a comparator that
puts each missing step in the revision as a ``TaskiqStepOp``. These tests run
autogenerate (``produce_migrations``) against databases at each step, and the
``alembic`` command itself in a throwaway environment. CI runs them on the
locked Alembic and on the oldest one the extra allows (``pytest -m alembic``),
because the two order their comparators differently.
"""

from __future__ import annotations

import ast
import asyncio
import dataclasses
import logging
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

import pytest
from alembic import op
from alembic.autogenerate import comparators, produce_migrations, render_python_code
from alembic.migration import MigrationContext
from alembic.operations import MigrateOperation, ops
from sqlalchemy import Column, Integer, MetaData, Table, create_engine

from taskiq_postgresql.contrib.alembic import (
    TaskiqStepOp,
    alembic_format,
    compare_taskiq_steps,
    exclude_taskiq_tables,
    taskiq_metadata,
    upgrade_taskiq_tables,
)
from taskiq_postgresql.contrib.sqlalchemy import METADATA_INFO_KEY, add_taskiq_tables
from taskiq_postgresql.migrations import (
    FormatOptions,
    MigrationStep,
    StepObjects,
    TableSchema,
    migration_statements,
    plan_migration,
    table_schema,
)
from tests.helpers import fetch_on_server, run_script
from tests.relax_schema import RELAXED, relaxing_schema
from tests.test_contrib_alembic import (
    SQLALCHEMY_DRIVERS,
    alembic,
    bounded,
    make_alembic_project,
    offline_sql,
    run_upgrade,
)
from tests.test_contrib_sqlalchemy import (
    LONG_NAME,
    catalog,
    databases,
    run_migrations,
    sqlalchemy_url,
    tables_in,
)
from tests.test_migration_plan import read_catalog
from tests.test_migrations import BROKER_TABLE_0_4_SQL

pytestmark = pytest.mark.alembic

STEP = "taskiq_postgresql_step"
BROKER = [("broker", "taskiq_messages")]


@pytest.fixture
def new_database(postgresql_dsn: str) -> Iterator[Callable[[], str]]:
    """Create empty databases for one test, and drop them after it."""
    yield from databases(postgresql_dsn)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """An Alembic environment with an empty ``versions`` directory."""
    return make_alembic_project(tmp_path)


def produce(dsn: str, metadata: Any, **options: Any) -> ops.MigrationScript:
    """Run autogenerate as ``alembic revision --autogenerate`` does."""
    engine = create_engine(sqlalchemy_url(dsn))
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection, opts=options)
            return produce_migrations(context, metadata)
    finally:
        engine.dispose()


def flat(diffs: list[Any]) -> list[tuple[Any, ...]]:
    """``as_diffs()`` nests column changes in lists; flatten them."""
    return [
        item for diff in diffs for item in (diff if isinstance(diff, list) else [diff])
    ]


def script_through(schema: TableSchema, through: int) -> str:
    statements = migration_statements([schema], through=through)
    return "".join(f"{statement};\n" for statement in ["BEGIN", *statements, "COMMIT"])


def prepare(dsn: str, driver: str, state: str, schema: TableSchema) -> None:
    """Bring the table of ``schema`` to ``state`` through ``driver``."""
    if state == "0.4-ddl":
        script = BROKER_TABLE_0_4_SQL.format(table=schema.name)
    elif state == "step-1":
        script = script_through(schema, 1)
    elif state == "current":
        script = script_through(schema, schema.steps[-1].number)
    else:
        return
    asyncio.run(run_script(dsn, driver, script))


def executed(rendered: str) -> list[str]:
    """Return the statements of the ``op.execute(...)`` lines of a revision."""
    return [
        ast.literal_eval(line.strip()[len("op.execute(") : -1])
        for line in rendered.splitlines()
        if line.strip().startswith("op.execute(")
    ]


def lock_of(schema: TableSchema) -> str:
    return migration_statements([schema])[0]


# --- autogenerate writes exactly the missing steps ----------------------------


@pytest.mark.parametrize(
    ("state", "expected"),
    [("fresh", [1, 2]), ("0.4-ddl", [2]), ("step-1", [2]), ("current", [])],
)
def test_autogenerate_writes_exactly_the_missing_steps(
    state: str,
    expected: list[int],
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    broker = table_schema("broker")
    prepare(dsn, driver, state, broker)

    script = produce(dsn, taskiq_metadata(broker))

    assert script.upgrade_ops.as_diffs() == [
        (STEP, "taskiq_messages", number) for number in expected
    ]
    # No create_table, add_column or create_index of ours is left beside them.
    assert all(
        isinstance(operation, TaskiqStepOp) for operation in script.upgrade_ops.ops
    )


def test_the_revision_writes_the_step_sql_and_imports_nothing(
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    broker = table_schema("broker")
    prepare(dsn, driver, "0.4-ddl", broker)
    script = produce(dsn, taskiq_metadata(broker))

    upgrade = render_python_code(script.upgrade_ops)
    downgrade = render_python_code(script.downgrade_ops)

    assert executed(upgrade) == bounded([lock_of(broker), *broker.steps[1].statements])
    assert "# taskiq-postgresql step 2 of the broker table taskiq_messages" in upgrade
    assert "ACCESS EXCLUSIVE" in upgrade
    # The downgrade leaves the additive step in place.
    assert executed(downgrade) == []
    assert "# taskiq-postgresql step 2 of taskiq_messages is additive" in downgrade
    for body in (upgrade, downgrade):
        assert "import" not in body
        assert "taskiq_postgresql." not in body
        compile(f"def function():\n{body}\n", "revision", "exec")


def test_the_alembic_format_writes_what_autogenerate_writes(
    new_database: Callable[[], str],
) -> None:
    schemas = [table_schema(kind) for kind in ("broker", "result_backend")]
    script = produce(new_database(), taskiq_metadata(*schemas))

    generated = render_python_code(script.upgrade_ops)
    formatted = alembic_format(schemas, FormatOptions())

    upgrade = formatted.split("def upgrade() -> None:\n", 1)[1].split("\n\n\n", 1)[0]
    assert executed(upgrade) == executed(generated)
    assert [line.strip() for line in upgrade.splitlines()] == [
        line.strip()
        for line in generated.splitlines()
        if not line.strip().startswith("# ###")
    ]


# --- alembic check, through the alembic command -------------------------------


@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
@pytest.mark.parametrize("state", ["fresh", "0.4-ddl"])
def test_alembic_check_fails_behind_and_passes_once_migrated(
    state: str,
    sqlalchemy_driver: str,
    project: Path,
    new_database: Callable[[], str],
) -> None:
    dsn, reference = new_database(), new_database()
    broker = table_schema("broker")
    prepare(dsn, "asyncpg", state, broker)
    url = sqlalchemy_url(dsn, sqlalchemy_driver).render_as_string(hide_password=False)

    behind = alembic(project, url, "check", tables=BROKER)
    revision = alembic(
        project,
        url,
        "revision",
        "--autogenerate",
        "-m",
        "taskiq steps",
        tables=BROKER,
    )
    # The revision runs where taskiq-postgresql cannot be imported.
    upgraded = alembic(project, url, "upgrade", "head", block=True)
    migrated = alembic(project, url, "check", tables=BROKER)
    downgraded = alembic(project, url, "downgrade", "base", block=True)

    expected = [2] if state == "0.4-ddl" else [1, 2]
    assert behind.returncode != 0
    assert str([(STEP, "taskiq_messages", number) for number in expected]) in (
        behind.stdout + behind.stderr
    )
    assert revision.returncode == 0, revision.stderr
    (source,) = [
        path.read_text(encoding="utf-8")
        for path in (project / "migrations" / "versions").glob("*.py")
    ]
    assert "taskiq_postgresql" not in "".join(
        line for line in source.splitlines() if line.startswith(("import", "from"))
    )
    assert upgraded.returncode == 0, upgraded.stderr
    assert migrated.returncode == 0, migrated.stdout + migrated.stderr
    assert "No new upgrade operations detected" in migrated.stdout + migrated.stderr
    # Downgrading leaves the additive steps, so the tables stay as migrated.
    assert downgraded.returncode == 0, downgraded.stderr
    run_migrations(reference, "broker")
    assert {
        name: [row for row in rows if "alembic_version" not in str(row)]
        for name, rows in catalog(dsn).items()
    } == catalog(reference)


def test_autogenerate_after_the_workers_migrated_writes_no_step(
    driver: str,
    project: Path,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    run_migrations(dsn, "broker", driver=driver)
    url = sqlalchemy_url(dsn).render_as_string(hide_password=False)

    checked = alembic(project, url, "check", tables=BROKER)
    revision = alembic(project, url, "revision", "--autogenerate", tables=BROKER)

    # Here the step exists only because a worker ran it, so the revision is
    # empty: a database migrated by Alembic alone never gets it.
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert revision.returncode == 0, revision.stderr
    (source,) = [
        path.read_text(encoding="utf-8")
        for path in (project / "migrations" / "versions").glob("*.py")
    ]
    assert "op.execute" not in source


def test_the_autogenerated_revision_prints_its_sql_offline(
    driver: str,
    project: Path,
    new_database: Callable[[], str],
) -> None:
    generated_from, applied_to = new_database(), new_database()
    url = sqlalchemy_url(generated_from).render_as_string(hide_password=False)
    revision = alembic(project, url, "revision", "--autogenerate", tables=BROKER)

    printed = alembic(project, url, "upgrade", "head", "--sql", block=True)

    assert revision.returncode == 0, revision.stderr
    assert printed.returncode == 0, printed.stderr
    broker = table_schema("broker")
    for statement in [lock_of(broker), *migration_statements([broker], lock=False)]:
        assert f"{statement};\n" in printed.stdout
    asyncio.run(run_script(applied_to, driver, printed.stdout))
    assert tables_in(applied_to) == {"taskiq_messages", "alembic_version"}
    # --sql ran nothing: the database the revision came from has no table of ours.
    assert "taskiq_messages" not in tables_in(generated_from)
    state = asyncio.run(read_catalog(applied_to, driver, [broker]))
    assert plan_migration([broker], state).is_current


# --- what the comparator keeps, and when it stays out --------------------------


class CompetitorOp(MigrateOperation):
    """An operation another library's comparator adds."""

    def reverse(self) -> CompetitorOp:
        """Undo nothing."""
        return self

    def to_diff_tuple(self) -> tuple[str]:
        """Name the operation in the diffs."""
        return ("competitor",)


@pytest.fixture
def competitor() -> Iterator[list[str]]:
    """
    Register a global schema comparator at the default priority, and remove it.

    Like many published ones, it reads ``metadata.tables`` and looks for the
    ``create_table`` of each table, so it depends on running after Alembic's.
    """
    seen: list[str] = []

    def compare(
        autogen_context: Any,
        upgrade_ops: ops.UpgradeOps,
        schemas: set[Optional[str]],
    ) -> None:
        for table in autogen_context.metadata.tables.values():
            created = any(
                isinstance(operation, ops.CreateTableOp)
                and operation.table_name == table.name
                for operation in upgrade_ops.ops
            )
            seen.append(f"{table.name} created={created}")
        upgrade_ops.ops.append(CompetitorOp())

    comparators.dispatch_for("schema")(compare)
    yield seen
    for entries in comparators._registry.values():
        for entry in list(entries):
            if entry is compare or (isinstance(entry, tuple) and entry[0] is compare):
                entries.remove(entry)


def test_a_competing_comparator_does_not_break_ours(
    competitor: list[str],
    new_database: Callable[[], str],
) -> None:
    script = produce(new_database(), taskiq_metadata(table_schema("broker")))

    diffs = script.upgrade_ops.as_diffs()
    assert competitor == ["taskiq_messages created=False"]
    assert diffs.count(("competitor",)) == 1
    assert [diff for diff in diffs if diff != ("competitor",)] == [
        (STEP, "taskiq_messages", 1),
        (STEP, "taskiq_messages", 2),
    ]


def test_the_comparator_is_registered_once_without_a_dialect_qualifier() -> None:
    registrations = [
        key
        for key, entries in comparators._registry.items()
        for entry in entries
        if compare_taskiq_steps in (entry, *(entry if isinstance(entry, tuple) else ()))
    ]

    (key,) = registrations
    assert key[:2] == ("schema", "default")
    try:
        from alembic.util import DispatchPriority  # noqa: PLC0415
    except ImportError:
        assert len(key) == 2  # noqa: PLR2004
    else:
        assert key[2] is DispatchPriority.LAST


def test_generic_operations_of_the_steps_go_and_other_drift_stays(
    driver: str,
    new_database: Callable[[], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    dsn = new_database()
    # Step 1 with a text task_id where the description says uuid, and a column
    # nobody describes.
    text_broker = table_schema("broker", table_name="jobs", field_for_task_id="Text")
    prepare(dsn, driver, "step-1", text_broker)
    asyncio.run(run_script(dsn, driver, "ALTER TABLE jobs ADD COLUMN stray INTEGER;"))
    metadata = MetaData()
    Table("app_users", metadata, Column("id", Integer, primary_key=True))
    add_taskiq_tables(metadata, table_schema("broker", table_name="jobs"))

    with caplog.at_level(logging.WARNING, logger="taskiq_postgresql.contrib.alembic"):
        diffs = flat(produce(dsn, metadata).upgrade_ops.as_diffs())

    # No add_column or add_index for channel and not_before: step 2 has them.
    assert sorted(diff[0] for diff in diffs) == [
        "add_table",
        "modify_type",
        "remove_column",
        STEP,
    ]
    found = {diff[0]: diff for diff in diffs}
    assert found["add_table"][1].name == "app_users"
    assert found["modify_type"][2:4] == ("jobs", "task_id")
    assert (found["remove_column"][2], found["remove_column"][3].name) == (
        "jobs",
        "stray",
    )
    assert found[STEP] == (STEP, "jobs", 2)
    assert any("column_type" in message for message in caplog.messages)


def test_a_list_of_metadata_with_the_tables_in_one_works(
    new_database: Callable[[], str],
) -> None:
    application = MetaData()
    Table("app_users", application, Column("id", Integer, primary_key=True))

    diffs = produce(
        new_database(),
        [application, taskiq_metadata(table_schema("broker"))],
    ).upgrade_ops.as_diffs()

    assert [diff[1].name for diff in diffs if diff[0] == "add_table"] == ["app_users"]
    assert [diff for diff in diffs if diff[0] == STEP] == [
        (STEP, "taskiq_messages", 1),
        (STEP, "taskiq_messages", 2),
    ]


def test_without_the_descriptions_the_comparator_stays_out(
    new_database: Callable[[], str],
) -> None:
    plain = MetaData()
    for table in taskiq_metadata(table_schema("broker")).tables.values():
        table.to_metadata(plain)

    diffs = produce(new_database(), plain).upgrade_ops.as_diffs()

    # The same tables without metadata.info: Alembic's own operations.
    assert METADATA_INFO_KEY not in plain.info
    assert {diff[0] for diff in diffs} == {"add_table", "add_index"}


def test_excluded_tables_get_no_step(new_database: Callable[[], str]) -> None:
    broker = table_schema("broker")

    diffs = produce(
        new_database(),
        taskiq_metadata(broker),
        include_object=exclude_taskiq_tables(broker),
    ).upgrade_ops.as_diffs()

    assert diffs == []


def test_another_dialect_gets_no_step() -> None:
    engine = create_engine("sqlite://")
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            diffs = produce_migrations(
                context,
                taskiq_metadata(table_schema("broker")),
            ).upgrade_ops.as_diffs()
    finally:
        engine.dispose()

    assert [diff[1].name for diff in diffs if diff[0] == "add_table"] == [
        "taskiq_messages",
    ]
    assert not [diff for diff in diffs if diff[0] == STEP]


def test_no_alembic_plugin_entry_point_is_published() -> None:
    # Before Alembic 1.20.1 such an entry point makes `import alembic` fail.
    ours = [
        entry_point
        for entry_point in entry_points(group="taskiq_postgresql.migration_formats")
        if entry_point.value.startswith("taskiq_postgresql.")
    ]
    plugins = [
        entry_point
        for entry_point in entry_points(group="alembic.plugins")
        if entry_point.value.startswith("taskiq_postgresql")
    ]

    assert ours  # the package's metadata is there to read
    assert plugins == []


# --- table names ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "table_name", "state", "expected"),
    [
        ("broker", "Jobs_Queue", "0.4-ddl", [2]),
        ("result_backend", LONG_NAME, "fresh", [1]),
    ],
    ids=["mixed-case-0.4", "long-mixed-case-fresh"],
)
def test_configured_table_names_get_their_steps(
    kind: str,
    table_name: str,
    state: str,
    expected: list[int],
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    schema = table_schema(kind, table_name=table_name)
    prepare(dsn, driver, state, schema)
    metadata = taskiq_metadata(schema)

    before = produce(dsn, metadata).upgrade_ops
    asyncio.run(
        run_script(
            dsn,
            driver,
            "".join(
                f"{statement};\n"
                for operation in before.ops
                for statement in operation.statements
            ),
        ),
    )
    after = produce(dsn, metadata).upgrade_ops

    assert before.as_diffs() == [(STEP, table_name, number) for number in expected]
    assert after.as_diffs() == []


# --- a step that relaxes NOT NULL ---------------------------------------------


def alter_column_ops(upgrade_ops: ops.UpgradeOps) -> list[ops.AlterColumnOp]:
    """Every ``AlterColumnOp`` of a revision, wherever autogenerate nested it."""
    found: list[ops.AlterColumnOp] = []
    for operation in upgrade_ops.ops:
        inner = (
            operation.ops if isinstance(operation, ops.ModifyTableOps) else [operation]
        )
        found.extend(item for item in inner if isinstance(item, ops.AlterColumnOp))
    return found


def test_autogenerate_leaves_only_the_step_for_a_column_it_relaxes(
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    schema = relaxing_schema()
    asyncio.run(
        run_script(
            dsn,
            driver,
            "".join(f"{statement};\n" for statement in schema.steps[0].statements),
        ),
    )
    metadata = taskiq_metadata(schema)

    # The metadata has the column nullable and the database has it NOT NULL:
    # without the step covering it, Alembic would propose an alter_column too.
    script = produce(dsn, metadata)

    assert alter_column_ops(script.upgrade_ops) == []
    assert script.upgrade_ops.as_diffs() == [(STEP, schema.name, 2)]
    # Putting NOT NULL back would refuse the first row that holds a NULL.
    downgrade = render_python_code(script.downgrade_ops)
    assert "SET NOT NULL" not in downgrade
    assert "alter_column" not in downgrade

    asyncio.run(
        run_script(
            dsn,
            driver,
            "".join(
                f"{statement};\n"
                for operation in script.upgrade_ops.ops
                for statement in operation.statements
            ),
        ),
    )

    assert produce(dsn, metadata).upgrade_ops.as_diffs() == []
    (row,) = asyncio.run(
        fetch_on_server(
            dsn,
            "SELECT attnotnull FROM pg_attribute"
            " WHERE attrelid = to_regclass($1) AND attname = $2",
            schema.name,
            RELAXED,
        ),
    )
    assert row[0] is False


# --- upgrade_taskiq_tables with through ----------------------------------------


@pytest.mark.parametrize("through", [1, 2])
def test_through_keeps_a_revision_at_its_step_when_the_package_gains_steps(
    through: int,
) -> None:
    broker = table_schema("broker")
    step_3 = MigrationStep(
        number=3,
        statements=("ALTER TABLE taskiq_messages ADD COLUMN IF NOT EXISTS later TEXT",),
        objects=StepObjects(creates_table=False, columns=("later",), indexes=()),
    )
    later = dataclasses.replace(broker, steps=(*broker.steps, step_3))

    before = offline_sql(lambda: upgrade_taskiq_tables(op, broker, through=through))
    after = offline_sql(lambda: upgrade_taskiq_tables(op, later, through=through))

    assert after == before
    assert before == "".join(
        f"{statement};\n\n"
        for statement in bounded(
            [
                lock_of(broker),
                *(
                    statement
                    for step in broker.steps[:through]
                    for statement in step.statements
                ),
            ],
        )
    )


@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
def test_a_revision_through_step_1_leaves_step_2_pending(
    sqlalchemy_driver: str,
    driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    broker = table_schema("broker", table_name="jobs")

    run_upgrade(
        dsn,
        sqlalchemy_driver,
        lambda: upgrade_taskiq_tables(op, broker, through=1),
    )

    plan = plan_migration([broker], asyncio.run(read_catalog(dsn, driver, [broker])))
    assert [(item.step.number, item.missing_columns) for item in plan.pending] == [
        (2, ("channel", "not_before")),
    ]


@pytest.mark.parametrize("sqlalchemy_driver", SQLALCHEMY_DRIVERS)
def test_colons_in_a_step_reach_the_server_as_written(
    sqlalchemy_driver: str,
    new_database: Callable[[], str],
) -> None:
    dsn = new_database()
    statement = (
        "CREATE TABLE IF NOT EXISTS colons (id INTEGER, "
        "note TEXT DEFAULT ':not_a_parameter', doc JSONB DEFAULT '{}'::jsonb)"
    )
    schema = dataclasses.replace(
        table_schema("scheduler_source", table_name="colons"),
        steps=(
            MigrationStep(
                number=1,
                statements=(statement,),
                objects=StepObjects(
                    creates_table=True,
                    columns=("id", "note", "doc"),
                    indexes=(),
                ),
            ),
        ),
    )
    rendered = render_python_code(
        ops.UpgradeOps([TaskiqStepOp("colons", "scheduler_source", schema.steps[0])]),
    )
    namespace: dict[str, Any] = {"op": op}
    exec(f"def upgrade():\n{rendered}\n", namespace)  # noqa: S102

    printed = offline_sql(namespace["upgrade"])
    run_upgrade(
        dsn,
        sqlalchemy_driver,
        lambda: upgrade_taskiq_tables(op, schema, through=1),
    )

    assert printed == "".join(
        f"{line};\n\n" for line in bounded([lock_of(schema), statement])
    )
    assert offline_sql(lambda: upgrade_taskiq_tables(op, schema, through=1)) == printed
    defaults = {
        row[1]: row[7]
        for row in catalog(dsn)["columns"]
        if row[0] == "colons" and row[1] in {"note", "doc"}
    }
    assert defaults == {"note": "':not_a_parameter'::text", "doc": "'{}'::jsonb"}
