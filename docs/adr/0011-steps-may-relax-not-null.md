# 11. Migration steps may relax NOT NULL

- Status: Accepted
- Date: 2026-11-25

## Context

The migration engine (`taskiq_postgresql/migrations.py`) described every step
as additive: it creates a table, adds a column or adds an index, each with
`IF NOT EXISTS`, and the DDL rule in `AGENTS.md` and `docs/architecture.md`
said "additive only". A step is pending when one of the objects it creates is
absent, so running it again completes it.

Task progress is going to be a column of the result row: a task that reports
progress stores it before it has a result, so the row exists while `result` is
still empty. The result table's `result` column is `NOT NULL` today, and a row
without a result needs `ALTER TABLE <results> ALTER COLUMN result DROP NOT
NULL`. That is not an addition, and the engine had no way to declare it, to
tell whether a database still needs it, or to keep Alembic from proposing it a
second time.

## Decision

- A step may make columns nullable. `StepObjects.nullable_columns` names them,
  and `DropNotNullQuery` in `abc/query.py` builds the statement, as every other
  DDL statement comes from a builder. `TableSchema` refuses a step that makes
  nullable a column the table does not have, one its final description has
  `NOT NULL`, or one that no step with a lower number creates or adds: the
  statement would fail on a table built from scratch.
- `catalog_query` also reads `pg_attribute.attnotnull`, beside the columns and
  in the same query. A step is pending while one of its `nullable_columns`
  exists and is still `NOT NULL`; `PendingStep.not_null_columns` names them.
  Such a step is `required`, because the components would violate `NOT NULL`
  without it. A column the table lacks is reported by the earlier step that
  adds it, and the step that relaxes it is pending in the same run, so the plan
  and the statements it renders carry both; when `CREATE TABLE` created the
  column, the report is a `missing_column` problem instead, and no step runs
  for it.
- The rule becomes "additive, or relaxes `NOT NULL`; nothing is removed or
  renamed". Dropping a column, changing a type or putting `NOT NULL` back stay
  out of the steps.
- The Alembic comparator removes the `alter_column` Alembic proposes when its
  only change makes a column of a pending step nullable, so a revision holds
  the `TaskiqStepOp` and nothing else for it. The downgrade of such a step
  does nothing, and its comment says that `NOT NULL` is not put back: `SET NOT
  NULL` would fail on the first row that holds a `NULL`. The revision's
  `downgrade=drop` option drops the tables instead, in `generate`; `plan`
  refuses it when a step is pending on a table that exists. Nothing ever puts
  `NOT NULL` back.
- The text a step writes into a generated file says what the step does: the
  comment of a step that makes a column nullable says so, and the text of every
  released step stays as it was.
- This change adds the kind of step only. No released step changes, and no step
  is added to a real table; `tests/golden/migration_steps.sql` stays as it is.

## Consequences

- `ALTER COLUMN ... DROP NOT NULL` takes an `ACCESS EXCLUSIVE` lock on the
  table, as `ADD COLUMN` does. It is brief, since the statement does not scan
  the table, and bounded by the `lock_timeout` the steps set (10 seconds by
  default) in the scripts, the Alembic revisions and the Django migrations.
  `PendingStep.notes` says so for each pending step.
- The engine runs the step only while it is pending, so a table that is already
  nullable takes no lock at all on startup.
- An older release that reads rows a newer one wrote may meet a `NULL` in a
  column it assumes is set. Whichever release adds the first such step carries
  the upgrade note: the order to upgrade the readers and the writers in.
- A release that adds such a step must also make `run_migrations=True` run its
  statement: the drivers' `on_startup` still runs their own builders, and
  `tests/test_migration_script.py::test_the_steps_are_the_sql_run_migrations_executes`
  fails until the two agree.
- Relaxing a column of a real table also needs the relaxed column carried
  through `_TableParts` and `_schema` in `migrations.py`, so that only the
  final description has it nullable while step 1's `CREATE TABLE` (which
  `tests/golden/migration_steps.sql` pins) still writes it `NOT NULL`. This
  change leaves `_schema` as it is: the tests describe their own tables.
- `taskiq postgresql check --json` reports `not_null_columns` for each pending
  step, and `check` exits 3 while a column is still `NOT NULL`.

## Update (2026-11-26)

The first real use of the kind of step: step 2 of the result backend's table
adds the `progress` column and makes `result` nullable. It is the first step
that changes a column instead of only adding, so the rule above applies to a
real table now, with the upgrade note it called for ("update every process
that reads results first, then the workers", in the changelog). The original
text stays as written; three of its statements moved with the code:

- `DropNotNullQuery` is built in `taskiq_postgresql/core/sql/catalog/ddl.py`;
  `taskiq_postgresql/abc/query.py` only re-exports it.
- The relaxed column is carried by the table's declaration, not by
  `_TableParts`: `TableDeclaration.steps` is an ordered list of
  `DeclaredStep`, and a step's `nullable_columns` names the columns it relaxes.
  `migrations._schema` makes them nullable in the final description only, so
  step 1's `CREATE TABLE`, which `tests/golden/migration_steps.sql` pins, still
  writes `result BYTEA NOT NULL`.
- `run_migrations=True` runs each step through `_migrate` in
  `core/stores/startup.py`, which builds a step's statements with the same
  catalog function (`declared_step_statements`) that `migrations._schema` uses
  for the description, instead of the drivers' `on_startup` running builders of
  their own.
