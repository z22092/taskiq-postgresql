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
  nullable a column the table does not have, or one its final description has
  `NOT NULL`.
- `catalog_query` also reads `pg_attribute.attnotnull`, beside the columns and
  in the same query. A step is pending while one of its `nullable_columns`
  exists and is still `NOT NULL`; `PendingStep.not_null_columns` names them. Such
  a step is `required`, because the components would violate `NOT NULL`
  without it. A column the table lacks is the business of the step that adds
  it, so it is not reported twice.
- The rule becomes "additive, or relaxes `NOT NULL`; nothing is removed or
  renamed". Dropping a column, changing a type or putting `NOT NULL` back stay
  out of the steps.
- The Alembic comparator removes the `alter_column` Alembic proposes when its
  only change makes a column of a pending step nullable, so a revision holds
  the `TaskiqStepOp` and nothing else for it. The downgrade of such a step does
  nothing, with `downgrade=drop` too: `SET NOT NULL` would fail on the first row
  that holds a `NULL`.
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
- `taskiq postgresql check --json` reports `not_null_columns` for each pending
  step.
