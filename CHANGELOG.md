# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

Upgrading from 0.4.x:

- Pass `dsn` to `PostgresqlBroker`, `PostgresqlResultBackend` and
  `PostgresqlSchedulerSource`: there is no default any more. Read it from your
  configuration, for example `dsn=os.environ["POSTGRESQL_URL"]`.
- A callable `dsn` is called once, by `startup()`, and the component cannot
  connect before that. Call `startup()` before using a component built with
  one.
- Schedules stored under pydantic v1 with a `timedelta` `cron_offset` hold it as
  a number of seconds; they load unchanged. `get_schedules()` now skips, with a
  warning in the log, any stored schedule it cannot read: check the log after
  upgrading and fix or delete the rows it names.

### Added

- Python 3.14 and 3.15 support.
- taskiq 0.13 support: the dependency range is now `taskiq>=0.11.7,<0.14`. The
  package no longer imports `taskiq.compat`, whose pydantic helpers taskiq 0.13
  removed, and reads a numeric `cron_offset` (written under pydantic v1) as
  seconds, which taskiq 0.13 would reject.
- `pydantic>=2.5,<3` as a runtime dependency, the floor taskiq 0.13 requires.
- `taskiq postgresql generate` prints the SQL script that creates the tables
  and indexes, with your table names and `field_for_task_id`, so you can run
  it yourself (`psql -f`) or add it to Alembic, Django or Flyway
  (`--no-transaction`). It reads the components from your application
  (`app:broker`, `app:scheduler`, as `taskiq worker` does) or, without
  importing it, from `--component`. The script takes the same advisory lock as
  `run_migrations=True` and is safe to run again.
- `taskiq postgresql migrate` creates the tables once, with the code
  `run_migrations=True` runs, whatever the components set. Run it before the
  workers, which can then use `run_migrations=False` and a role that only reads
  and writes rows.
- `taskiq_postgresql.migrations` describes the tables as data (`table_schema`,
  `collect_schemas`) and renders the script (`render_sql`), for tools that
  build their own migrations. Other packages can add output formats to
  `generate --format` through the `taskiq_postgresql.migration_formats` entry
  point group; the README has the contract.

### Changed

- Runtime dependencies use closed ranges (`>=floor,<ceiling`): `taskiq`,
  `typing-extensions`, `pydantic` and the `asyncpg`, `psqlpy` and `psycopg`
  extras. `typing-extensions>=4.0.0,<5` is declared.
- `requires-python` is `>=3.10,<3.16`. On a Python outside that range, pip
  installs an older release of this package instead of this one.
- A callable `dsn` is resolved once, by `startup()`, and every driver of the
  component gets that string. The broker used to call it at construction.
- `PostgresqlSchedulerSource.get_schedules()` skips a stored schedule it cannot
  read as a `ScheduledTask` (not JSON, not a JSON object, or not valid) instead
  of failing for the whole table, so `startup()` with a `startup_schedule` keeps
  going too. The warning names the row id and the error type, never the stored
  value.
- `PostgresqlBroker.startup()` raises `DatabaseConnectionError` when the LISTEN
  connection cannot start, for every driver (asyncpg, psqlpy and psycopg).
- The drivers pair query columns and values with `zip(..., strict=True)`, so a
  count mismatch raises `ValueError` instead of silently dropping values.
- With `run_migrations=True`, `startup()` runs no DDL and takes no lock when the
  table and every index it creates already exist. A role with `USAGE` on the
  schema, `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the tables and `USAGE`
  on the broker's sequence can now start with it; every start used to need
  `CREATE` on the schema and ownership of the table.

### Fixed

- A schedule stored with a `timedelta` `cron_offset` loads back as a
  `timedelta` on taskiq older than 0.13 too. It used to come back as its ISO
  8601 string (`"PT1H"`), which the scheduler took for a time zone name and
  failed on.
- A callable `dsn` works with `PostgresqlResultBackend` and
  `PostgresqlSchedulerSource`; it used to reach the driver uncalled.
- `PostgresqlBroker.kick()` accepts a fractional `delay` label, such as the
  ones `SmartRetryMiddleware` and `@broker.task(delay=2.5)` send; it used to
  raise `ValueError` after storing the message, which was then never
  delivered. A `delay` that is not a finite number of seconds still raises
  `ValueError`, now before anything is stored.
- Concurrent first startups with `run_migrations=True`, such as the two
  processes `taskiq worker` starts by default, no longer fail with
  `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`.
  The migration runs on one connection, in one transaction that first takes
  `pg_advisory_xact_lock(hashtext('taskiq_postgresql'), hashtext(<table_name>))`
  with the table name as configured. This lock key stays the same across
  releases, so two versions running side by side in a rolling deploy wait for
  each other too.
- `run_migrations=True` works with a pool of one connection (asyncpg or psycopg
  `min_size=1, max_size=1`). The migration used to hold one connection while it
  waited for a second: asyncpg waited with no timeout, psycopg until its pool
  timeout.

### Removed

- Python 3.9.
- pydantic v1 support.
- **BREAKING:** the default `dsn` of `PostgresqlBroker`,
  `PostgresqlResultBackend` and `PostgresqlSchedulerSource`, which embedded a
  user name and password for a server on localhost. `dsn` is now a required
  first parameter: omitting it raises Python's own `TypeError`, and type
  checkers flag the call. `None` or an empty string, given directly or returned
  by a callable `dsn`, raises `TypeError` with an example of the expected value.

Earlier versions are not described in this file; see the
[GitHub releases page](https://github.com/z22092/taskiq-postgresql/releases) and
the [PyPI release history](https://pypi.org/project/taskiq-postgresql/#history).

[unreleased]: https://github.com/z22092/taskiq-postgresql/compare/v0.4.1...HEAD
