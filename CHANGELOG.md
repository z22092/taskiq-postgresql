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
- The broker's table gets two columns, `channel` and `not_before`, and an
  index on them. With `run_migrations=True`, `startup()` adds them, and so do
  `taskiq postgresql migrate` and the script `taskiq postgresql generate`
  prints. Otherwise run this once per broker table before deploying (shown for
  the default `taskiq_messages`):

  ```sql
  ALTER TABLE taskiq_messages
    ADD COLUMN IF NOT EXISTS channel VARCHAR,
    ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
  CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx
    ON taskiq_messages (channel, not_before, id);
  ```

  Without it, `kick()` fails with
  `column "channel" of relation "taskiq_messages" does not exist`, and the
  workers of this version deliver nothing from the table. Each of them logs
  a failed sweep every `poll_interval`
  (`Error claiming a due message on channel ...`) and, for every NOTIFY a
  0.4.x producer sends, a failed claim of that message
  (`Error claiming message <id> on channel ...`). `ADD COLUMN` does not
  rewrite the table, but `CREATE INDEX` blocks writes to it while it builds.
  On a large table, run the upgrade before deploying even with
  `run_migrations=True`: the `ALTER TABLE` first, then, outside a transaction,
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS taskiq_messages_not_before_idx ON taskiq_messages (channel, not_before, id);`.
  `startup()` then finds the columns and the index and runs no DDL. A role
  that cannot run DDL on the table fails `startup()` with
  `run_migrations=True` until this SQL has run; after that it starts without
  DDL.
- Privileges: a role that sends tasks needs `INSERT` and `SELECT` on the
  broker's table (the `INSERT` returns the new id) and `USAGE` on its
  `<table_name>_id_seq` sequence. A worker needs `SELECT`, `UPDATE` and
  `DELETE` on it. `UPDATE` is new: the sweep locks the row it claims
  `FOR UPDATE`, which needs it, while a 0.4.x worker only deleted by id. Grant
  it before deploying the workers, for example
  `GRANT UPDATE ON taskiq_messages TO <worker_role>;`. `run_migrations=True`
  needs DDL privileges only until the columns and the index exist.
- Rows stored before the upgrade, or by a 0.4.x producer at any time, have no
  channel (`channel IS NULL`). The sweep never takes them; only the NOTIFY of
  their id delivers them, which a 0.4.x producer sends, so a row whose NOTIFY
  reached no worker stays in the table until you hand it to a channel. To
  review them:
  `SELECT id, task_name, created_at FROM taskiq_messages WHERE channel IS NULL;`
  To hand them to the workers of a channel:
  `UPDATE taskiq_messages SET channel = '<channel_name>' WHERE channel IS NULL;`
  Their `not_before` is the time of the upgrade or of their insert, so the
  next sweep delivers them at once, whatever their `delay` label said.
- Rollout order: run the SQL above, or start once with `run_migrations=True`,
  before starting the workers of this version. Run the `UPDATE` above after
  the last 0.4.x producer stops, since until then it keeps storing rows
  without a channel.
- While 0.4.x and this version run side by side, a 0.4.x worker delivers the
  messages this version kicks without a delay, but never a delayed one, which
  gets no NOTIFY: keep at least one worker of this version on each channel.
- Downgrading to 0.4.x: the columns and the index can stay, 0.4.x ignores
  them. Delayed messages stored by this version and not delivered yet stay in
  the table, because 0.4.x delivers only the ids it is notified of.

### Added

- Python 3.14 and 3.15 support.
- taskiq 0.13 support: the dependency range is now `taskiq>=0.11.7,<0.14`. The
  package no longer imports `taskiq.compat`, whose pydantic helpers taskiq 0.13
  removed, and reads a numeric `cron_offset` (written under pydantic v1) as
  seconds, which taskiq 0.13 would reject.
- `pydantic>=2.5,<3` as a runtime dependency, the floor taskiq 0.13 requires.
- `PostgresqlBroker(poll_interval=1.0)`: the seconds between two sweeps of
  `listen()` for due messages. A running worker that keeps up with its
  messages claims a delayed one within about this long after it is due. A
  value that is not a finite number of seconds above zero, or a `bool`, raises
  `ValueError`.
- `taskiq postgresql generate` prints the SQL script that creates the tables
  and indexes and adds the broker's `channel` and `not_before` columns, so it
  also upgrades a broker table 0.4.x created. It uses your table names and
  `field_for_task_id`, so you can run it yourself (`psql -f`) or add it to
  Alembic, Django or Flyway (`--no-transaction`). It reads the components from your application
  (`app:broker`, `app:scheduler`, as `taskiq worker` does) or, without
  importing it, from `--component`. The script takes the same advisory lock as
  `run_migrations=True` and is safe to run again.
- `taskiq postgresql migrate` creates the tables once, with the code
  `run_migrations=True` runs, whatever the components set. Run it before the
  workers, which can then use `run_migrations=False` and a role that only reads
  and writes rows.
- `taskiq_postgresql.migrations` describes the tables as data (`table_schema`,
  `collect_schemas`) and renders the script (`render_sql`), or its statements
  one by one (`migration_statements`, which stops after a given step with
  `through=N`), for tools that build their own migrations. Other packages can
  add output formats to `generate --format` through the
  `taskiq_postgresql.migration_formats` entry point group; the README has the
  contract.
- `taskiq_postgresql.migrations` also compares a database with that
  description. Each migration step declares the table, columns and indexes it
  creates (`MigrationStep.objects`); `catalog_query` reads, in one read-only
  query, what a database holds under those names; `plan_migration` turns that
  into the steps still pending (`required` when a table or column is missing,
  `advisory` when only an index is) and the problems no step fixes, such as an
  invalid index or an index name another table took. It runs no DDL itself.
- SQLAlchemy, SQLModel and Alembic integrations, each installed as an extra:
  `pip install "taskiq-postgresql[sqlalchemy]"`, `[sqlmodel]` or `[alembic]`.
  `taskiq_postgresql.contrib.sqlalchemy.add_taskiq_tables` adds the tables to
  a `MetaData` (and `taskiq_metadata` returns a new one) with the columns,
  defaults, primary key and indexes `run_migrations=True` creates, under the
  same names, so `create_all` builds the same tables;
  `taskiq_postgresql.contrib.sqlmodel.add_taskiq_tables` puts them on
  `SQLModel.metadata`.
- Alembic autogenerate writes the migration steps a database lacks. With
  `taskiq_metadata(...)` from `taskiq_postgresql.contrib.alembic` in
  `target_metadata` (or `add_taskiq_tables` on your own metadata and that
  module imported in `env.py`), `alembic revision --autogenerate` writes each
  missing step as `op.execute(...)` calls holding the step's SQL and the
  advisory lock, so the revision never imports this package, and
  `alembic check` fails while a step is missing from the revisions. Each
  step's `downgrade()` leaves its columns and indexes in place. Generate
  revisions against a database only Alembic migrates: where workers with
  `run_migrations=True` already ran a step, autogenerate leaves it out. The
  `alembic` extra allows Alembic 1.14 to 1.20, and both ends are tested. The
  module also gives `exclude_taskiq_tables` for
  `include_object` (without it, autogenerate proposes dropping tables that
  `run_migrations=True` created), `upgrade_taskiq_tables(op, ..., through=N)`
  for a hand-written revision when table names differ between environments,
  and `taskiq postgresql generate --format alembic`, which writes a revision
  file with every step.

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
- `PostgresqlBroker.kick()` stores, in one `INSERT`, the broker's channel and
  the time the message is due, computed by the server as `NOW()` plus the
  `delay` label. It sends a NOTIFY only for a message due at once and no
  longer schedules a timer on the event loop for a delayed one. When that
  NOTIFY fails after the `INSERT`, `kick()` logs a warning and returns instead
  of raising, because the message is stored and the next sweep delivers it; a
  caller that retried used to send it twice. An error on the `INSERT` is
  still raised, and nothing is sent again.
- `PostgresqlBroker.listen()` claims the due messages of its channel, earliest
  due first, when it starts and then every `poll_interval`, however many
  NOTIFYs arrive in between, besides the ids it is notified of. A notified id
  is claimed only once it is due, and only if its row belongs to the broker's
  channel or to none. Delivery stays at most once: a claimed row is deleted
  before the task runs.
- When reading the LISTEN connection ends or fails, `listen()` keeps
  delivering through the sweep alone for the rest of that call; the
  connection is opened again only when the broker starts again. With asyncpg
  and psycopg it logs one warning when that happens. psqlpy's listener gives
  no sign that its connection ended, so with psqlpy nothing is logged, and the
  sweep still delivers. A failed claim is logged and tried again at the next
  sweep. `listen()` returns as soon as `shutdown()` starts, so its sweep does
  not reopen the pool `shutdown()` closes.
- With `run_migrations=True`, the startup check that skips the DDL also
  requires the broker's `channel` and `not_before` columns and their index.
- The listen drivers (`ListenDriver` subclasses) yield each NOTIFY payload as
  the `str` the server sent, and the broker reads the message id from it; the
  bundled drivers used to convert it to `int`. A custom listen driver must
  yield the payload unparsed.
- A custom `QueryDriver` must implement `fetch_row(query, *values)`, which
  returns the first row of a query as a `dict`, or `None`. The broker builds
  its claim queries itself and runs them through it.

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
- A delayed message (a `delay` label, `@broker.task(delay=...)` or
  `SmartRetryMiddleware`) is delivered even when the process that sent it
  exits before the delay ends. Its NOTIFY used to come from a timer in that
  process, so the row stayed in the table for good.
- A message sent by this version whose NOTIFY reached no worker, because none
  was listening or the LISTEN connection was down, is delivered by the next
  sweep; it used to stay in the table.
- A NOTIFY on the broker's channel whose payload is not a message id is
  skipped with a warning, and the NOTIFYs after it are still delivered. The
  drivers parsed the payload in their NOTIFY callback and raised `ValueError`
  there, so the warning `listen()` logs for such a payload never ran.
- `taskiq worker` stops on SIGINT or SIGTERM while no message arrives. With the
  asyncpg and psqlpy drivers its worker processes used to keep running until a
  message arrived (with asyncpg, until its connection pool's idle timer fired
  about five minutes later), and the main process waited for them.

### Removed

- Python 3.9.
- pydantic v1 support.
- `QueryDriver.delete_returning()` and `DeleteReturningQuery`, which nothing
  uses since the broker claims rows through `fetch_row()`.
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
