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

  `taskiq postgresql plan` prints the same statements, with the advisory lock
  and a lock timeout, for your own table names, and `taskiq postgresql check`
  tells whether they have run. Without them, a component of this version with
  `run_migrations=False` refuses to start: `startup()` raises
  `SchemaNotReadyError`, naming the missing columns. `ADD COLUMN` does not
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

- A migration step can now make a column nullable: `StepObjects` has a
  `nullable_columns` field, and `PendingStep` a `not_null_columns` field naming
  the columns of a pending step that are still `NOT NULL` (`taskiq postgresql
  check --json` reports it as `not_null_columns`). Such a step is `required`
  (`check` exits 3 while it is pending), and the Alembic integration leaves its
  `ALTER COLUMN ... DROP NOT NULL` to the step instead of proposing it twice.
  No step of this release relaxes a column.
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
  `run_migrations=True` and is idempotent. It still locks the tables when
  nothing is missing, so on a live database it starts with
  `SET LOCAL lock_timeout = '10s'` and fails after that long without a lock
  instead of stalling the broker; `--lock-timeout SECONDS` changes the limit
  and `--lock-timeout 0` leaves it out. `migrate` runs no DDL when everything
  exists.
- `taskiq postgresql migrate` creates the tables once, with the code
  `run_migrations=True` runs, whatever the components set. Run it before the
  workers, which can then use `run_migrations=False` and a role that only reads
  and writes rows.
- `taskiq postgresql plan` reads one database's catalog, in one read-only
  query, and prints only the SQL of the migration steps that database still
  lacks: the script of `generate`, with its options (`--lock-timeout`,
  `--no-transaction`, `--no-lock`, `--format alembic` for a revision of the
  pending steps), under a comment that names the database and `search_path` it
  is valid for, and nothing when every step has run. Any format, this
  package's or another's, that takes a keyword-only `comments` parameter gets
  that comment as the first line of its file; a name that holds a line break
  or an encoding declaration (`coding: ...`, which Python reads on a file's
  first two lines) is written escaped. The locks each step takes and the
  problems no step fixes go to standard error.
- `taskiq postgresql check` reads the same catalog, changes nothing, and
  reports the pending steps and the problems on standard error (and as JSON
  with `--json`). It exits with 0 when the database is up to date or lacks
  only an index, 3 when a table or column is missing, and 4 when a problem no
  step fixes needs a person, such as an invalid index; 1 stays for errors, so
  a deploy can tell a forgotten migration from a failed connection. Both
  commands read each component's own database, or `--dsn`, and need no DDL
  privilege; `--component` describes one table without the application.
- `QueryDriver.component_kind`, which the components set on their driver, at
  construction and again in `startup()`, so a driver swapped in later gets it
  too. The bundled drivers' `on_startup()` describes the table with it and
  compares the database with that description, with `run_migrations` or
  without. A driver built without a component checks nothing without
  `run_migrations`; with `run_migrations=True` it probes its table and indexes
  as before (`RelationsExistQuery`) and takes no lock and runs no DDL once they
  exist.
- `taskiq_postgresql.migrations` describes the tables as data (`table_schema`,
  and `collect_schemas`, which also takes `table_schema(...)` descriptions and
  describes each table once) and renders the script (`render_sql`), or its
  statements one by one (`migration_statements`, which stops after a given
  step with `through=N`; `step_statements`, which also bounds their lock waits
  inside a transaction they share), for tools that build their own
  migrations. Other packages can add output formats to `generate --format`
  through the `taskiq_postgresql.migration_formats` entry point group; the
  README has the contract. A format that writes another tool's migration file
  can start from `render_steps`, which refuses a comment, an option or a table
  name the file cannot hold and lays out each step with its lock and lock
  timeout.
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
  file with every step. Like the script of `generate`, each step bounds its
  lock waits, 10 seconds by default, so it fails and rolls the revision back
  instead of stalling the broker on a live table; then it gives the
  transaction back its own `lock_timeout`, so the revision's other
  migrations wait as before. Set the seconds with
  `context.configure(..., taskiq_postgresql_lock_timeout=N)` in `env.py`
  (autogenerate writes the value into the revision),
  `upgrade_taskiq_tables(..., lock_timeout=N)` or
  `generate --format alembic --lock-timeout N`; `0` leaves the bound out. The
  lock and the bound only work inside the revision's transaction, so
  `upgrade_taskiq_tables` raises `RuntimeError` on an autocommit connection
  (for example inside `autocommit_block()`) before it runs a statement. The
  format and `TaskiqStepOp` refuse a table name that is not a plain SQL
  identifier, a line break in a table name, or an unknown component kind, the
  format refuses a comment that holds an encoding declaration, and
  `downgrade=drop` bounds its `DROP TABLE` statements by the same lock timeout.
- A Django app, installed with the `django` extra
  (`pip install "taskiq-postgresql[django]"`), which allows Django 5.2 LTS
  (5.2.19 or later) and, on Python 3.12 and later, Django 6.0 and 6.1. Add
  `"taskiq_postgresql.contrib.django"` to `INSTALLED_APPS` and name the
  components in `TASKIQ_POSTGRESQL_TARGETS`, as `module:object` paths or as
  dicts such as `{"kind": "broker", "table_name": "jobs"}`:
  `manage.py migrate taskiq_postgresql` then creates their tables with the SQL
  `run_migrations=True` runs, one migration per migration step, each in its
  transaction with the advisory lock and a lock timeout
  (`TASKIQ_POSTGRESQL_LOCK_TIMEOUT`, 10 seconds by default, `0` for none),
  after which the transaction gets its own `lock_timeout` back. The
  migrations read the settings when they run: missing or empty settings, or a
  table name that is not a plain SQL identifier, fail `migrate` with
  `ImproperlyConfigured` before any statement runs, and `manage.py check`
  reports them as `taskiq_postgresql.E001`, except the names behind a
  `module:object` path, which only `migrate` imports. Reversing the app's migrations
  drops nothing of its tables. A table added to the settings after the first
  `migrate` comes from `taskiq postgresql migrate` or `plan`, or from a
  migration of your own: `taskiq postgresql generate --format django` writes
  a migration file with every step, which never imports this package (option
  `dependencies`), and `plan --format django` writes one with only the
  pending steps. The format imports no Django, so neither command needs the
  `django` extra.
- `PostgresqlBroker(health_check_interval=30.0, query_timeout=30.0)`. When the
  LISTEN connection stays silent for `health_check_interval` seconds (with
  psycopg, every `health_check_interval` seconds), the worker sends the same
  LISTEN again and reopens the connection if no answer comes within
  `query_timeout` seconds; a NAT or a load balancer can drop a
  connection without either side being told. `query_timeout` also bounds each
  claim with asyncpg and psycopg, and each close in `shutdown()`. It does not
  bound psqlpy's claims: psqlpy can neither cancel a query on the server nor
  drop one pooled connection, so a claim given up would leave its `DELETE` to
  run later; with psqlpy, set `tcp_user_timeout_sec` and the `keepalives_*`
  settings in `connection_kwargs` instead. Both refuse what `poll_interval`
  refuses. `QueryDriver.query_timeout` and `ListenDriver.health_check_interval`
  / `ListenDriver.query_timeout` carry them to the drivers and default to
  `None` (no check, no bound). A custom driver gets the reopening of LISTEN,
  but no health check and no bound unless it reads these attributes.

### Changed

- **BREAKING:** with `run_migrations=False` (the default), `startup()` checks
  the component's table before anything else starts. It reads the catalog in
  one read-only query, through the component's own pool, and runs no DDL. When
  the table, or a column a migration step adds (such as the broker's `channel`
  and `not_before` on a table 0.4.x created), is missing, it raises
  `SchemaNotReadyError` from `taskiq_postgresql.exceptions`, which names the
  table, the step and the columns; such a component used to start and fail on
  its first query. A missing index, or an index no step can fix, only logs a
  warning. What to do: create the tables before the workers start, with
  `taskiq postgresql migrate`, with the SQL `taskiq postgresql plan` prints,
  with an Alembic revision (`alembic revision --autogenerate`), or by starting
  once with `run_migrations=True`. The check opens the component's pool at
  startup, so on a database it cannot reach `startup()` now fails with
  `DatabaseConnectionError`, for every component, before the worker starts,
  as it already did with `run_migrations=True`. With psycopg it fails once
  the pool has waited for a connection for its `timeout`, 30 seconds by
  default (`psycopg_pool.AsyncConnectionPool`). With `run_migrations=True`,
  `startup()` also logs a warning for each problem no step fixes, such as an
  invalid index or an index name another table took, which `IF NOT EXISTS`
  skips.
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
- When reading the LISTEN connection ends or fails, `listen()` logs one
  warning, keeps delivering through the sweep and reopens the connection
  through the listen driver's `on_shutdown()` and `on_startup()`, waiting
  0.5 s and then twice as long after each failed attempt, up to 30 s. Each
  failed attempt logs a warning and a reopened connection an INFO line. A
  custom listen driver gets this as long as both methods can run again. All
  three drivers now notice a dropped connection, psqlpy included: its listen
  driver reads the listener by iterating, which raises when the server ends
  the connection, instead of through a background callback. A failed claim is
  logged and tried again at the next sweep. `listen()` returns as soon as
  `shutdown()` starts, and `shutdown()` stops a reopen in progress, so neither
  the sweep nor a reopen brings back what `shutdown()` closes.
- The psycopg listen driver reads notifications only through `notifies()`; it
  no longer registers a notify handler that queued every payload a second
  time and never drained it.
- The asyncpg listen driver no longer sends UNLISTEN before closing: closing
  ends the LISTEN on the server.
- The warning logged when LISTEN stops now ends with "while it is reopened"
  instead of "the LISTEN connection is not reopened until the worker
  restarts"; each failed reopen logs "Could not reopen the LISTEN connection
  of channel ...", and a claim with no answer "Claiming ... got no answer
  within ... s".
- With asyncpg, the broker's pool now gives up after `query_timeout` seconds
  waiting for a free connection, so `kick()` raises `asyncio.TimeoutError`
  instead of waiting for good when the pool stays exhausted that long.
- Custom drivers: the broker calls a listen driver's `on_shutdown()` and then
  `on_startup()` to reopen LISTEN, so both must work any number of times and
  `on_shutdown()` must not fail on a connection that is already gone. A reopen
  gives `on_startup()` `query_timeout` seconds, and the broker gives each
  driver's `on_shutdown()` twice that before it stops waiting, so a driver
  should bound its own close by `query_timeout`, as the bundled ones do. The
  asyncpg and psycopg query drivers bound `fetch_row()` by `query_timeout`
  themselves and raise `asyncio.TimeoutError`; the broker no longer cancels it
  from outside, which would leave psycopg 3.3.4 and older waiting on a silent
  socket and psqlpy running the claim after it was given up.
- With `run_migrations=True`, the startup check that skips the DDL also
  requires the broker's `channel` and `not_before` columns and their index.
- The listen drivers (`ListenDriver` subclasses) yield each NOTIFY payload as
  the `str` the server sent, and the broker reads the message id from it; the
  bundled drivers used to convert it to `int`. A custom listen driver must
  yield the payload unparsed.
- A custom `QueryDriver` must implement `fetch_row(query, *values)`, which
  returns the first row of a query as a `dict`, or `None`. The broker builds
  its claim queries itself and runs them through it.
- The `driver` parameter of the three components is typed
  `Literal["asyncpg", "psqlpy", "psycopg"]`, so type checkers reject
  `"pg8000"`. It was listed before but no release implemented it: passing it
  raised `ValueError` at construction, as it still does.
- The asyncpg query driver's `connection()`, used outside `async with driver`,
  raises `DatabaseConnectionError` like the psqlpy and psycopg drivers do,
  instead of `AttributeError`.

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
- `PostgresqlBroker.shutdown()` and `PostgresqlSchedulerSource.shutdown()`
  raised `AttributeError`, for every driver, when the component had run no
  query since `startup()` with `run_migrations=False`: for example a worker
  that received no message, or a schedule source never asked for its
  schedules. A query driver's `on_shutdown()` now does nothing when no query
  opened its pool.
- `PostgresqlResultBackend.shutdown()` closes the backend's connection pool,
  which it used to leave open. A second call, or a call before `startup()`,
  does nothing.
- The warning for a NOTIFY payload that is not a message id holds at most
  the first 64 characters of it, followed by its length when it is longer,
  because any role that can connect can notify the channel. An integer
  outside the range of the broker's `id` column (1 to 2147483647) is not a
  message id either: it gets that warning, where its claim used to log an
  error with the whole number and a traceback, or match no row.
- `InsertOrUpdateQuery.make_query(..., on_conflict_action="NOTHING")` raised
  `TypeError`; it now names the conflict columns.
- `shutdown()` no longer hangs on a connection that stopped answering: the
  asyncpg pool and LISTEN connection are closed by force once `query_timeout`
  runs out, and the whole of it takes at most about twice `query_timeout`. A
  second call does nothing.
- With asyncpg and psycopg, a claim on a pooled connection that stopped
  answering no longer blocks `listen()`: it is given up after `query_timeout`
  seconds (asyncpg may take up to about twice that) and logged, and asyncpg
  now drops that connection from the pool instead of waiting on it until the
  operating system gives up on the socket.
- With psqlpy, a NOTIFY sent right after a worker starts, or right after its
  LISTEN connection is reopened, is delivered at once. psqlpy sends `LISTEN`
  only once reading begins, so such a NOTIFY could reach no listener.

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

### Security

- The error `taskiq postgresql migrate`, `plan` and `check` print when a
  driver fails on a connection string no longer holds its password. When the
  driver cannot read the string at all, as asyncpg with an unencoded `/` in
  the password or psqlpy with an unquoted space in a `key=value` password,
  its message quoted part of the password: it is left out, and the error says
  what to check instead. Any other driver message loses the connection string
  and its password, as written and percent-decoded. Other users of the
  machine can read a command line, so give `--dsn` a connection string
  without the password, and the password through a password file or
  `PGPASSWORD`, which asyncpg and psycopg read and psqlpy does not.

Earlier versions are not described in this file; see the
[GitHub releases page](https://github.com/z22092/taskiq-postgresql/releases) and
the [PyPI release history](https://pypi.org/project/taskiq-postgresql/#history).

[unreleased]: https://github.com/z22092/taskiq-postgresql/compare/v0.4.1...HEAD
