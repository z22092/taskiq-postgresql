# Architecture

How `taskiq-postgresql` is put together. Decisions and their reasons are in
[`adr/`](adr/); commands and conventions are in [`../AGENTS.md`](../AGENTS.md).

## Planned for 1.0

This page describes the code as it is; the data layer proposed for 1.0 is in
ADRs [0007](adr/0007-driver-port-and-core-owned-sql.md),
[0008](adr/0008-identifiers.md), [0009](adr/0009-delivery-modes.md) and
[0010](adr/0010-plugin-registry-and-codec-interface.md), with the plan in
[`spikes/2026-11-25-v1-data-layer.md`](spikes/2026-11-25-v1-data-layer.md).
Payload encryption is proposed in
[ADR 0012](adr/0012-payload-encryption-codec.md) and the result serializer
without pickle in
[ADR 0013](adr/0013-result-serializer-without-pickle.md).

## Components

Three classes implement taskiq's extension points. Each builds its own driver
at construction, so creating one never opens a connection.

| Component | Module | taskiq base | Table (default name) |
|---|---|---|---|
| `PostgresqlBroker` | `taskiq_postgresql/broker.py` | `AsyncBroker` | `taskiq_messages` |
| `PostgresqlResultBackend` | `taskiq_postgresql/result_backend.py` | `AsyncResultBackend` | `taskiq_results` |
| `PostgresqlSchedulerSource` | `taskiq_postgresql/scheduler_source.py` | `ScheduleSource` | `taskiq_schedulers` |

Every component takes a `dsn` (a string, or a callable returning one) and a
`driver` name. The DSN helpers live in `utils/dsn.py`: `require_dsn` rejects
`None`, an empty or blank string, or a value that is neither a string nor a
callable with `TypeError` at construction. A callable is resolved once, by
`startup()` (`resolve_dsn`), which hands the string to every driver of the
component; until then the drivers hold an empty connection string
(`dsn_before_startup`) and refuse to connect. The broker also builds a listen
driver for `LISTEN`.

## Driver abstraction

`taskiq_postgresql/abc/driver.py` defines the two interfaces:

- `QueryDriver` holds the table definition (primary key, columns,
  `created_at`, index columns, and the `added_columns` and `added_indexes`
  that `on_startup` adds to a table that may already exist) and one builder
  per statement from `core/sql/catalog/`, created in its `__init__`. Drivers
  implement `insert`, `insert_or_update`, `delete`, `fetch_row`,
  `delete_by_date`, `select`, `exists`, `execute`, `on_startup` and
  `on_shutdown`. `fetch_row(query, *values)` runs a query its caller built
  and returns the first row as a `dict`, or `None`; the broker runs its claims
  through it. Every query method enters `async with driver`, which creates the
  connection pool on first use; `on_shutdown` closes it, and does nothing when
  no query ever created it or a previous call closed it. The `query_timeout`
  attribute (seconds; `None`, the default, waits for as long as it takes) is
  set by the broker on its own driver. asyncpg and psycopg bound `fetch_row`
  and `on_shutdown` by it, and `fetch_row` raises `asyncio.TimeoutError` when
  it runs out. psqlpy does not bound its claims, since it can neither cancel
  a query on the server nor drop one pooled connection; its close returns at
  once.
- `ListenDriver` owns the connection that listens on the channel (for psqlpy,
  a pool listener). Iterating it (`async for`) yields the payload of each
  `NOTIFY` unparsed, as the `str` the server sent, and ends or raises when
  the connection is lost. The broker reopens a lost connection by calling
  `on_shutdown()` and then `on_startup()`, so both must work any number of
  times. The broker also sets its `health_check_interval` and `query_timeout`
  attributes (`None` by default: no check, no bound). The bundled drivers
  then check a connection that stayed silent for `health_check_interval`
  seconds (psycopg: every `health_check_interval` seconds) by sending the same
  `LISTEN` again (`check_listen` in `drivers/_health.py`); with no answer
  within `query_timeout`, they close the connection and the iterator raises
  `DatabaseConnectionError`.

`utils/get_db_driver.py` and `utils/get_db_listen_driver.py` map the `driver`
name to a class. `drivers/__init__.py` imports a driver module only when
`utils/libs_available.py` finds its library, so a missing extra raises
`ImportError` naming `taskiq-postgresql[<driver>]`, and an unknown name raises
`ValueError`.

### The three drivers

| | asyncpg (`drivers/_asyncpg.py`) | psqlpy (`drivers/_psqlpy.py`) | psycopg (`drivers/_psycopg.py`) |
|---|---|---|---|
| Pool | `asyncpg.create_pool` | `psqlpy.ConnectionPool` | `psycopg_pool.AsyncConnectionPool` with `AsyncRawCursor`, so `$n` placeholders work |
| JSONB values | dict -> JSON text (`pydantic_core.to_json`) | wrapped in `psqlpy.extra_types.JSONB` (JSON text is parsed first) | dict -> JSON text (`pydantic_core.to_json`) |
| UUID values | `UUID` -> hex text | text -> `uuid.UUID` | `UUID` -> hex text |
| Listen | `asyncpg.connect` + `add_listener` callback into a queue; a termination listener ends the iteration when the connection closes | pool `listener()`; `add_callback` with a no-op registers the channel, `LISTEN <channel>` on the listener's connection subscribes before `on_startup()` returns (psqlpy sends its own only on the first read), and a task iterates the listener into a queue, then queues the error that ended it | autocommit connection, `LISTEN <channel>`, then `notifies(timeout=health_check_interval)` in rounds, with no notify handler |
| Bounded waits (`query_timeout`) | `pool.acquire(timeout=...)`, `fetchrow(timeout=...)`; a close that runs out aborts the connections (`terminate()`) | the startup `LISTEN`, the LISTEN health check and its close only | `_answered_within` shuts the socket down and closes the connection when a query runs out; `pool.close(timeout=...)` |

The broker is the only place a payload is parsed: `_read_notified_ids` in
`broker.py` reads the message id with `_parse_message_id`. A payload that is
not an integer from 1 to 2147483647, the range of the broker's `id` column,
is logged, cut to its first 64 characters (`_payload_excerpt`) because any
role that can connect can notify the channel, and skipped.

asyncpg and psycopg shape their bind parameters with the shared
`encode_params` in `drivers/_params.py`; psqlpy has its own
`encode_psqlpy_params`. Both pair columns and values with
`zip(..., strict=True)`, so a count mismatch raises `ValueError` before
anything reaches the server.

## SQL

`taskiq_postgresql/core/sql/catalog/` is the one owner of SQL: it builds
every DDL statement, row query, the `pg_notify` call, the `LISTEN` script and
the catalog read, each as a `Statement` or `Script` that declares the type of
its binds and result columns (`statement.py`). `abc/query.py` re-exports the
builders for third-party drivers. No other module formats SQL, and a test that
reads the source keeps it so.

Values are always bind parameters (`$1`, `$2`, ...). A table or channel name
is checked by `Identifier` (`core/sql/identifier.py`, ADR 0008) when a
component is built, so only a plain, unreserved ASCII name of at most 63 bytes
reaches the SQL, unquoted; PostgreSQL folds it to lower case, and a name with
upper case letters warns. The broker sends its notification as
`SELECT FROM pg_notify($1, $2)` with the channel and the id as binds. The
channel in `LISTEN` is the one name written into a runtime statement besides
the table name, and all three drivers write it folded, so each one receives the
notification, a mixed-case channel included.

### Tables

Created by `on_startup` only when the component gets `run_migrations=True`,
with `CREATE TABLE IF NOT EXISTS` and hash indexes. The broker's table then
gets its `channel` and `not_before` columns and a btree index on them from
`AddColumnsQuery` (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, then `CREATE
INDEX IF NOT EXISTS`), so a table created by 0.4.x ends up with the same
columns as a new one. The DDL is additive, or relaxes `NOT NULL`
(`DropNotNullQuery`; [ADR 0011](adr/0011-steps-may-relax-not-null.md)): nothing
is removed or renamed. `tests/unit/test_components.py::test_default_schema`
pins the `CREATE TABLE` and hash index DDL below, and
`tests/test_migrations.py::test_add_claim_columns_query_is_the_upgrade_sql` the
added columns and their index.

The three drivers' `on_startup` is one flow, `start_table` in
`drivers/_schema.py`; each driver lends it only its primitives, a `Session`
(fetch one value, execute a statement or a script) on one pooled connection in
one transaction. With `run_migrations` it first reads the catalog with the
comparison engine described below (`read_catalog`, then `plan_migration`); if
no migration step is pending, it returns without a lock or DDL, so a role with
only DML privileges can start. Otherwise it takes
`pg_advisory_xact_lock(hashtext('taskiq_postgresql'), hashtext(<table name>))`
(`MigrationLockQuery`) and runs the DDL in that transaction: `CREATE TABLE`,
the hash indexes, then the added columns and their index, before the commit.
A concurrent startup waits for the commit, and its `IF NOT EXISTS` then finds
the objects. The lock key stays the same across releases, so two versions in a
rolling deploy also wait for each other. Then it logs a warning for each
problem no step fixes, which `IF NOT EXISTS` skipped.

Without `run_migrations`, `on_startup` runs the same catalog query through
`fetch_row`, on the driver's own pool, and no DDL. A missing table or column,
or a problem the components cannot work with, raises `SchemaNotReadyError`,
which names the table, the steps, the missing columns and indexes, and, when
a step is what is missing, the commands that run it (`HOW_TO_MIGRATE`); a
missing index, or an index no step can fix, is logged as a warning. The driver
describes its table from its own builders and `QueryDriver.component_kind`
(`table_schema(driver)`), which each component sets at construction and again
in `startup()`. A driver no component owns cannot describe its table: without
`run_migrations` it checks nothing, and with it it keeps the probe of the
releases before the engine, `RelationsExistQuery`, and skips the lock and the
DDL once its table and indexes exist.

`taskiq_postgresql/migrations.py` describes each table twice, from the same
builders: as columns and indexes (`TableSchema`), and as the ordered
statements `on_startup` runs (`MigrationStep`: step 1 is `CREATE TABLE` and
its indexes; step 2, on the broker's table only, the added columns and their
index). `taskiq postgresql
generate` (`cli.py`) renders the steps as a script for another migration tool,
with the same advisory lock and, by default, a `SET LOCAL lock_timeout` first,
so that a run against a live database fails instead of queueing behind open
transactions. `taskiq postgresql migrate` runs the migration of
`run_migrations=True` once. `taskiq postgresql plan` and `check` read one
database's catalog, in one `SELECT` that also returns `current_database()`
and `current_schemas(false)`, through the driver of a component (or a
throwaway one with `--component`): `plan` prints `MigrationPlan.render_sql`
with a comment naming that database, or hands the tables cut down to their
pending steps (`MigrationPlan.pending_schemas`) to another format, and the same
comment, as the file's first line, to any format that declares a keyword-only
`comments` parameter (`inspect.signature`), whichever package registers it;
`check` reports, exits with 3 for a missing table or column and 4 for a
problem, and prints JSON with `--json`. The database name and `search_path`
come from the server, so the comment writes them escaped (`repr`) when they
hold a line break or an encoding declaration (PEP 263, which Python reads on
the first two lines of a file), with `:` and `=` escaped in the latter. `plan`
lets the format refuse its options before it connects, by rendering no table,
and refuses the alembic format's `downgrade=drop` when a pending step's table
already exists, since the revision would drop a table it did not create. It
tells the alembic format by the callable `load_format` returns, not by the
name given to `--format`.

A format that writes a migration file of another tool starts from one
pipeline in `migrations.py`: `render_steps` refuses a comment `check_comment`
refuses (a line break, a NUL, an encoding declaration), an option the format
does not take, and a table name `check_plain_identifier` refuses, then lays out
each step of each table as a `RenderedStep`, whose own checks refuse an
unknown kind and a step number that is not a positive `int`. A `RenderedStep`
holds the step's comment lines and its statements, from `step_statements`: the
advisory lock and the step's SQL inside `scoped_lock_timeout`, which keeps the
transaction's `lock_timeout`, sets its own with `SET LOCAL`, and restores the
kept value in its last statement. The `alembic` and `django` formats keep only
their file template and how they write a statement.
An error raised while a driver reads or connects with a connection string
reaches `_fail` tagged with that string: the message leaves out the string and
its password, and when the driver could not read the string at all (asyncpg's
`ValueError`, psqlpy's `ConnectionPoolBuildError`), the driver's message too,
since it can quote any part of the string.
`tests/test_migration_script.py` checks that the
steps are the statements the drivers build for `on_startup` and that the
script builds the catalog `on_startup` builds.

The same module compares a database with that description. Each step also names
what it creates (`MigrationStep.objects`, a `StepObjects`: whether it creates
the table, its columns, its indexes). `catalog_query` is one read-only `SELECT`
with the names written in as literals, so every driver runs it as it is; it
resolves each table and index name with `to_regclass` and returns, as one JSON
text value, the relation's kind and columns (`pg_attribute`, with
`format_type`) and, per index name, whether it is an index of that table,
`indisvalid`, its access method, columns, uniqueness and predicate, and, beside
the columns, which of them are `NOT NULL` (`attnotnull`).
`CatalogState.from_json` reads it, and `plan_migration` compares it with the
steps: a step whose table, column or index is absent is pending (`required`
when the table or a column is missing, `advisory` when only an index is), and
since every statement is `IF NOT EXISTS`, running the whole step again
completes it. A step that makes columns nullable names them in
`StepObjects.nullable_columns`; it is pending, and `required`, while one of
them exists and is still `NOT NULL` (`PendingStep.not_null_columns`). A column
the table lacks is reported by the earlier step that adds it, and the step that
relaxes it is pending in the same run; when `CREATE TABLE` created the column,
the report is a `missing_column` problem instead. `TableSchema` refuses a step
that makes nullable a column its final description lacks or has `NOT NULL`, or
that no earlier step adds. What `IF NOT EXISTS` would skip becomes a
`SchemaProblem` instead, never DDL: a name another relation took, an index on
another table, an invalid index, an index of another shape, a column of another
type, or a missing column that only a step's `CREATE TABLE` creates, which
skips a table that exists. A released step never changes;
`tests/golden/migration_steps.sql` pins them.

`contrib/alembic.py` puts that engine into Alembic's autogenerate. Importing it
registers `compare_taskiq_steps` as a schema comparator that runs after
Alembic's own. It finds the descriptions `taskiq_metadata` and
`add_taskiq_tables` (`contrib/sqlalchemy.py`) leave in `MetaData.info`, runs
`catalog_query` on autogenerate's connection, and calls `plan_migration`. It
removes the `create_table`, `add_column` and `create_index` operations Alembic
proposed for objects of the pending steps, keeps every other operation (a type
change still shows), and adds one `TaskiqStepOp` per pending step; problems
become warnings in the log. A `TaskiqStepOp` renders as `op.execute(...)` calls
holding the step's SQL, so a revision runs the statements `run_migrations=True`
runs without importing the package; its downgrade does nothing, since no step
removes or renames anything and putting `NOT NULL` back would fail on a row
that holds a `NULL` (`downgrade=drop` drops the tables in `generate`, and
`plan` refuses it when a step is pending on an existing table; nothing puts
`NOT NULL` back). When autogenerate also proposes an `alter_column` whose only
change makes a column of a pending step nullable, it removes that one too: the
step runs the statement. Unless configured to `0`, each step keeps the
transaction's `lock_timeout`, sets its own (`SET LOCAL`) before it takes the
same advisory lock, and ends by giving Alembic's transaction back the kept
value, so the user's own migrations in that transaction keep theirs; the step
is laid out as a `RenderedStep`, as the `alembic` format lays out its steps.
`upgrade_taskiq_tables` runs the same steps, up to a given step number, from a
hand-written revision (`step_statements(..., through=N)`), and `alembic check`
fails while a step is missing because `TaskiqStepOp` is an upgrade operation.

`contrib/django/` is a Django app (label `taskiq_postgresql`) with
migrations and no models. Its migration `000N` holds one `RunTaskiqStep(N)`
(`operations.py`): when `migrate` applies it, the operation reads
`TASKIQ_POSTGRESQL_TARGETS` (`conf.py`: `module:object` paths and dicts given
to `table_schema`, all resolved by `collect_schemas`, which describes each
table once) and `TASKIQ_POSTGRESQL_LOCK_TIMEOUT`, and runs
`step_statements(..., step=N)` for the configured tables through the schema
editor, one statement at a time and without parameters, in the migration's
transaction: the advisory locks in name order and the steps' statements,
bounded as the Alembic steps are. So `sqlmigrate` shows the SQL, a router's
`allow_migrate` decides as for any operation, a database that is not
PostgreSQL raises `NotSupportedError`, and the reverse runs nothing. Missing or
empty targets, and a table name that is not a plain SQL identifier, raise
`ImproperlyConfigured` before any statement runs; the app's `ready()` registers
a system check (`taskiq_postgresql.E001`, `E002`) that reads the settings, and
the names of the dicts, without importing the paths (`parse_target` only
checks their form). Because the migrations are static and read the settings
when they run, a step added by a release needs a new migration file;
`tests/test_contrib_django.py` pins one migration per step.
`contrib/django_format.py` is the `django` migration format: one
`migrations.RunSQL` per step and table with the SQL written out, so the file
imports only Django, `RunSQL.noop` as reverse, and the option `dependencies`
checked against `app_label.migration_name` before it is written. It imports no
Django itself, so `generate --format django` works without the extra.

| Table | Columns | Indexes |
|---|---|---|
| `taskiq_messages` | `id SERIAL` (PK), `task_id` (type from `field_for_task_id`), `task_name VARCHAR`, `message BYTEA`, `labels JSONB`, `created_at`; added: `channel VARCHAR`, `not_before TIMESTAMP WITH TIME ZONE` | hash on `id`; added: btree `taskiq_messages_not_before_idx` on `(channel, not_before, id)` |
| `taskiq_results` | `task_id` (PK, type from `field_for_task_id`), `result BYTEA`, `created_at` | hash on `task_id` |
| `taskiq_schedulers` | `id UUID` (PK), `task_name VARCHAR(100)`, `schedule JSONB`, `updated_at`, `created_at` | hash on `id` and on `task_name` |

All columns but the broker's `channel` are `NOT NULL`; `created_at`,
`updated_at` and `not_before` default to `NOW()`. A row stored by 0.4.x has no
`channel`.

### Queries by operation

| Operation | Builder | SQL shape |
|---|---|---|
| `PostgresqlBroker.kick` | `InsertQuery` | `INSERT ... (..., channel, not_before) VALUES ($1, ..., $5, NOW() + make_interval(secs => $6::double precision)) RETURNING id`, then `NOTIFY` when the message is due at once |
| `PostgresqlBroker.listen`, sweep | `ClaimDueQuery`, run by `fetch_row` | `DELETE FROM ... WHERE id = (SELECT id ... WHERE channel = $1 AND not_before <= NOW() ORDER BY not_before, id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING message` |
| `PostgresqlBroker.listen`, notified id | `ClaimByIdQuery`, run by `fetch_row` | `DELETE FROM ... WHERE id = $2 AND not_before <= NOW() AND (channel = $1 OR channel IS NULL) RETURNING message` |
| `PostgresqlResultBackend.set_result` | `InsertOrUpdateQuery` | `INSERT ... ON CONFLICT (task_id) DO UPDATE SET result = EXCLUDED.result` |
| `is_result_ready` | `SelectQuery` | `SELECT 1 FROM ... WHERE task_id = $1` |
| `get_result` | `SelectQuery`, then `DeleteQuery` unless `keep_results` | `SELECT result ... WHERE task_id = $1` |
| `delete_by_date` | `DeleteByDateQuery` | `DELETE ... WHERE created_at >= $1 AND ($2::... IS NULL OR created_at <= $2)`, so a `NULL` `to_date` means no upper bound |
| `PostgresqlSchedulerSource.get_schedules` | `SelectQuery` | `SELECT id, schedule FROM ...` |
| `add_schedule` | `InsertQuery` | `INSERT (id, task_name, schedule) ...` |
| `delete_schedule`, `post_send` (one-off schedules) | `DeleteQuery` | `DELETE ... WHERE id = $1` |

## LISTEN/NOTIFY flow

```mermaid
sequenceDiagram
    participant P as Producer (kick)
    participant DB as PostgreSQL
    participant W1 as Worker 1 (listen)
    participant W2 as Worker 2 (listen)
    P->>DB: INSERT row with channel and not_before, RETURNING id
    P->>DB: NOTIFY channel, 'id' (only when due at once)
    DB-->>W1: notification('id')
    DB-->>W2: notification('id')
    W1->>DB: DELETE ... WHERE id = $2 AND due RETURNING message
    DB-->>W1: row (claimed)
    W2->>DB: DELETE ... WHERE id = $2 AND due RETURNING message
    DB-->>W2: no row (skip)
    loop on start, then every poll_interval
        W1->>DB: DELETE the oldest due row of the channel, SKIP LOCKED
    end
```

1. `kick` inserts the message with the broker's channel and `not_before`, which
   the server computes as `NOW()` plus the `delay` label. Only a message due at
   once gets a `NOTIFY` with the new row id; a delayed one waits in the table
   for a sweep, so its delivery does not depend on the producer's process. A
   failed `NOTIFY` is logged as a warning, and the sweep delivers the message.
2. `listen()` starts two tasks: one reads the listen driver, queues the id the
   broker parses from each payload and reopens the connection when it is lost
   (`_queue_notified_ids`); the other waits for `shutdown()` and then wakes
   the loop up (`_end_on_shutdown`). The loop of `listen()` itself claims a
   notified id at once (`ClaimByIdQuery`), and sweeps the channel for due
   messages (`ClaimDueQuery`) when it starts and then every `poll_interval`
   seconds, however many notifications arrive in between.
3. Each claim is one `DELETE ... RETURNING`; PostgreSQL gives the row to one
   worker, and the others get no row and move on. The message is yielded as an
   `AckableMessage` whose `ack` is a no-op, because the row is already gone.
4. When reading `LISTEN` ends or fails (the connection was lost, or failed
   its health check), `listen()` logs a warning, keeps delivering by the sweep
   and reopens the connection through the listen driver's `on_shutdown()` and
   `on_startup()` (`_reopen_listen`). The wait before an attempt is drawn
   between half and all of a ceiling that starts at 0.5 s and doubles after
   each failed attempt, up to 30 s (`_ReopenWaits`); a reopened connection
   that drops within 30 s counts as one more failure. Each failed attempt logs
   a warning, a reopened connection an INFO line.
5. `shutdown()` sets an `Event` that ends `listen()` and cancels the tasks
   that read `LISTEN`, so neither the sweep nor a reopen brings back what it
   closes. It closes both drivers side by side, each bounded by twice
   `query_timeout`, so a silent pool and a silent `LISTEN` connection cost one
   wait, not two.

## Error handling

`taskiq_postgresql/exceptions.py` defines `DatabaseConnectionError`,
`ResultIsMissingError` and `SchemaNotReadyError`, all subclasses of
`BaseTaskiqAsyncpgError`.

- `SchemaNotReadyError`: `startup()` without `run_migrations` found the table,
  or a column a migration step adds, missing, or a problem the component
  cannot work with; it carries `table`, `pending` and `problems`. The query
  driver raises it after leaving its connection block, so psqlpy and psycopg
  do not wrap it in `DatabaseConnectionError`.

- `DatabaseConnectionError`:
  - every `ListenDriver.on_startup` raises it when the `LISTEN` connection
    cannot start, so `PostgresqlBroker.startup()` fails with it;
  - asyncpg raises it when the pool cannot be created;
  - psqlpy and psycopg raise it for any error while a pooled connection is
    held, query errors included, because their `connection()` context wraps
    the whole block (psycopg lets `asyncio.TimeoutError` through);
  - a listen driver's iterator raises it when its connection fails the
    health check, and psqlpy's when the server ends the connection;
  - every driver raises it from `connection()` when called outside
    `async with driver`, before any pool exists;
  - a component built with a callable DSN raises it when used before
    `startup()`, since its drivers have no connection string yet.
- `ResultIsMissingError`: `get_result` finds no row for the task id.
- `asyncio.TimeoutError`: asyncpg's and psycopg's `fetch_row` got no answer
  within `query_timeout`; asyncpg's `connection()` found no free pooled
  connection within it, so `kick()` raises it when the pool stays exhausted.
- `TypeError`: a component is built without a usable DSN, or its DSN
  callable returns no usable DSN at `startup()`.
- `ValueError`: an unknown driver name, a column/value count mismatch, an
  upsert without update columns, a `delay` label that is not a finite number
  of seconds, or a `poll_interval`, `health_check_interval` or
  `query_timeout` that is not one above zero.
- `PostgresqlBroker.listen` logs a failed claim, a claim with no answer within
  `query_timeout` as one error line without a traceback, and tries again at
  the next sweep; when reading `LISTEN` fails, it logs a warning, keeps
  delivering by the sweep and reopens the connection.
