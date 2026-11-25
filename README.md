# taskiq-postgresql

PostgreSQL integration for [taskiq](https://taskiq-python.github.io/): a broker
that keeps pending messages in a table and delivers them with `LISTEN`/`NOTIFY`
and a periodic sweep, a result backend, and a persistent schedule source, all in
one PostgreSQL database. It works with three drivers,
[asyncpg](https://magicstack.github.io/asyncpg/),
[psycopg 3](https://www.psycopg.org/psycopg3/) and
[psqlpy](https://github.com/psqlpy-python/psqlpy), each installed as an extra.

## Installation

Install the package with the extra for the driver you use. The drivers are
optional dependencies, so without an extra no driver is available.

```bash
pip install "taskiq-postgresql[asyncpg]"
pip install "taskiq-postgresql[psycopg]"
pip install "taskiq-postgresql[psqlpy]"
```

With uv: `uv add "taskiq-postgresql[asyncpg]"`. The integrations with
SQLAlchemy, SQLModel and Alembic are extras too (see
[Integrations](#integrations)).

## Quickstart

The connection string comes from an environment variable. There is no default
DSN: omitting `dsn`, or passing `None` (what `os.environ.get` returns for an
unset variable), raises `TypeError`, and an empty string is rejected at
construction.

```bash
export POSTGRESQL_URL="postgresql://<user>@<host>:5432/<database>"
```

Put the broker, the result backend and the scheduler source in one module, so
the worker and the scheduler import the same objects:

```python
# app.py
import os

from taskiq import TaskiqScheduler

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

dsn = os.environ["POSTGRESQL_URL"]

# "asyncpg" is the default; "psycopg" and "psqlpy" are the other drivers.
result_backend = PostgresqlResultBackend(
    dsn=dsn,
    driver="asyncpg",
    run_migrations=True,
)
broker = PostgresqlBroker(
    dsn=dsn,
    driver="asyncpg",
    run_migrations=True,
).with_result_backend(result_backend)
scheduler_source = PostgresqlSchedulerSource(
    dsn=dsn,
    driver="asyncpg",
    run_migrations=True,
)
scheduler = TaskiqScheduler(broker=broker, sources=[scheduler_source])


@broker.task
async def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b
```

`run_migrations=True` creates the tables and indexes on startup (`CREATE ... IF
NOT EXISTS`) and adds the broker's `channel` and `not_before` columns to its
table (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), which also upgrades a table
created by 0.4.x. Processes that start at the same time, such as the two that
`taskiq worker` starts by default, take a PostgreSQL advisory lock first, so
they create the tables one after the other. Once the tables, indexes and
columns exist, startup only checks for them, so the application's role needs no
`CREATE` privilege from then on ([Privileges](#privileges)). `run_migrations`
is off by default; you can leave it off and create the tables yourself, with
`taskiq postgresql migrate` or the SQL that `taskiq postgresql generate` or
`taskiq postgresql plan` prints (see [Migrations](#migrations)). A component
that does not migrate still checks its table when it starts, and refuses to
start without it ([Check at startup](#check-at-startup)).

Start a worker. It claims a message as soon as the message's `NOTIFY` arrives,
and it also sweeps the table for due messages when it starts and every
`poll_interval` seconds, so a message sent while no worker was listening is
delivered once one is ([Delivery](#delivery)).

```bash
taskiq worker app:broker
```

Send a task and wait for its result:

```python
# kick.py
import asyncio

from app import add, broker


async def main() -> None:
    await broker.startup()
    task = await add.kiq(1, 2)
    result = await task.wait_result(timeout=10)
    print(result.return_value)  # 3
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

Store a schedule and run the scheduler, which sends the task whenever the
schedule is due:

```python
# schedule.py
import asyncio

from taskiq import ScheduledTask

from app import scheduler_source


async def main() -> None:
    await scheduler_source.startup()
    await scheduler_source.add_schedule(
        ScheduledTask(
            task_name="app:add",
            labels={},
            args=[1, 2],
            kwargs={},
            cron="* * * * *",
        ),
    )
    await scheduler_source.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
taskiq scheduler app:scheduler
```

`PostgresqlSchedulerSource` is imported from
`taskiq_postgresql.scheduler_source`; the package root exports only the broker
and the result backend.

## Configuration

Every constructor takes `dsn` as its first argument. It is a connection string
or a callable that takes no arguments and returns one. A callable is resolved
once, at startup, for all three components and all drivers; if it returns
`None` or an empty string, startup fails with an error that says so.

### PostgresqlBroker

| Argument | Type | Default | Meaning |
|---|---|---|---|
| `dsn` | `str` or `Callable[[], str]` | required | Connection string. |
| `result_backend` | `AsyncResultBackend` or `None` | `None` | Result backend; `.with_result_backend(...)` does the same. |
| `task_id_generator` | `Callable[[], str]` or `None` | `None` | Custom task id factory. |
| `field_for_task_id` | `"VarChar"`, `"Text"` or `"Uuid"` | `"Uuid"` | SQL type of the `task_id` column. |
| `channel_name` | `str` | `"taskiq"` | `LISTEN`/`NOTIFY` channel, also stored with each message: a worker's sweep takes only the messages of its own channel. Use a different one per queue that shares a database. |
| `table_name` | `str` | `"taskiq_messages"` | Table that holds pending messages. |
| `driver` | `"asyncpg"`, `"psqlpy"` or `"psycopg"` | `"asyncpg"` | Database driver. |
| `max_retry_attempts` | `int` | `5` | Accepted for compatibility, currently unused. |
| `connection_kwargs` | `dict` or `None` | `None` | Keyword arguments for the driver's pool and for its `LISTEN` connection, so each option must be valid for both. |
| `pool_kwargs` | `dict` or `None` | `None` | Accepted for compatibility, currently unused. Use `connection_kwargs`. |
| `run_migrations` | `bool` | `False` | Create the table and index on startup, and add the `channel` and `not_before` columns with their index. Off, startup only checks them; see [Check at startup](#check-at-startup). |
| `poll_interval` | `float` | `1.0` | Seconds between two sweeps of the table for due messages; see [Delivery](#delivery). A value that is not a finite number above zero raises `ValueError`. |
| `health_check_interval` | `float` | `30.0` | Seconds the `LISTEN` connection may stay silent before the worker checks it; see [Lost connections](#lost-connections). A value that is not a finite number above zero raises `ValueError`. |
| `query_timeout` | `float` | `30.0` | Seconds the worker waits for that check, for a claim (not with psqlpy) and for a close before it gives the connection up; see [Lost connections](#lost-connections). A value that is not a finite number above zero raises `ValueError`. |

### PostgresqlResultBackend

| Argument | Type | Default | Meaning |
|---|---|---|---|
| `dsn` | `str` or `Callable[[], str]` | required | Connection string. |
| `keep_results` | `bool` | `True` | When `False`, a result row is deleted after it is read. |
| `table_name` | `str` | `"taskiq_results"` | Table that holds results. |
| `field_for_task_id` | `"VarChar"`, `"Text"` or `"Uuid"` | `"Uuid"` | SQL type of the `task_id` column. |
| `serializer` | `TaskiqSerializer` or `None` | `None` | Result serializer; `None` means `PickleSerializer()`; see the note below before changing it. |
| `driver` | `"asyncpg"`, `"psqlpy"` or `"psycopg"` | `"asyncpg"` | Database driver. |
| `run_migrations` | `bool` | `False` | Create the table and index on startup. Off, startup only checks them; see [Check at startup](#check-at-startup). |
| `**connect_kwargs` | `Any` | none | Keyword arguments for the driver's connection pool. |

Results are stored with pickle, and unpickling can run code. Treat the results
table as trusted data that only your own workers write: anyone who can write to
it can run code in the process that reads a result. Restrict database access
accordingly, for example with a role that only the workers use to write the
results table.

Leave `serializer` unset. A result is a `TaskiqResult` object, and taskiq's
`JSONSerializer` and `MSGPackSerializer` raise `TypeError` when asked to store
one, so they cannot replace pickle here.

### PostgresqlSchedulerSource

| Argument | Type | Default | Meaning |
|---|---|---|---|
| `dsn` | `str` or `Callable[[], str]` | required | Connection string. |
| `table_name` | `str` | `"taskiq_schedulers"` | Table that holds schedules. |
| `driver` | `"asyncpg"`, `"psqlpy"` or `"psycopg"` | `"asyncpg"` | Database driver. |
| `startup_schedule` | `dict[str, list[dict]]` or `None` | `None` | Schedules to insert on startup, keyed by task name. Each entry needs `cron` or `time` and may have `args`, `kwargs` and `cron_offset`. A task that already has a stored schedule is skipped, and so is a task the broker does not know. |
| `broker` | `AsyncBroker` or `None` | `None` | Needed only with `startup_schedule`, to look the tasks up. |
| `run_migrations` | `bool` | `False` | Create the table and indexes on startup. Off, startup only checks them; see [Check at startup](#check-at-startup). |
| `**connect_kwargs` | `Any` | none | Keyword arguments for the driver's connection pool. |

The source implements taskiq's `ScheduleSource` (`get_schedules`,
`add_schedule`, `delete_schedule`, `post_send`). A schedule with a `time` is
deleted after it is sent. `schedule_id` is stored in a `UUID` column, so keep
the generated value or pass a UUID string.

Table names, in all three components, and the broker's `channel_name` are
placed in SQL as written. Use identifiers you control, never user input.

### Driver options

The keyword arguments go to the driver you chose:

| Driver | Pool | `LISTEN` connection |
|---|---|---|
| `asyncpg` | `asyncpg.create_pool` | `asyncpg.connect` |
| `psycopg` | `psycopg_pool.AsyncConnectionPool` | `psycopg.AsyncConnection.connect` |
| `psqlpy` | `psqlpy.ConnectionPool` | `psqlpy.ConnectionPool` |

```python
import os

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend

dsn = os.environ["POSTGRESQL_URL"]

# A pool option goes to the result backend directly; the broker shares its
# kwargs with the LISTEN connection, so give it only options both accept.
result_backend = PostgresqlResultBackend(dsn=dsn, min_size=1, max_size=10)
broker = PostgresqlBroker(
    dsn=dsn,
    connection_kwargs={"command_timeout": 30},
).with_result_backend(result_backend)
```

## Delivery

`kick()` stores each message in the broker's table in one `INSERT`, together
with the broker's channel and the time the message becomes due. The PostgreSQL
server computes that time from its own clock: `NOW()`, plus the `delay` label
in seconds when the message has one, as `@broker.task(delay=...)` and
`SmartRetryMiddleware` set. A message due at once is also announced with
`NOTIFY`, and a listening worker claims it right away. A delayed message gets
no `NOTIFY`, and nothing waits for it in the process that sent it, so it is
delivered even if that process exits before the delay ends. If the `NOTIFY`
fails once the message is stored, `kick()` logs a warning and returns, and the
next sweep delivers the message; an error while storing it is raised to the
caller.

Every worker sweeps the table for the due messages of its channel, earliest due
first, when it starts listening and then every `poll_interval` seconds, however
many notifications arrive in between. The sweep also delivers a message whose
`NOTIFY` reached no worker, because none was listening or the `LISTEN`
connection was down. No message is claimed before it is due. Once it is due, a
running worker of its channel that keeps up with its messages claims it within
about `poll_interval` seconds.

A worker claims a message by deleting its row (`DELETE ... RETURNING`), so each
message reaches one worker only. Delivery is at most once: a claimed message is
gone from the table, and a worker that stops before finishing the task does not
give it back.

### Lost connections

If the `LISTEN` connection closes or fails, the worker logs one warning, keeps
delivering through the sweep and opens the connection again. It waits about
0.5 s before the first attempt and twice as long after each failed one, up to
30 s, logging a warning for each failure and an INFO line once `LISTEN` works
again.

A connection can also go silent without closing: a NAT or a load balancer that
drops an idle flow sends no FIN or RST, so the client still sees an open socket
that never gets an answer. When the `LISTEN` connection has been quiet for
`health_check_interval` seconds, the worker sends the same `LISTEN` again, which
changes nothing on the server, and reopens the connection if no answer comes
within `query_timeout` seconds. With psycopg the check runs every
`health_check_interval` seconds, even while notifications arrive. These checks
also keep the flow alive in a NAT, and `pg_stat_activity` keeps showing the
`LISTEN`. The server keeps the
abandoned session, still listening, until its own TCP keepalive notices the
dead client (`tcp_keepalives_idle`, which defaults to the operating system's
two hours on Linux); set it, and `tcp_user_timeout`, on the server to free such
sessions sooner.

With asyncpg and psycopg, a claim that gets no answer within `query_timeout`
seconds is given up and logged as an error, a connection that does not answer
is dropped from the pool, and the sweep tries again. asyncpg first gives the
server up to `query_timeout` more to cancel the query, so it may take about
twice `query_timeout` to give a claim up. A pool gets over silent connections
one claim at a time, so with several of them the sweep delivers again after
each has timed out once. A claim given up after its `DELETE` was done loses
that message, as any lost answer to a claim does under at-most-once delivery.

`shutdown()` gives a connection that does not answer `query_timeout` seconds
to close and then closes it by force, so with silent connections it may take up
to about twice `query_timeout`.

`query_timeout` does not bound psqlpy's claims. psqlpy can neither cancel a
query on the server nor drop one pooled connection, so a claim it gave up would
leave its `DELETE` queued, to run later and take a row nobody receives. A claim
on a silent psqlpy connection, and `listen()` with it, waits until the
operating system gives up on the socket, which by default takes about 15
minutes on Linux. To shorten that, set psqlpy's `tcp_user_timeout_sec` and its
`keepalives_*` settings in `connection_kwargs`.

## TLS

The broker, the result backend and the scheduler source each open their own
connections, so give all three the same TLS settings. To verify the server
certificate and its host name, each driver takes its own options:

- **asyncpg**: the `ssl` keyword argument, here an `ssl.SSLContext` that trusts
  your CA and checks the host name (the default of `create_default_context`).
- **psycopg**: `sslmode=verify-full` and `sslrootcert` in the DSN. They are
  libpq parameters, so the same DSN serves all three components.
- **psqlpy**: the `ssl_mode` and `ca_file` keyword arguments.

The broker takes keyword arguments in `connection_kwargs`; the result backend
and the scheduler source take them directly.

```python
import os
import ssl

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

dsn = os.environ["POSTGRESQL_URL"]
tls = {"ssl": ssl.create_default_context(cafile="/path/to/ca.pem")}

broker = PostgresqlBroker(dsn=dsn, driver="asyncpg", connection_kwargs=tls)
result_backend = PostgresqlResultBackend(dsn=dsn, driver="asyncpg", **tls)
scheduler_source = PostgresqlSchedulerSource(dsn=dsn, driver="asyncpg", **tls)
```

For psycopg the options live in the DSN, so nothing else changes:

```text
postgresql://<user>@<host>:5432/<database>?sslmode=verify-full&sslrootcert=/path/to/ca.pem
```

```python
import os

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

dsn = os.environ["POSTGRESQL_URL"]  # the DSN above

broker = PostgresqlBroker(dsn=dsn, driver="psycopg")
result_backend = PostgresqlResultBackend(dsn=dsn, driver="psycopg")
scheduler_source = PostgresqlSchedulerSource(dsn=dsn, driver="psycopg")
```

For psqlpy:

```python
import os

from psqlpy import SslMode

from taskiq_postgresql import PostgresqlBroker, PostgresqlResultBackend
from taskiq_postgresql.scheduler_source import PostgresqlSchedulerSource

dsn = os.environ["POSTGRESQL_URL"]
tls = {"ssl_mode": SslMode.VerifyFull, "ca_file": "/path/to/ca.pem"}

broker = PostgresqlBroker(dsn=dsn, driver="psqlpy", connection_kwargs=tls)
result_backend = PostgresqlResultBackend(dsn=dsn, driver="psqlpy", **tls)
scheduler_source = PostgresqlSchedulerSource(dsn=dsn, driver="psqlpy", **tls)
```

Keep the password out of the source and out of version control: read the DSN
from the environment or a secret store, or pass a callable as `dsn`.

## Database schema

With `run_migrations=True` the package runs this SQL, shown with the default
table names and `field_for_task_id="Uuid"`. A test builds the same statements
from the code and compares them with these blocks. `taskiq postgresql generate`
prints them for your own names (see [Migrations](#migrations)).

The broker (`taskiq_messages`):

```sql
CREATE TABLE IF NOT EXISTS taskiq_messages (
    id SERIAL NOT NULL PRIMARY KEY,
    task_id Uuid NOT NULL,
    task_name VARCHAR NOT NULL,
    message BYTEA NOT NULL,
    labels JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS taskiq_messages_id_idx ON taskiq_messages USING HASH (id);
ALTER TABLE taskiq_messages
  ADD COLUMN IF NOT EXISTS channel VARCHAR,
  ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx
  ON taskiq_messages (channel, not_before, id);
```

`channel` holds the broker's `channel_name` and `not_before` the time the
message becomes due. They are added by `ALTER TABLE` rather than in
`CREATE TABLE`, so a new table and one created by 0.4.x end up with the same
columns.

The result backend (`taskiq_results`):

```sql
CREATE TABLE IF NOT EXISTS taskiq_results (
    task_id Uuid NOT NULL PRIMARY KEY,
    result BYTEA NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS taskiq_results_task_id_idx ON taskiq_results USING HASH (task_id);
```

The scheduler source (`taskiq_schedulers`):

```sql
CREATE TABLE IF NOT EXISTS taskiq_schedulers (
    id UUID NOT NULL PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    schedule JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS taskiq_schedulers_id_idx ON taskiq_schedulers USING HASH (id);
CREATE INDEX IF NOT EXISTS taskiq_schedulers_task_name_idx ON taskiq_schedulers USING HASH (task_name);
```

`task_id` has the type set by `field_for_task_id` (`VarChar`, `Text` or
`Uuid`). A broker message is deleted
when a worker claims it (`DELETE ... RETURNING`), which is why only one worker
gets each message.

### Privileges

Once the tables, indexes and columns above exist, no role needs a DDL
privilege. Besides `USAGE` on the schema, a role that sends tasks needs
`INSERT` and `SELECT` on the broker's table (the `INSERT` returns the new id)
and `USAGE` on its `taskiq_messages_id_seq` sequence. A worker needs `SELECT`,
`UPDATE` and `DELETE` on the broker's table: `UPDATE` because the sweep locks
the row it claims with `FOR UPDATE SKIP LOCKED`. A worker that also sends
tasks, for example through `SmartRetryMiddleware`, needs both sets.

## Migrations

There are three ways to create the tables and their indexes:

- `run_migrations=True`, as in the quickstart: each process creates what is
  missing when it starts.
- `taskiq postgresql migrate`, once per deploy before the workers start, for
  example in a release job. The workers then run with `run_migrations=False`.
- `taskiq postgresql generate`, which prints the SQL so that you run it
  yourself or hand it to your migration tool, or `taskiq postgresql plan`,
  which prints only the SQL one database still lacks.

`taskiq postgresql check` tells whether a database has every table, column and
index, so that a deploy can stop before the workers start; a component that
does not migrate runs the same check when it starts
([Check at startup](#check-at-startup)).

SQLAlchemy, SQLModel, Alembic and Django can also hold the tables (see
[Integrations](#integrations)).

The commands take the `module:object` paths that `taskiq worker` and
`taskiq scheduler` take, and `--app-dir` like them. A broker brings its result
backend when that is a `PostgresqlResultBackend`, and a `TaskiqScheduler`
brings its broker and its `PostgresqlSchedulerSource` sources; a table named
twice is created once. The commands import your module but start nothing:
`generate` never connects, `migrate` connects only to create the tables, and
`plan` and `check` only to read the catalog, with each component's own DSN and
driver. The exit status is 0 on success, 1 on an error or when a path holds no
component of this package, and 2 on a usage error; `plan` and `check` also
exit with 3 or 4 ([Plan and check](#plan-and-check)).

### Generate the SQL

```bash
taskiq postgresql generate app:scheduler > taskiq_tables.sql
psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f taskiq_tables.sql
```

Only the script goes to standard output; messages go to standard error. The
script starts with a comment that names the package version and the tables,
then runs in one transaction: it sets a lock timeout for that transaction,
takes, per table, the advisory lock `run_migrations=True` takes, then creates
each table and index with `IF NOT EXISTS` and adds the broker's `channel` and
`not_before` columns and their index the same way, which also upgrades a
broker table created by 0.4.x. The statements are the ones in
[Database schema](#database-schema), with your table names and
`field_for_task_id`.

The script is idempotent: a second run changes nothing, and the advisory lock
lets it run while workers with `run_migrations=True` are starting. It is not
lock-free. On a live database it locks the tables even when nothing is
missing: `CREATE INDEX` waits for every open transaction that wrote to the
table, and `ALTER TABLE` for every open transaction that used it, before they
find that the index or the column exists, and every later query on the
table, the producers' `INSERT` included, waits behind the `ALTER TABLE`. So
the script starts with `SET LOCAL lock_timeout = '10s'`: a statement that
waits that long for a lock, the advisory lock included, fails, and the whole
script rolls back instead of stalling the broker. Run it again once the long
transaction has ended. `--lock-timeout SECONDS` sets another limit;
`--lock-timeout 0` leaves the statement out, so the session's own
`lock_timeout` applies (PostgreSQL's default, 0, waits for ever).
`taskiq postgresql migrate` checks first and runs no DDL when every table,
column and index exists, so it takes no table lock on a database that is
already up to date, and `taskiq postgresql plan` prints only the steps a
database still lacks ([Plan and check](#plan-and-check)).

To generate without importing the application, for example where its settings
are not available, name one component per call. `--table-name` and
`--field-for-task-id` default to the component's own defaults:

```bash
taskiq postgresql generate --component broker --table-name taskiq_messages --field-for-task-id Uuid
```

`--no-transaction` leaves out `BEGIN` and `COMMIT`, and `--no-lock` the lock.
Without `BEGIN`, `SET LOCAL lock_timeout` is the first statement and lasts
until the transaction that runs the script ends; outside a transaction
PostgreSQL ignores it with a warning.
`--format` picks another output format, when a package has installed one (see
[Writing a migration format](#writing-a-migration-format)), and
`--option KEY=VALUE` passes options to it.

### Apply it with migrate

```bash
taskiq postgresql migrate app:scheduler
```

`migrate` runs the code `run_migrations=True` runs at startup, whatever the
components set, then closes its connections. It does not start the broker, so
nothing listens, and it inserts no `startup_schedule`. To create what is
missing, the role it connects with needs `CREATE` on the schema, and must own
a table to add a missing index to it.

From then on the workers can run with `run_migrations=False` and a role that
only reads and writes rows. With the default table names:

```sql
GRANT USAGE ON SCHEMA public TO taskiq_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON taskiq_messages, taskiq_results, taskiq_schedulers TO taskiq_worker;
GRANT USAGE ON SEQUENCE taskiq_messages_id_seq TO taskiq_worker;
```

Such a role can also keep `run_migrations=True`: once every table and index
exists, startup only checks for them.

### Plan and check

`plan` reads the catalog of one database, in one read-only query, and prints
the SQL of the migration steps that database still lacks, and only that: on a
database that has every step it prints nothing. On a broker table that 0.4.x
created, that is the step that adds `channel` and `not_before`:

```bash
taskiq postgresql plan app:scheduler > taskiq_pending.sql
psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f taskiq_pending.sql
```

The script is the one `generate` writes, with the same lock timeout and
advisory lock and the same options (`--lock-timeout`, `--no-transaction`,
`--no-lock`, `--format`), for the pending steps only. A comment after the
version line names the database and `search_path` the plan was computed for:
it is right for that database at that moment, and each statement still only
adds what is missing. The locks each step takes (`note:` lines) and what no
step fixes (`problem:` lines), such as an invalid index or an index name that
another table took, go to standard error, each with what to do.
`--format alembic` writes a revision file with only the pending steps, and
`--format django` a Django migration, whose first line is the same comment. A
plan
is for one database: when the components use different connection strings,
pass `--dsn`, or plan one table at a time with `--component`.

`check` reads the same catalog and changes nothing. It reports each pending
step and each problem on standard error, and `--json` also prints the report
on standard output, as one object with the keys `current`, `pending` (each
with `table`, `kind`, `step`, `severity`, what is `missing`: `table`,
`columns`, `indexes`, and the `not_null_columns` the step still has to make
nullable), `problems` (each with `table`, `code`, `severity` and
`detail`) and `advisory_only`:

```bash
taskiq postgresql check app:scheduler --json
```

Both commands take `--dsn` to read another database than the components'
own, and, like `generate`, `--component` with `--table-name` and
`--field-for-task-id` to describe one table without importing the application;
`--component` needs `--dsn`, and reads with the first driver installed of
asyncpg, psycopg and psqlpy. Their
query reads only `pg_catalog`, which every role can read by default, so a role
without DDL privileges can run them.

Other users of the machine can read a command line (`ps`), and it stays in
the shell history and in a CI job's definition: give `--dsn` a connection
string without the password, and the password through a password file
(`~/.pgpass`, or the file `PGPASSFILE` names) or `PGPASSWORD`. asyncpg and
psycopg use them when the string has no password, `PGPASSWORD` first; psqlpy
reads neither. When a driver cannot read the connection string, the error of
`plan`, `check` and `migrate` leaves out the driver's message, which can quote
part of the password, and says what to check; their other errors leave out
the connection string and its password.

```bash
PGPASSFILE=/run/secrets/pgpass taskiq postgresql check --component broker --dsn postgresql://taskiq@db.internal/app
```

The exit status says what to do:

| Exit status | `plan` | `check` |
|---|---|---|
| 0 | The plan is printed, or no step is pending. | Up to date; a missing index alone only warns. |
| 1 | An error: the connection, the import of the application, a path without a component. | The same. |
| 2 | A usage error. | The same. |
| 3 | Not used. | A table or a column is missing: a step is pending. |
| 4 | A problem no step fixes needs a person; the plan, if any, is still printed. | The same; it wins over 3. |

So a deploy can check the database before it starts the workers, with the
workers' own configuration, and stop when a migration was forgotten: exit
status 3 or 4 fails the step.

```bash
# In the deploy job, before the workers start.
taskiq postgresql check app:scheduler
```

### Check at startup

A component with `run_migrations=False`, the default, reads the catalog once
when it starts, with the query `check` runs, through its own connection pool,
and compares its table with what this release needs. It runs no DDL. When the
table, or a column that a step adds, is missing, `startup()` raises
`SchemaNotReadyError` (from `taskiq_postgresql.exceptions`) before anything
else starts, naming what is missing and how to create it. On a broker table
that 0.4.x created:

```text
taskiq_messages is not ready and run_migrations is off: step 2 has not run (missing columns: channel, not_before; missing indexes: taskiq_messages_not_before_idx). Create what is missing with `taskiq postgresql migrate <module>:<object>`, with the SQL `taskiq postgresql plan <module>:<object>` prints, or with an Alembic revision (`alembic revision --autogenerate`); or start once with run_migrations=True.
```

A missing index, or an index no step can fix (an invalid one, or one whose
name another table took), logs a warning instead: the component works, if more
slowly. With
`run_migrations=True` startup reads the catalog the same way, takes the
migration lock and runs the DDL only when a step is pending, and logs the same
warning for an index no step can fix.

The check needs the database at startup: on one it cannot reach, `startup()`
fails with `DatabaseConnectionError` before the worker starts, as it does with
`run_migrations=True`. With psycopg that happens once the connection pool has
waited for a connection for its `timeout`, 30 seconds by default.

### Alembic, Django and Flyway

Flyway runs each migration in a transaction of its own, so give it the script
without `BEGIN` and `COMMIT`. The lock and the lock timeout stay, and last
until its transaction ends. Write the script as a versioned migration, with
the next free version number:

```bash
taskiq postgresql generate app:scheduler --no-transaction > db/migration/V2__taskiq_tables.sql
```

A Flyway file holds the SQL of the release that generated it, and the script
only adds what is missing, so when an upgrade changes the tables (the
changelog says so), add a new migration with the new script.

For Alembic and Django, install their extra instead of copying the script.
[Alembic](#alembic) shows how autogenerate writes the steps a database lacks,
each with a lock timeout that ends with the step, a helper for a revision of
your own, and a format that writes the revision file. [Django](#django) shows
an app whose migrations run the steps, and a format that writes a migration
file for an app of your own.

### Writing a migration format

`--format NAME` loads the callable registered as `NAME` in the
`taskiq_postgresql.migration_formats` entry-point group. This package registers
`sql`; another package adds a format in its own `pyproject.toml`:

```toml
[project.entry-points."taskiq_postgresql.migration_formats"]
tables = "my_package.formats:render"
```

The callable takes the tables and the options, and returns the text that
`generate` prints. This one lists the tables in Markdown:

```python
from collections.abc import Sequence

from taskiq_postgresql.migrations import (
    FormatOptions,
    MigrationFormatOptionError,
    TableSchema,
)


def render(schemas: Sequence[TableSchema], options: FormatOptions) -> str:
    """List each table's columns and indexes in Markdown."""
    unknown = sorted(set(options.extra) - {"title"})
    if unknown:
        raise MigrationFormatOptionError(f"unknown options: {', '.join(unknown)}")
    lines = [f"# {options.extra.get('title', 'taskiq tables')}"]
    for table in schemas:
        lines.append(f"## {table.name} ({table.kind})")
        for column in table.columns:
            null = "null" if column.nullable else "not null"
            lines.append(f"- `{column.name}`: {column.type_name}, {null}")
        for index in table.indexes:
            columns = ", ".join(index.columns)
            lines.append(f"- index `{index.name}`: {index.method} on {columns}")
    return "\n".join(lines) + "\n"
```

The contract:

- `schemas` is a sequence of `TableSchema`, one per table: `kind`, `name`,
  `columns` (`ColumnSchema`: `name`, `sql_type`, `type_name`, `length`,
  `nullable`, `default`, `primary_key`), `indexes` (`IndexSchema`: `name`,
  `columns`, `method`) and `steps` (`MigrationStep`: `number`, the SQL
  `statements` in the order they run, each idempotent, and `objects`, a
  `StepObjects` with `creates_table` and the `columns` and `indexes` the step
  creates). The columns and indexes are the table's shape after every step,
  except that `indexes` leaves out the primary key's own index, which
  `CREATE TABLE` creates and PostgreSQL names (`taskiq_messages_pkey` for
  `taskiq_messages`). Read the structure instead of parsing SQL; to emit SQL,
  reuse `render_sql` or the steps' statements as they are.
- `options` is a `FormatOptions`: `transaction` and `lock` (`False` with
  `--no-transaction` and `--no-lock`), `lock_timeout` (the seconds of
  `--lock-timeout`, 10 by default, 0 for none), and `extra`, the
  `--option KEY=VALUE` pairs. Raise `MigrationFormatOptionError` for an option
  the format does not accept; `generate` then exits with 2.
- A format may also take a keyword-only `comments` parameter, a sequence of
  lines: `plan` then passes the line that names the database the plan is for,
  to write as a comment at the top of the file. For a file another migration
  tool runs, `render_steps(schemas, options, ...)` refuses a comment, an option
  or a table name the file cannot hold, and returns each step with its comment
  lines and its statements, the lock and the lock timeout included.
- When the format needs an optional dependency, import it at module level and
  put it in an extra named after the format. If the import fails, `generate`
  tells the user to run `pip install '<your-package>[<format>]'`. Listing the
  formats loads none of them.
- Pick a name no other package uses. When several installed packages register
  one name, `generate` refuses it, exits with 2 and names the packages. A name
  this package registers, such as `sql`, always loads this package's own
  format, so no other package can replace it.

All these names are importable from `taskiq_postgresql.migrations`.

## Integrations

SQLAlchemy, SQLModel, Alembic and Django can hold the tables too. Each
integration is an extra, installed with the extra of your driver:

```bash
pip install "taskiq-postgresql[asyncpg,sqlalchemy]"
pip install "taskiq-postgresql[asyncpg,sqlmodel]"
pip install "taskiq-postgresql[asyncpg,alembic]"
pip install "taskiq-postgresql[psycopg,django]"
```

`sqlmodel` and `alembic` bring SQLAlchemy with them, and `django` brings
Django. The integrations build the tables from the description that
`generate` renders as SQL, so the columns, types, defaults, indexes and their
names are the ones `run_migrations=True` creates.
Like the commands, the SQLAlchemy, SQLModel and Alembic integrations take
`module:object` paths (with `app_dir=` for `--app-dir`), components, brokers
and schedulers. They also take `table_schema(...)` descriptions, which need
neither your application nor a DSN; the Django app reads paths and such
descriptions from its settings.

### SQLAlchemy and SQLModel

`add_taskiq_tables` adds the tables to a `MetaData` as SQLAlchemy `Table`
objects and returns them:

```python
# models.py
from sqlalchemy.orm import DeclarativeBase

from taskiq_postgresql.contrib.sqlalchemy import add_taskiq_tables


class Base(DeclarativeBase):
    """The base class of your models."""


# The quickstart's app:scheduler brings its broker, the broker's result
# backend and the scheduler source.
taskiq_tables = add_taskiq_tables(Base.metadata, "app:scheduler")
```

`Base.metadata.create_all(engine)` then creates them along with your models. A
table that the metadata already holds under the same name is reused if it is
the same table; otherwise `ValueError` is raised. `taskiq_metadata(...)`
returns a new `MetaData` that holds only these tables. The tables are not
models: read and write them through the components.

With SQLModel, the tables go to `SQLModel.metadata`, or to the `metadata=` you
pass:

```python
from taskiq_postgresql.contrib.sqlmodel import add_taskiq_tables

add_taskiq_tables("app:scheduler")
```

### Alembic

Import `taskiq_postgresql.contrib.alembic` in `env.py` and put the tables in
`target_metadata`:

```python
# alembic/env.py
from myapp.models import Base

from taskiq_postgresql.contrib.alembic import taskiq_metadata

target_metadata = [Base.metadata, taskiq_metadata("app:scheduler")]
```

Importing the module registers an autogenerate hook. `alembic revision
--autogenerate` then reads which migration steps the database lacks and writes
each one into the revision as `op.execute(...)` calls that hold the step's
SQL: the statements `run_migrations=True` and `generate` run, the advisory
lock first. On an empty database that is every step; on a database 0.4.x set
up, only the step that adds the broker's `channel` and `not_before`; on an
up-to-date one, nothing. The SQL is written out, so the revision never imports
this package and keeps doing the same thing after you upgrade it. Its
`downgrade()` leaves the steps in place: they only add columns and indexes,
which an earlier release ignores. The hook takes the place of the
`create_table`, `add_column` and `create_index` Alembic would write for these
tables and keeps everything else, so a column whose type differs from the
description still shows up.

If your models' metadata already holds the tables, as after
`add_taskiq_tables` above, import the module for its hook and keep your
metadata:

```python
# alembic/env.py
import taskiq_postgresql.contrib.alembic  # noqa: F401  (the autogenerate hook)
from myapp.models import Base

target_metadata = Base.metadata
```

`alembic check` runs autogenerate without writing a revision, and fails while
the database lacks a step that no revision adds, with
`New upgrade operations detected: [('taskiq_postgresql_step', 'taskiq_messages', 1), ...]`.
Run it in CI against an empty database that only Alembic migrates, so that a
step a release adds fails the build until a revision brings it:

```bash
alembic upgrade head
alembic check
```

Autogenerate compares the revisions with the database it connects to. If
workers with `run_migrations=True` already ran a step there, as on a shared
development database, the step exists and the revision leaves it out; a
production database that only Alembic migrates then never gets it. Generate
revisions against a database that only Alembic migrates, or run the workers
with `run_migrations=False` wherever Alembic owns the schema; `alembic check`
in CI catches what slips through.

The lock each step takes holds until Alembic commits, and workers that start
meanwhile wait for it. With the default `transaction_per_migration=False`
that is the end of `alembic upgrade`; pass
`context.configure(..., transaction_per_migration=True)` to commit after each
revision instead.

Each step also bounds its own lock waits, as the script of `generate` does. It
starts with `SET LOCAL lock_timeout = '10s'`, so a statement that waits longer
for a lock (the advisory lock, or `ALTER TABLE` on a table that a worker's
open transaction has read) fails and Alembic rolls the revision back, instead
of making every query on the table wait behind it. Its last statement gives
the transaction back the `lock_timeout` it had, so your own migrations in the
same revision wait for locks as before. Set the number of seconds with
`context.configure(..., taskiq_postgresql_lock_timeout=5)` in `env.py`, which
autogenerate writes into each step of the revision; `0` leaves both statements
out, and the transaction's own `lock_timeout` applies. The lock and the bound
only work inside the revision's transaction: `upgrade_taskiq_tables` raises
`RuntimeError` on an autocommit connection (`autocommit_block()`, or an
`AUTOCOMMIT` isolation level) instead of running the steps unbounded.

If the workers keep `run_migrations=True` and Alembic should leave the tables
alone, keep them out of `target_metadata` and filter them out instead;
otherwise autogenerate proposes `op.drop_table` for each of them:

```python
# alembic/env.py
from taskiq_postgresql.contrib.alembic import exclude_taskiq_tables

include_object = exclude_taskiq_tables("app:scheduler")
```

Pass it as `context.configure(..., include_object=include_object)`. It hides
the tables, their columns and their indexes, from Alembic and from the hook.
Alembic does not compare sequences, so the broker's sequence needs no filter.
If you already have an `include_object`, include an object only when both do.

An autogenerated revision names the tables as they were where it was
generated. When the names come from settings that differ between
environments, write the revision by hand with `upgrade_taskiq_tables`. It runs
the steps of each table up to the step `through` names, one statement at a
time and in the revision's transaction. A released step never changes, so the
revision runs the same SQL whatever release is installed; when a release adds
a step, add a revision with the new `through`. Every table in a call must have
that step, so tables with different numbers of steps take separate calls
(`[step.number for step in table_schema("broker").steps]` lists them):

```python
# alembic/versions/taskiq_tables.py
from alembic import op

from myapp.settings import QUEUE
from taskiq_postgresql.contrib.alembic import upgrade_taskiq_tables
from taskiq_postgresql.migrations import table_schema

revision = "taskiq_tables"
down_revision = None


def upgrade() -> None:
    upgrade_taskiq_tables(
        op,
        table_schema("broker", table_name=f"{QUEUE}_messages"),
        through=2,
    )
    upgrade_taskiq_tables(
        op,
        table_schema("result_backend", table_name=f"{QUEUE}_results"),
        table_schema("scheduler_source", table_name=f"{QUEUE}_schedulers"),
        through=1,
    )


def downgrade() -> None:
    pass
```

Pass `field_for_task_id` too if you changed it, and `lock_timeout=` to change
the 10 seconds each call bounds its lock waits by (`0` for no bound of its
own). `alembic upgrade --sql` prints the statements instead of running them.

Without autogenerate, `generate --format alembic` writes a revision file with
every step of each table, as autogenerate writes it for an empty database:

```bash
taskiq postgresql generate app:scheduler --format alembic --option revision=taskiq_tables > alembic/versions/taskiq_tables.py
```

The format takes these options:

- `revision`: the revision id, made of letters, digits and `_`. By default,
  the first 12 hex digits of the SHA-256 of the statements, so the same tables
  give the same id.
- `down_revision`: the revision it follows, usually your current head. By
  default there is none: it is a first revision.
- `downgrade`: `none` (the default) leaves the tables in place; `drop` makes
  `downgrade()` drop them with `DROP TABLE IF EXISTS`, bounded by the same lock
  timeout as the steps. The table names must be plain SQL identifiers (letters,
  digits, `_` and `$`, not starting with a digit): the format refuses others.

The file has no `BEGIN` or `COMMIT`, because Alembic runs each revision in its
own transaction, so `--no-transaction` changes nothing; `--no-lock` leaves
out the locks, and `--lock-timeout` sets each step's bound on lock waits as
above. Any other option, or a value the format does not accept, exits with 2.
`plan --format alembic` takes the same options and writes the same file with
only the steps that the database it reads still lacks
([Plan and check](#plan-and-check)), under a first comment line that names
that database and its `search_path`. It checks them before it connects, and
refuses `downgrade=drop`, with exit status 2, when a table it would drop
already exists, such as a broker table 0.4.x created that only needs step 2:
the revision does not create that table, so its downgrade must not drop it.

### Django

The `django` extra adds a Django app whose migrations create the tables with
the SQL `run_migrations=True` runs. It allows Django 5.2 LTS (5.2.19 or
later) and, on Python 3.12 and later, Django 6.0 and 6.1. Add the app to
`INSTALLED_APPS` and name in `TASKIQ_POSTGRESQL_TARGETS` the components whose
tables it creates, as the `module:object` paths the commands take:

```python
# settings.py
INSTALLED_APPS = [
    # ... your apps
    "taskiq_postgresql.contrib.django",
]

# The quickstart's app:scheduler brings its broker, the broker's result
# backend and the scheduler source.
TASKIQ_POSTGRESQL_TARGETS = ["app:scheduler"]
```

Then migrate it with your other apps, or on its own:

```bash
python manage.py migrate taskiq_postgresql
```

The app, labelled `taskiq_postgresql`, has no models, only migrations: one
per migration step. `0001` runs step 1 of every table (the table and its
indexes) and `0002` step 2 (the broker table's `channel` and `not_before`,
which also completes a broker table 0.4.x created); a release that adds a step
adds a migration. Each migration runs its statements one at a time, in the
migration's transaction. It first keeps the transaction's own `lock_timeout`,
then sets `SET LOCAL lock_timeout = '10s'`, so that a statement that waits
longer for a lock fails and rolls the migration back instead of stalling the
broker. It then takes the advisory lock `run_migrations=True` takes, so
workers that start meanwhile wait for the migration, and runs statements that
only add what is missing; its last statement gives the transaction back the
`lock_timeout` it kept, so the other migrations of the same transaction wait
for locks as before. `python manage.py sqlmigrate taskiq_postgresql 0001`
shows the SQL. Once the tables exist, the workers can run with
`run_migrations=False`.

An entry can also be a dict that describes a table by kind, which needs no
import of your application: `kind` is `broker`, `result_backend` or
`scheduler_source`, and `table_name` and `field_for_task_id` default to the
component's own. `TASKIQ_POSTGRESQL_LOCK_TIMEOUT` sets the seconds of the
lock timeout; `0` leaves the bound out:

```python
# settings.py
TASKIQ_POSTGRESQL_TARGETS = [
    {"kind": "broker", "table_name": "jobs", "field_for_task_id": "Text"},
    {"kind": "result_backend", "table_name": "job_results"},
]
TASKIQ_POSTGRESQL_LOCK_TIMEOUT = 5
```

The migrations read the settings when they run, not when they were written.
With `TASKIQ_POSTGRESQL_TARGETS` missing or empty, or with a table name that
is not a plain SQL identifier (letters, digits, `_` and `$`, not starting with
a digit), `migrate` raises `ImproperlyConfigured` before any statement runs,
instead of recording migrations that created nothing, and `manage.py check`
reports `taskiq_postgresql.E001`, or `taskiq_postgresql.E002` for a lock
timeout that is not a whole number of seconds. The check only reads the
settings; the paths, and so the table names of their components, are
imported when `migrate` or `sqlmigrate` runs. Unlike the `taskiq postgresql`
commands, the app imports them with the project's own `sys.path`, the one
`manage.py` starts with its own directory first, and never from the
directory the command runs in.

Django never applies a migration twice, so a table you add to the settings,
or rename, after the app was migrated is not created by the next `migrate`.
Create it with `taskiq postgresql migrate` ([Apply it with
migrate](#apply-it-with-migrate)) or with the SQL `taskiq postgresql plan`
prints for that database ([Plan and check](#plan-and-check)), or keep it in
a migration of your own, written by the format below.

The app's migrations reverse to nothing, the same default as the Alembic
`downgrade()`: the tables and their rows stay, because the steps only add
tables, columns and indexes that an earlier release ignores. Drop the tables
yourself if you mean to. To unapply them:

```bash
python manage.py migrate taskiq_postgresql zero
```

That also unapplies every migration of your other apps that depends on one of
this app's, such as a migration that grants a role access to the tables or
adds a trigger to them, and each of those runs its own reverse: a reversed
`CreateModel` drops its table, with its rows. `migrate taskiq_postgresql zero
--plan` lists them without running anything.

The migrations run on the database `migrate` migrates (`--database`), and a
database router's `allow_migrate` decides for the label `taskiq_postgresql`
as for any app. On a database that is not PostgreSQL they raise
`NotSupportedError`: keep the app off it with a router.

To keep the tables in an app of your own instead of installing this one,
`generate --format django` writes a migration file with every step, its SQL
written out, so that the file never imports this package. Writing the file
needs no Django, so the format works without the `django` extra:

```bash
taskiq postgresql generate app:scheduler --format django --option dependencies=shop.0004_orders > shop/migrations/0005_taskiq_tables.py
```

The option `dependencies` names the migrations it follows, as
`app_label.migration_name` separated by commas, usually the latest migration
of that app; by default there are none. Each step becomes a
`migrations.RunSQL` with the advisory lock (`--no-lock` leaves it out) and the
lock timeout (`--lock-timeout`), whose reverse runs nothing; keep the
migration atomic, since both only last inside its transaction.
`--no-transaction` changes nothing: Django runs each migration in a
transaction of its own. Table names must be plain SQL identifiers, and any
other option exits with 2. `plan --format django` writes the same file with
only the steps the database it reads still lacks, under a first comment line
that names that database. A released step never changes, so when a release
adds one, add a migration with the new step.

## Compatibility

- **Python and dependencies**: the supported Python versions are the
  `requires-python` field of
  [`pyproject.toml`](https://github.com/z22092/taskiq-postgresql/blob/main/pyproject.toml);
  its `dependencies` and extras hold the supported ranges of taskiq,
  pydantic, the drivers, SQLAlchemy, SQLModel and Alembic.
- **PostgreSQL**: the test suite runs on PostgreSQL 17 and 18.
- **Drivers**: asyncpg, psycopg 3 and psqlpy, selected with the `driver`
  argument. The test suite runs against all three.

## Upgrading

Coming from 0.4.x? Follow "Upgrading from 0.4.x" in the
[changelog](https://github.com/z22092/taskiq-postgresql/blob/main/CHANGELOG.md).
It is the only upgrade guide.

## Troubleshooting

- `TypeError: PostgresqlBroker.__init__() missing 1 required positional argument:
  'dsn'` (or the same for `PostgresqlResultBackend` or
  `PostgresqlSchedulerSource`): pass a connection string.
- `TypeError: PostgresqlBroker needs a non-empty PostgreSQL connection string for
  'dsn', but got None.`: the value you passed is `None` or empty, usually an
  environment variable that is not set. For an empty string the message says
  `got ''`.
- `SchemaNotReadyError: ... is not ready and run_migrations is off` on
  startup: the table, or a column this release adds, is missing in the
  database the component connects to. The message names them. Create them with
  `taskiq postgresql migrate`, with the SQL `taskiq postgresql plan` prints or
  with an Alembic revision, or start once with `run_migrations=True`;
  `taskiq postgresql check` shows what is missing
  ([Plan and check](#plan-and-check)).
- `relation "..." does not exist`: the tables are missing. Start once with
  `run_migrations=True`, or create them as described in
  [Migrations](#migrations).
- `ImportError: ... is not installed`: install the extra for the driver you
  selected, for example `pip install "taskiq-postgresql[psycopg]"`.
- `DatabaseConnectionError` on startup: the DSN, the network path or TLS
  settings are wrong; the message carries the driver's own error.
- A task is sent but no worker runs it: use the same `channel_name` and
  `table_name` on both sides. A delayed task waits for its delay, then for the
  next sweep.
- `column "channel" of relation "taskiq_messages" does not exist` from
  `kick()`, or a worker logging `Error claiming a due message on channel ...`
  with `column "channel" does not exist` every `poll_interval`, or
  `Error claiming message <id> on channel ...` with
  `column "not_before" does not exist` for each `NOTIFY`: the table was
  created by 0.4.x and lacks the new columns. Start once with
  `run_migrations=True`, or run the SQL in "Upgrading from 0.4.x" of the
  changelog.
- `Fatal Python error: PyGILState_Release` when a process that used psqlpy
  exits: psqlpy occasionally aborts the interpreter at exit, after the work is
  done, which turns the exit code into an abort. It happens with psqlpy alone,
  and it was seen with psqlpy 0.13.1 on Python 3.10, not on 3.13. Where the exit
  code matters on 3.10, use another driver.
- No `Listening started.` line from `taskiq worker` on Python 3.14 or newer:
  taskiq 0.13.0 sets up logging in its worker processes only when they start
  with `spawn`, and Python 3.14 starts them with `forkserver` on Linux. The
  worker runs; only its log lines are missing.

## Contributing and security

Setup, tests, the dependency policy and the release process are in
[CONTRIBUTING.md](https://github.com/z22092/taskiq-postgresql/blob/main/.github/CONTRIBUTING.md).
Changes are listed in
[CHANGELOG.md](https://github.com/z22092/taskiq-postgresql/blob/main/CHANGELOG.md).
Report vulnerabilities as described in
[SECURITY.md](https://github.com/z22092/taskiq-postgresql/blob/main/SECURITY.md),
not in a public issue.

## License

MIT. See
[LICENSE](https://github.com/z22092/taskiq-postgresql/blob/main/LICENSE).
