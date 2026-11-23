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

With uv: `uv add "taskiq-postgresql[asyncpg]"`.

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
is off by default; you can leave it off and create the tables yourself, using
the layout in [Database schema](#database-schema).

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
| `run_migrations` | `bool` | `False` | Create the table and index on startup, and add the `channel` and `not_before` columns with their index. |
| `poll_interval` | `float` | `1.0` | Seconds between two sweeps of the table for due messages; see [Delivery](#delivery). A value that is not a finite number above zero raises `ValueError`. |

### PostgresqlResultBackend

| Argument | Type | Default | Meaning |
|---|---|---|---|
| `dsn` | `str` or `Callable[[], str]` | required | Connection string. |
| `keep_results` | `bool` | `True` | When `False`, a result row is deleted after it is read. |
| `table_name` | `str` | `"taskiq_results"` | Table that holds results. |
| `field_for_task_id` | `"VarChar"`, `"Text"` or `"Uuid"` | `"Uuid"` | SQL type of the `task_id` column. |
| `serializer` | `TaskiqSerializer` or `None` | `None` | Result serializer; `None` means `PickleSerializer()`; see the note below before changing it. |
| `driver` | `"asyncpg"`, `"psqlpy"` or `"psycopg"` | `"asyncpg"` | Database driver. |
| `run_migrations` | `bool` | `False` | Create the table and index on startup. |
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
| `run_migrations` | `bool` | `False` | Create the table and indexes on startup. |
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

If the `LISTEN` connection closes or fails, the worker keeps delivering through
the sweep alone, so every message then waits for the next sweep. The connection
is opened again only when the broker starts again. With asyncpg and psycopg the
worker logs a warning when this happens; psqlpy gives no sign that its `LISTEN`
connection ended, so with psqlpy nothing is logged.

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
from the code and compares them with these blocks. To create the tables
yourself, run them with your own names.

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

## Compatibility

- **Python and dependencies**: the supported Python versions are the
  `requires-python` field of
  [`pyproject.toml`](https://github.com/z22092/taskiq-postgresql/blob/main/pyproject.toml);
  its `dependencies` and driver extras hold the supported ranges of taskiq,
  pydantic and the drivers.
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
- `relation "..." does not exist`: the tables are missing. Start once with
  `run_migrations=True`, or create them with the layout above.
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
