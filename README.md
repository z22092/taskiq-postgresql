# taskiq-postgresql

PostgreSQL integration for [taskiq](https://taskiq-python.github.io/): a broker
that delivers messages with `LISTEN`/`NOTIFY`, a result backend, and a
persistent schedule source, all in one PostgreSQL database. It works with three
drivers, [asyncpg](https://magicstack.github.io/asyncpg/),
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
NOT EXISTS`). It is off by default, so a deployment where the application user
may not create tables can leave it off and create the tables itself, using the
layout in [Database schema](#database-schema).

Start a worker. Start it before anything sends tasks: the worker claims a
message when it receives its `NOTIFY`, and a notification sent while no worker
is listening is not replayed.

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
| `channel_name` | `str` | `"taskiq"` | `LISTEN`/`NOTIFY` channel. Use a different one per queue that shares a database. |
| `table_name` | `str` | `"taskiq_messages"` | Table that holds pending messages. |
| `driver` | `"asyncpg"`, `"psqlpy"` or `"psycopg"` | `"asyncpg"` | Database driver. `"pg8000"` is in the type annotation but raises `ValueError`. |
| `max_retry_attempts` | `int` | `5` | Stored on the broker; the delivery code does not read it. |
| `connection_kwargs` | `dict` or `None` | `None` | Keyword arguments for the driver's pool and for its `LISTEN` connection, so each option must be valid for both. |
| `pool_kwargs` | `dict` or `None` | `None` | Stored on the broker; the drivers do not read it. Use `connection_kwargs`. |
| `run_migrations` | `bool` | `False` | Create the table and index on startup. |

### PostgresqlResultBackend

| Argument | Type | Default | Meaning |
|---|---|---|---|
| `dsn` | `str` or `Callable[[], str]` | required | Connection string. |
| `keep_results` | `bool` | `True` | When `False`, a result row is deleted after it is read. |
| `table_name` | `str` | `"taskiq_results"` | Table that holds results. |
| `field_for_task_id` | `"VarChar"`, `"Text"` or `"Uuid"` | `"Uuid"` | SQL type of the `task_id` column. |
| `serializer` | `TaskiqSerializer` or `None` | `None` | Result serializer; `None` means `PickleSerializer()`. |
| `driver` | `"asyncpg"`, `"psqlpy"` or `"psycopg"` | `"asyncpg"` | Database driver. |
| `run_migrations` | `bool` | `False` | Create the table and index on startup. |
| `**connect_kwargs` | `Any` | none | Keyword arguments for the driver's connection pool. |

Results are deserialized with the serializer, and pickle can run code while
loading. Anyone who can write to the results table can therefore run code in
the process that reads it. Pass `serializer=JSONSerializer()` (from
`taskiq.serializers`) when the table is not fully trusted.

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

Table names are placed in SQL as written. Use identifiers you control, never
user input.

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

## TLS

To verify the server certificate and its host name, set `sslmode=verify-full`
and point `sslrootcert` at the CA bundle. asyncpg and psycopg read both from the
DSN:

```text
postgresql://<user>@<host>:5432/<database>?sslmode=verify-full&sslrootcert=/path/to/ca.pem
```

psqlpy takes the equivalent keyword arguments instead:

```python
import os

from psqlpy import SslMode

from taskiq_postgresql import PostgresqlBroker

broker = PostgresqlBroker(
    dsn=os.environ["POSTGRESQL_URL"],
    driver="psqlpy",
    connection_kwargs={
        "ssl_mode": SslMode.VerifyFull,
        "ca_file": "/path/to/ca.pem",
    },
)
```

Keep the password out of the source and out of version control: read the DSN
from the environment or a secret store, or pass a callable as `dsn`.

## Database schema

With `run_migrations=True` the package creates these tables. Every column is
`NOT NULL`, and each table also has a `created_at timestamptz DEFAULT now()`
column.

| Table (default name) | Columns | Indexes |
|---|---|---|
| Broker (`taskiq_messages`) | `id SERIAL` primary key, `task_id`, `task_name VARCHAR`, `message BYTEA`, `labels JSONB` | hash on `id` |
| Result backend (`taskiq_results`) | `task_id` primary key, `result BYTEA` | hash on `task_id` |
| Scheduler source (`taskiq_schedulers`) | `id UUID` primary key, `task_name VARCHAR(100)`, `schedule JSONB`, `updated_at` | hash on `id` and on `task_name` |

`task_id` has the type set by `field_for_task_id`. A broker message is deleted
when a worker claims it (`DELETE ... RETURNING`), which is why only one worker
gets each message.

## Compatibility

- **Python and dependencies**: the supported ranges are in
  [`pyproject.toml`](https://github.com/z22092/taskiq-postgresql/blob/main/pyproject.toml)
  (`requires-python`, `dependencies` and the driver extras). PyPI shows the
  Python requirement and the extras but not the version ranges.
- **PostgreSQL**: the test suite runs on PostgreSQL 17 and 18.
- **Drivers**: asyncpg, psycopg 3 and psqlpy, selected with the `driver`
  argument. The tests that need a database run once per installed driver.

## Upgrading from 0.4 to 0.5

The full list is in the
[changelog](https://github.com/z22092/taskiq-postgresql/blob/main/CHANGELOG.md).
Three changes can break an existing application:

- **`dsn` is required.** The old default embedded a user name and password for a
  server on localhost and is gone. Code that relied on it, or on
  `os.environ.get("...")` returning `None`, now gets `TypeError`. Pass the DSN
  explicitly, as in the quickstart.
- **Pydantic v1 and Python 3.9 are dropped.** The package needs `pydantic>=2.5`
  and Python 3.10 or newer, and works with taskiq 0.11.7 up to 0.13.
- **`PostgresqlBroker.startup()` raises `DatabaseConnectionError`** when the
  `LISTEN` connection cannot start, with every driver. Handle it where you start
  the broker (`from taskiq_postgresql.exceptions import DatabaseConnectionError`).

Schedules saved by 0.4 under pydantic v1 keep working. A numeric `cron_offset`
is converted when the schedule is loaded, and a stored row that still fails
validation is skipped and logged, so one bad row does not empty the schedule
source.

## Troubleshooting

- `TypeError: ... missing required argument 'dsn'`: pass a connection string, and
  check the environment variable it comes from is set.
- `relation "..." does not exist`: the tables are missing. Start once with
  `run_migrations=True`, or create them with the layout above.
- `ImportError: ... is not installed`: install the extra for the driver you
  selected, for example `pip install "taskiq-postgresql[psycopg]"`.
- `DatabaseConnectionError` on startup: the DSN, the network path or TLS
  settings are wrong; the message carries the driver's own error.
- A task is sent but no worker runs it: start the worker before the sender, and
  use the same `channel_name` on both sides.

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
