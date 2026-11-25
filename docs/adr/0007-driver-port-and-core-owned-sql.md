# 7. Driver port and core-owned SQL

- Status: Proposed
- Date: 2026-11-25

## Context

Each driver implements a wide interface and decides alone what the core
should decide. `QueryDriver` in `abc/driver.py` has twelve methods and
`ListenDriver` three, and the same call behaves differently per driver: a
claim is bounded by `_answered_within` on psycopg, by `fetchrow(timeout=...)`
on asyncpg and not at all on psqlpy. `fetch_row(query)` and `execute(query)`
take SQL text from any caller, the broker formats its `NOTIFY` with an
f-string (`_send_notification`), a driver error reaches the user as
`str(error)`, which can quote the connection string (only the CLI strips it,
`_without_dsn` in `cli.py`), and the broker logs as `taskiq.asyncpg_broker`
whatever its driver.

## Decision

| Layer | May | May not |
|---|---|---|
| Components (broker, result backend, scheduler source) | call their own store | see the driver, build SQL |
| Stores (messages, results, schedules, schema) | run the `Statement`s of their catalog | run SQL from anywhere else |
| SQL catalog (core, the CLI's catalog read included) | build `Statement`s from `Identifier`s (ADR 0008) | take SQL text from outside |
| Driver (plugin, ADR 0010) | carry a `Statement` and typed parameters; own the pool, `LISTEN` and `abort` | build SQL; decide transactions, deadlines or retries |
| Codec (plugin, ADR 0010) | turn payload bytes into stored bytes and back | touch the database |

`fetch_row` and `execute` leave the public API, and `NOTIFY` becomes
`SELECT pg_notify($1, $2)`. Only the core builds a `Statement`; a test that
reads the source keeps it so. That is hygiene; the security control is the
identifier rule plus bind parameters.

The driver contract, `typing.Protocol`s valid at the Python 3.10 floor:

```python
class ParamType(Enum):
    TEXT, INT4, INT8, FLOAT8, JSONB, UUID, BYTEA, TIMESTAMPTZ
    INT4_ARRAY, UUID_ARRAY  # heartbeat and release of many rows (ADR 0009)
    TEXT_ARRAY  # `task_name = ANY($n)`, the adoption of `channel IS NULL` rows (ADR 0009)

class Capabilities(NamedTuple):
    abort: bool  # can drop one connection without waiting for the server

class Connection(Protocol):
    async def fetch(self, statement: Statement, params: Sequence[object]) -> list[Row]: ...
    async def execute(self, statement: Statement, params: Sequence[object]) -> None: ...
    async def run_script(self, script: Script) -> None: ...  # DDL, no params
    def transaction(self) -> AbstractAsyncContextManager["Connection"]: ...
    async def abort(self) -> None: ...  # NotSupported unless capabilities.abort

class Listener(Protocol):
    async def start(self) -> None: ...  # returns once LISTEN is active
    async def stop(self, *, timeout: float) -> None: ...
    async def ping(self, *, timeout: float) -> None: ...
    def __aiter__(self) -> AsyncIterator[str]: ...  # raw payload

class Handle(Protocol):
    async def open(self) -> None: ...  # the first holder resolves the DSN and opens the pool
    def connection(self, *, timeout: float) -> AbstractAsyncContextManager[Connection]: ...
    def listener(self, channel: Identifier) -> Listener: ...
    async def release(self, *, timeout: float) -> None: ...  # the last one closes the pool

class Driver(Protocol):
    name: str
    capabilities: Capabilities
    def encode(self, value: object, type_: ParamType) -> object: ...
    def decode(self, value: object, type_: ParamType) -> object: ...
    def acquire(self, dsn_source: str | Callable[[], str]) -> Handle: ...
```

- A `Statement` declares the types of its parameters and result columns, so a
  driver converts by `ParamType` only (this absorbs `encode_params` and
  `encode_psqlpy_params`).
- One driver instance can serve several components, each and each listener
  holding one handle. Handles match on the DSN source, the same string or the
  same callable object, not on the resolved string, since a callable returning
  a token gives a new string on every call; a different source on the same
  instance is an error. The pool closes at the last `release`, with the
  longest deadline any holder asked for.
- Deadlines, giving up, health checks and reopening live in the core:
  `_answered_within` and `_give_up` (psycopg) are generalized over
  `Connection.abort`, `check_listen` over `Listener.ping`; `_ReopenWaits`
  stays in the broker.
- One error mapping in the core raises the package's own errors with a
  generic message, the DSN and its password removed as `_without_dsn` and
  `_passwords` do, and the driver's text dropped when it could not read the
  DSN, as `_unreadable` decides.
- Loggers are `taskiq_postgresql.*`; external values are logged with `repr`,
  cut to a fixed length, as `_payload_excerpt` does.

## Consequences

- **BREAKING**, each with its CHANGELOG entry: `QueryDriver`, `ListenDriver`,
  `fetch_row` and `execute` go; the broker's `connection_kwargs` and
  `pool_kwargs` and the other components' `**connect_kwargs` become `pool=`
  and `connect_kwargs=` handed to the driver; the broker's logger name changes.
- A third-party driver implements these protocols and passes the
  conformance tests; it never builds SQL.
- A driver without `abort` cannot bound a query on the server; ADR 0009 says
  what that costs psqlpy.
