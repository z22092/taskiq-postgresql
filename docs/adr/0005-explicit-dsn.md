# 5. The DSN is a required argument

- Status: Accepted (0.5.0)
- Date: 2026-11-22

## Context

Up to 0.4.x, `PostgresqlBroker`, `PostgresqlResultBackend` and
`PostgresqlSchedulerSource` defaulted `dsn` to a URL with a user name and
password for a server on localhost. A forgotten argument then connected to
whatever answered there, and the repository carried credential-shaped
strings that secret scanners flag.

## Decision

- `dsn` is a required first parameter with no default, so positional callers
  keep working and type checkers flag a call without it. Omitting it raises
  Python's own `TypeError`.
- Passing `None` (what `os.environ.get` returns for an unset variable), an
  empty or blank string, or a value that is neither a string nor a callable
  raises `TypeError` at construction, with an example DSN that has no
  password (`require_dsn` in `taskiq_postgresql/utils/dsn.py`).
- A callable returning the DSN is still accepted. `startup()` calls it once
  and hands the string to every driver of the component (`resolve_dsn`);
  until then the drivers hold an empty connection string and refuse to
  connect (`dsn_before_startup`). A callable that returns `None`, a
  non-string or a blank string makes `startup()` raise `TypeError`.

## Consequences

- Breaking change, released in 0.5.0 and described in `CHANGELOG.md`: code
  that relied on the default must pass a DSN.
- A missing DSN fails with a message naming the component instead of later
  with a connection error against an unexpected server: at construction for a
  value, at `startup()` for a callable.
- A component built with a callable DSN cannot query before `startup()`; it
  raises `DatabaseConnectionError` instead of connecting to the local
  defaults.
- `.secrets.baseline` no longer lists the old defaults.
