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

- `dsn` has no usable default. Omitting it, or passing `None` (what
  `os.environ.get` returns for an unset variable), raises `TypeError` at
  construction, with an example DSN that has no password
  (`utils/require_dsn.py`).
- `dsn` stays the first parameter, so positional callers keep working.
- A callable returning the DSN is still accepted; the drivers receive the
  string it returns at construction (`resolve_dsn`).

## Consequences

- Breaking change, released in 0.5.0 and described in `CHANGELOG.md`: code
  that relied on the default must pass a DSN.
- A missing setting fails at startup with a message naming the component,
  instead of later with a connection error against an unexpected server.
- `.secrets.baseline` no longer lists the old defaults.
