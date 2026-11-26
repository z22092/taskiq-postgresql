# 10. Plugin registry and codec interface

- Status: Proposed
- Date: 2026-11-25

## Context

`driver=` takes one of three names, which `get_db_driver` and
`get_db_listen_driver` map to classes in code: a driver cannot come from
another package, and a configured instance cannot be passed in.

Payload bytes reach the database as produced: the broker stores what taskiq's
formatter built, the result backend what its serializer built
(`PickleSerializer` by default, read back with `loadb`), the scheduler source
JSON. Nothing sits between those bytes and the database. A cipher in a
`TaskiqSerializer` covers too little. `AsyncBroker` defaults its formatter to
`ProxyFormatter(self)` (taskiq 0.11.7 and 0.13.0, `taskiq/abc/broker.py`),
whose `dumps` calls `broker.serializer.dumpb`, and `PostgresqlBroker` does
not replace it; so under the default a serializer covers the message body,
but never the JSONB `labels` column, and not at all when the user configures
`JSONFormatter` or another formatter. The result backend has its own
serializer path, separate from the broker's.

The package already loads migration formats by entry point (`load_format` in
`migrations.py`): only the entry point with the exact name asked for, the
one whose distribution is named `taskiq-postgresql` first, and an error when
several other distributions register the name. A distribution's name is its
own metadata, which any installed distribution can claim.

Encryption of payloads at rest and a result serializer without pickle are
not decided here (see the spike note); this record only defines where they
plug in.

## Decision

- Entry-point groups `taskiq_postgresql.drivers` and `taskiq_postgresql.codecs`
  join `taskiq_postgresql.migration_formats`, loaded through one registry that
  generalizes `load_format`: by exact name and on demand, importing only the
  matching entry point.
- Bundled names resolve from a table inside the package before any
  entry-point lookup, so no installed distribution can shadow them, even one
  that declares this package's name. The bundled names are every entry the
  package's own `pyproject.toml` declares in these groups, derived and not a
  hand list: today the `sql`, `alembic` and `django` migration formats, and
  with this change the `asyncpg`, `psqlpy` and `psycopg` drivers and the
  `legacy` codec. A test compares the internal table with the entry points of
  the installed `taskiq-postgresql` distribution, group by group, and fails
  on a name in one and not the other. Another name registered by several
  distributions is an error that names them.
- `driver=` takes a name or an instance; an instance is used as is, and can
  serve several components through the handles of ADR 0007.
- The claim is limited to name collisions: the registry decides which code a
  name loads and refuses ambiguity. It neither vets nor isolates a plugin,
  whose code runs in the worker; the documentation lists plugins as dangerous
  functionality: install only packages you trust, and pin them.
- The codec interface:

  ```python
  class CodecContext(NamedTuple):
      table: Literal["messages", "results", "schedules"]  # logical name
      column: str  # the payload column
      key: str  # task_id, or the schedule id

  class Codec(Protocol):
      name: str
      def encode(self, data: bytes, *, context: CodecContext) -> bytes: ...
      def decode(self, data: bytes, *, context: CodecContext) -> bytes: ...
  ```

  A codec sits in the stores, at the database boundary, below any formatter
  or serializer; it never touches the database or picks another codec.
  `labels` never pass through it: the broker reads `labels["timeout"]`
  (ADR 0009).
- Each component decodes only with the codecs its own configuration names,
  never with whatever is installed or whatever the stored bytes suggest.
- The default codec, `legacy`, returns its input unchanged, so payloads stay
  byte for byte those of a release cut from `main` before 1.0, both ways.
- The envelope layout, the cipher (AES-GCM or other), the contents and layout
  of the additional authenticated data, nonce generation and key rotation
  belong to a later encryption record. This record fixes only what an
  encrypting codec receives.

## Consequences

- Entry-point names and the `Codec` and `CodecContext` signatures become
  public contracts.
- An installed package cannot replace a bundled driver, codec or format, and
  two packages claiming one name fail loudly instead of by install order.
- `key` is the `task_id`, and taskiq's retry middlewares kick a retry with the
  same `task_id`, so a codec that authenticates `context` binds a payload to a
  `task_id`, not to a row: it can move between rows of one task, and the
  context gives no replay protection. In the results table, where `task_id`
  is the primary key, it does bind the row.
- Residual risk: the result serializer stays pickle by default until a
  serializer change that is not decided here. A role with `UPDATE` on the results table can
  still make a reader unpickle what it wrote; the codec allow-list does not
  cover the serializer.
- With `legacy`, a release cut from `main` before 1.0 reads what 1.0 writes
  and the other way round; any other codec is turned on only after every
  reader of the table runs 1.0.
