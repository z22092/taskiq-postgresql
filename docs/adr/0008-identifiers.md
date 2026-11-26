# 8. Identifiers: today's unquoted names, checked once

- Status: Proposed
- Date: 2026-11-25

## Context

Table and channel names come from configuration and are written unquoted
into SQL text by the builders of `abc/query.py`. The components check none of
them, so any string reaches the SQL. Two partial rules exist. The public
`check_plain_identifier` (`migrations.py`, in `__all__`; letters, digits, `_`
and `$`, not starting with a digit or `$`) is called by `_name` in
`contrib/sqlalchemy.py`, which then folds to lower case and cuts the name to
63 bytes, by `contrib/django/conf.py` on every collected table name, and by
`render_steps`, which the `alembic` and `django` migration formats use.
`_check_table_name` in `migrations.py` guards the `sql` format,
`catalog_query`, `RenderedStep` and `table_schema`, and refuses only an empty
name, a line break or a NUL.

PostgreSQL folds an unquoted name to lower case, but only `A` to `Z` in a
multibyte encoding (`downcase_identifier`), and cuts any identifier at 63
bytes (`truncate_identifier`). It names the primary key and the `SERIAL`
sequence itself, keeping the `_pkey` and `_id_seq` suffixes whole
(`makeObjectName`). This package names its indexes `<table>_<column>_idx`
(`index_name`), which the parser cuts like any name, so on a long table two
index names, or an index name and the table name, can be equal, and
`CREATE INDEX IF NOT EXISTS` then skips an index without an error. asyncpg's
listen driver quotes the channel in `LISTEN` (`_quote_ident` in
`drivers/_asyncpg.py`) while the broker's `NOTIFY` folds it.
The migration lock key is `hashtext` of the table name as configured
(`MigrationLockQuery`, and `_lock_statement` for scripts), a contract between
releases.

## Decision

- One `Identifier` type checks every table and channel name at construction:
  ASCII letters, digits, `_` and `$`, not starting with a digit or `$`, and
  not a reserved key word of PostgreSQL. It replaces `_name` and
  `_check_table_name`, so the components, the CLI and the integrations accept
  the same names.
- `check_plain_identifier` stays public as a thin wrapper that delegates to
  `Identifier` and returns the name unchanged, with no fold and no warning.
  Callers outside the package, and `contrib/django/conf.py`, keep working; the
  names it refuses beyond today's rule (reserved key words, over 63 bytes) are
  a behaviour change, listed under Consequences.
- A name with upper case letters is folded to lower case, the name PostgreSQL
  already uses for it, with a `DeprecationWarning`.
- The folded name is at most 63 bytes; a longer one is refused instead of
  being cut by the server.
- Nothing is quoted. Statements keep writing names bare, so the released
  migration steps stay byte for byte the same (`tests/golden/migration_steps.sql`).
- The lock key stays `hashtext` of the table name exactly as configured, not
  folded, so a release that folds and one that does not serialize on the same
  key. `MigrationLockQuery` and `_lock_statement` get one source.
- Channel: `LISTEN` and `pg_notify` use the folded name, so all three drivers
  agree, asyncpg's quoted `LISTEN` included. The `channel` column value and
  the claim's bind stay the configured string, raw, so rows written by a
  release cut from `main` with a mixed-case channel still match.
- Derived names: the package computes every name a table's steps create, the
  index names cut at 63 bytes as the parser cuts them and `_pkey` and
  `_id_seq` as `makeObjectName` builds them, and refuses a table name for
  which two of them, or one of them and the table name, are equal. That gives
  these limits, the same for every name of the length:

  | Component | Index suffixes | No derived name cut up to | Longest name accepted |
  |---|---|---|---|
  | broker | `_id_idx`, `_not_before_idx` | 48 bytes | 61 bytes |
  | result backend | `_task_id_idx` | 51 bytes | 62 bytes |
  | scheduler source | `_id_idx`, `_task_name_idx` | 49 bytes | 61 bytes |

- A refused name appears in the error with `repr`, cut to a fixed length.
- Scripts (`taskiq postgresql generate`, the `sql`, `alembic` and `django`
  formats, `contrib/django/*` and `contrib/django_format.py` included) have no
  bind parameters; names there stay literals written from a checked
  `Identifier`.

## Consequences

- **BREAKING:** names that work today and will be refused at construction:
  - over 63 bytes, which PostgreSQL cuts today;
  - with non-ASCII letters, which PostgreSQL accepts unquoted but does not
    fold in UTF-8, so the package cannot tell the name the server keeps;
  - broker and scheduler names of 62 or 63 bytes, and result backend names of
    63 bytes, whose index names collide: one index is already missing today;
  - anything else that is not an identifier (a space, a quote, `;`), which
    today reaches the SQL text as is;
  - for callers of `check_plain_identifier`, a reserved key word or a name
    over 63 bytes, which it accepts today.
- Migration path for such a table, given in the upgrade guide: with the
  workers stopped, rename it (`ALTER TABLE ... RENAME TO`) to a name the rule
  accepts, configure that name, and run the migration, which creates the
  indexes under the derived names. The old index names are listed for
  dropping. The rename changes the lock key, hence the stopped workers.
- Names between the two limits of the table keep working, with cut index
  names; mixed-case names keep working and warn.
- Every accepted name gets the same lock key it has today.
