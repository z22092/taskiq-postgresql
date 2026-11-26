# Spike: the 1.0 data layer

- Date: 2026-11-25
- Base: `main` @ `e4be3dc`
- Method: read-only survey of the SQL layer, the driver calls and the tests,
  of reference projects, of taskiq's worker at the versions the package
  supports, and of the OWASP sources; adversarial reviews of the plan against
  the code and the installed taskiq.
- Decisions: [ADR 0007](../adr/0007-driver-port-and-core-owned-sql.md)
  (driver port, core-owned SQL), [ADR 0008](../adr/0008-identifiers.md)
  (identifiers), [ADR 0009](../adr/0009-delivery-modes.md) (delivery modes)
  and [ADR 0010](../adr/0010-plugin-registry-and-codec-interface.md) (plugin
  registry, codec interface). The records hold the decisions; this note holds
  the order of the work, the files and statements each step changes, the
  verification plan, the references and the OWASP map.

## Summary

Today each of the three drivers owns transactions, deadlines, errors and
quoting, behind a twelve-method `QueryDriver` and a three-method
`ListenDriver`; delivery is at most once, with an `ack` that does nothing; no
driver can be plugged in from outside. The 1.0 data layer moves all SQL and
its semantics into one core: components call stores, stores run the
statements of a catalog, and a driver only carries typed statements over a
pool and a `LISTEN` connection. Delivery becomes at least once by default,
with a lease kept in `not_before`, so claims of releases cut from `main` skip
a leased row; drivers and codecs become plugins loaded by exact name.

## PR sequence

This series has its own numbering. It lands before any encryption or
serializer change (see "Not decided here"). No release is cut between PR6a
and PR6d.

| PR | Content |
|---|---|
| PR0 | ADRs 0007 to 0010 and this note. No code. |
| PR1 | Tests, green on today's code, of what the design relies on: the claim SQL `main` runs today (unreleased; 0.5.0 will ship it) leaves a row due later in place and serves a `channel IS NULL` row by id; the migration lock key is `hashtext` of the configured name, mixed case included; names every future rule keeps are accepted; binds reach the server as `$n` and values round-trip per driver. |
| PR2 | SQL core: `Identifier`, typed `Statement`, the catalog (the CLI's catalog read included), `pg_notify` with binds, the migration lock in one place, derived names checked. Golden file untouched. |
| PR3 | Stores over the current drivers; migrations and the CLI stop reaching into `component.driver`. |
| PR4 | The driver port, three adapters, deadlines, abort and health in the core, the error mapping. The `LISTEN` tests check capabilities instead of driver names and become the conformance tests. |
| PR5 | `QueryDriver` and `ListenDriver` removed (breaking); the registry; `driver=` takes an instance. |
| PR6a | Delivery DDL: step 3 (`attempts`, `lease_token`, `dead_at`), appended to the golden file. |
| PR6b | Lease, ack, heartbeat, release and adoption of `channel IS NULL` rows, as an opt-in. |
| PR6c | Dead letter and `max_attempts`. |
| PR6d | At least once becomes the default; upgrade guide. |
| PR7 | The codec interface, the allow-list and the `legacy` codec. |
| PR8 | `pool=` and `connect_kwargs=` in place of `connection_kwargs`, `pool_kwargs` and `**connect_kwargs`; the final 1.0 upgrade guide. |

Every PR: a test red before green; a CHANGELOG entry when a user sees the
change; `make lint typecheck test test-lowest build`; a static analysis scan;
an independent review with no open blocking finding; its rows in the OWASP
map below.

Not decided here, each for an ADR of its own:

- Encryption of payloads at rest: the envelope, the cipher, the additional
  authenticated data, nonce generation and key rotation. ADR 0010 fixes only
  what a codec receives.
- A result serializer without pickle, which closes the residual risk under
  `v5.0.0-1.5.2` below.
- `GRANT`s rendered per component, with a runtime role that has no DDL.
- Publishing the conformance tests as a distribution of their own, with its
  own CD job and trusted publisher, which amends ADR 0004.

## Files and statements by step

Paths are under `taskiq_postgresql/`.

| PR | Files | Statements and builders |
|---|---|---|
| PR2 | new `core/sql/` (`Identifier`, `Statement`, catalog); `abc/query.py`; `migrations.py` (`check_plain_identifier`, `_check_table_name`, `_lock_statement`, `catalog_query`); `contrib/sqlalchemy.py` (`_name`, the primary key name in `_table`); `contrib/django/conf.py` and `contrib/django_format.py` (names checked with `check_plain_identifier`, the public wrapper ADR 0008 keeps, and the scripts the django format writes); `contrib/django/migrations/` (the steps it applies); `broker.py` (`_send_notification`); `cli.py` (`_read_database`) | Every builder of `abc/query.py` (`CreateTableQuery`, `CreateIndexQuery`, `AddColumnsQuery`, `RelationsExistQuery`, `InsertQuery`, `InsertOrUpdateQuery`, `DeleteQuery`, `ClaimDueQuery`, `ClaimByIdQuery`, `DeleteByDateQuery`, `SelectQuery`) becomes a catalog `Statement` with the same text; `MigrationLockQuery` and `_lock_statement` get one source; `NOTIFY <channel>, '<id>'` becomes `SELECT FROM pg_notify($1, $2)`; the CLI's `SELECT current_database(), current_schemas(false), (<catalog_query>)` joins the catalog. |
| PR3 | new `core/stores/`; `broker.py`; `result_backend.py`; `scheduler_source.py`; `drivers/_schema.py` (`start_table` moves to the core); `migrations.py` (`table_schema` and `_driver_schema` stop reading `QueryDriver` attributes); `cli.py` (`_connections`, `_migrate`, `_apply`, `_read_database`) | Message store: `InsertQuery` (`kick`), `ClaimDueQuery` and `ClaimByIdQuery` (`_claim`), `pg_notify`. Result store: `InsertOrUpdateQuery` (`set_result`), `SELECT 1` (`is_result_ready`), `SelectQuery` and `DeleteQuery` (`get_result`), `DeleteByDateQuery` (`delete_by_date`). Schedule store: `SelectQuery` (`get_schedules`), `InsertQuery` (`add_schedule`), `DeleteQuery` (`delete_schedule`, `post_send`). Schema store: `RelationsExistQuery`, `MigrationLockQuery`, `CreateTableQuery`, `CreateIndexQuery`, `AddColumnsQuery`, the catalog read. Behaviour unchanged. |
| PR4 | new port module in `core/`; deadlines, abort, health and the error mapping in `core/`; `drivers/_asyncpg.py`; `drivers/_psycopg.py` (`_answered_within`, `_give_up` move); `drivers/_psqlpy.py` (`encode_psqlpy_params` moves); `drivers/_params.py` (`encode_params` becomes `encode`/`decode` by `ParamType`); `drivers/_health.py` (`check_listen` moves); every `DatabaseConnectionError(str(error))` | No statement changes. `LISTEN` moves into `Listener.start` with the folded name; stores call `Connection.fetch`, `execute`, `run_script` and `transaction`. |
| PR5 | `abc/driver.py` (`QueryDriver`, `ListenDriver` removed); `utils/get_db_driver.py` and `utils/get_db_listen_driver.py` replaced by the registry, which generalizes `load_format` and `_entry_points`; `drivers/_schema.py` and `migrations.py`; `pyproject.toml` (`taskiq_postgresql.drivers` group); components (`driver=` instance) | No statement changes. |
| PR6a | catalog (step 3, in the style of `AddColumnsQuery`); `migrations.py`; `tests/golden/migration_steps.sql` (appended only) | `ALTER TABLE ... ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0, ADD COLUMN IF NOT EXISTS lease_token UUID, ADD COLUMN IF NOT EXISTS dead_at TIMESTAMPTZ`. |
| PR6b | `broker.py` (`listen`, the message's `ack`, a `pre_execute` middleware, `shutdown`); message store; catalog; `AGENTS.md` and `docs/architecture.md` (the integer literals of the claim) | Under `at_least_once`, the batch claim (`UPDATE ... RETURNING`), the claim by id with a lease, the adoption of `channel IS NULL` rows (`task_name = ANY($n)`), ack (`DELETE ... AND lease_token = $2 AND dead_at IS NULL`), heartbeat (`UPDATE ... FROM unnest`), release (`UPDATE ... SET not_before = now(), lease_token = NULL`). `at_most_once` keeps a `DELETE ... RETURNING` claim, in a transaction. |
| PR6c | message store; catalog; `broker.py` (`max_retry_attempts` becomes `max_attempts`) | Dead letter (`dead_at = now()`, `not_before = 'infinity'`, `lease_token = NULL`). |
| PR6d | `broker.py` (default `delivery`); `README.md`; `CHANGELOG.md` | None. |
| PR7 | registry (`taskiq_postgresql.codecs` group); stores; `result_backend.py` (`set_result`, `get_result`); `scheduler_source.py`; `broker.py` (`kick`, `listen`) | No SQL change; the stores pass payload columns through the configured codec. |
| PR8 | `broker.py`, `result_backend.py`, `scheduler_source.py` (constructors); `README.md`; `CHANGELOG.md` | None. |

A later change to the result or message tables (a column, a per-column
upsert, a retention delete) adds statements to the catalog of the store that
owns the table, after PR3, and a migration step after the last released one.

## Verification plan

- PR1: the frozen claim SQL of `main` (unreleased; 0.5.0 will ship it; 0.4.x
  deletes the notified id regardless of `not_before`), run on a raw connection,
  leaves a row due later in place and deletes a due one; the by-id statement
  takes a `channel IS NULL` row; the lock key for `jobs` and `Jobs_Mixed`;
  names every future rule keeps; binds seen as `$1` in
  `pg_stat_activity.query` from another connection; value round trips.
- Conformance, on all three drivers; psqlpy skips only the `abort` tests of
  at most once, with a link to psqlpy#180 in the report: fetch, execute, a
  transaction rolled back; `abort` with nothing written; server-side binds;
  encode and decode per `ParamType` both ways; `LISTEN` with a raw payload, a
  `NOTIFY` right after `start`, reopening after a terminate, a silent
  connection caught by `ping` (`tests/silent_proxy.py`); `close` with a
  deadline; an exception carrying no DSN or password.
- Driver handles: double startup, a different DSN source refused, the same
  token callable accepted, the pool closed only by the last handle.
- Delivery, red before green: a subprocess worker killed with SIGKILL
  mid-task gets the message again after the lease; the heartbeat holds a task
  longer than the lease; an unreadable message, an unknown task or a failing
  ack stop the heartbeat at the cap; shutdown with buffered rows loses
  nothing and spends no attempt; SIGTERM with `max_prefetch > 0` runs no task
  twice, on taskiq 0.11.7 (`test-lowest`) and 0.13.0; a released row reaching
  `pre_execute` runs nothing; `attempts` at the limit leads to the dead
  letter with the exact claim count, and a late heartbeat, ack or release
  does not revive it; a `channel IS NULL` row of a registered task is
  adopted, one of another task is not; `ClaimDueQuery` of `main` does not
  take a leased row; an unknown task without a `timeout` label comes back
  after `max_task_seconds`; a row with a mixed-case channel written by
  `main` is claimed by 1.0; `EXPLAIN` of the claim, the adoption and the
  dead letter shows no Sort.
- At most once: giving up before `COMMIT` leaves the row in place.
- psqlpy under at least once: the first deadline expiry rebuilds the pool;
  heartbeat and ack keep running while a claim is stuck; a failed rebuild
  fails the health check.
- Identifiers: a malicious, reserved, non-ASCII or 64-byte name refused; a
  mixed-case name folded with a warning; the golden file byte for byte; each
  component's longest accepted name accepted and the next one refused.
- Limits: a `bool`, a non-integer or an out-of-range limit refused, and
  `query_timeout >= lease - heartbeat interval` refused.
- Codecs: a codec outside the allow-list refused on read; with `legacy`, a
  result written by `main` is read by 1.0 and the other way round, bytes
  equal; `labels` never reach a codec.
- Registry: a name loads only its entry point, and a planted entry point that
  fails on import is never imported; a bundled name never reaches entry
  points, even when another distribution declares this package's name; two
  other packages with one name raise and are named.
- No `# noqa: S608` and no f-string SQL outside the catalog (a test that reads
  the source).
- Scale, measured before any default is promised: throughput with and without
  a `NOTIFY` per `kick`, with batches of 1, 10 and 100; heartbeat rate and
  bloat of the `(channel, not_before, id)` index.

## References

| Source | What was taken |
|---|---|
| taskiq [0.11.7 `cli/worker/run.py`](https://github.com/taskiq-python/taskiq/blob/0.11.7/taskiq/cli/worker/run.py), [`receiver/receiver.py`](https://github.com/taskiq-python/taskiq/blob/0.11.7/taskiq/receiver/receiver.py), [`acks.py`](https://github.com/taskiq-python/taskiq/blob/0.11.7/taskiq/acks.py) | `interrupt_handler` raises `KeyboardInterrupt` on the first signal and `start_listen` runs `shutdown_broker` in `except KeyboardInterrupt`, out of `run_until_complete(receiver.listen())`; `Receiver.callback` returns without ack for an unreadable message or an unknown task and runs the `pre_execute` loop before `run_task`, outside any `try`; the prefetcher never closes `broker.listen()`; `AcknowledgeType` has no `MANUAL`. |
| taskiq [0.11.20 `cli/worker/run.py`](https://github.com/taskiq-python/taskiq/blob/0.11.20/taskiq/cli/worker/run.py), [`receiver/receiver.py`](https://github.com/taskiq-python/taskiq/blob/0.11.20/taskiq/receiver/receiver.py) | The signal sets `shutdown_event` and `shutdown_broker` runs in `finally`, after `receiver.listen(shutdown_event)`; the prefetcher polls `__anext__()` in a task and cancels it on shutdown, with no `aclose()`; no ack for an unreadable or unknown message; `pre_execute` before `run_task`. |
| taskiq [0.13.0 `cli/worker/run.py`](https://github.com/taskiq-python/taskiq/blob/0.13.0/taskiq/cli/worker/run.py), [`receiver/receiver.py`](https://github.com/taskiq-python/taskiq/blob/0.13.0/taskiq/receiver/receiver.py), [`acks.py`](https://github.com/taskiq-python/taskiq/blob/0.13.0/taskiq/acks.py), [`formatters/json_formatter.py`](https://github.com/taskiq-python/taskiq/blob/0.13.0/taskiq/formatters/json_formatter.py), [`middlewares/`](https://github.com/taskiq-python/taskiq/tree/0.13.0/taskiq/middlewares) | `shutdown_broker` in `finally`; `_close_prefetch_state` cancels the pending read, then `aclose()`; `_discard_queued_messages` drops prefetched messages not started; `callback` acks an unreadable or unknown message, runs `pre_execute` before `run_task`, and `_get_ack_time` raises on an invalid `ack_type`; `run_task` turns an exception or a `timeout` into a result, so the ack follows; `MANUAL`; `AsyncBroker` defaults to `ProxyFormatter(self)`, whose `dumps` calls `broker.serializer.dumpb` (the message body only, never `labels`), while `JSONFormatter.dumps` uses `model_dump_json`, no serializer; the retry middlewares re-kick `with_task_id(message.task_id)`. |
| procrastinate 3.12.0, [`procrastinate/sql/__init__.py:10-32`](https://github.com/procrastinate-org/procrastinate/blob/3.12.0/procrastinate/sql/__init__.py#L10-L32) | The core owns SQL as `dict[str, LiteralString]`; connectors only bind. |
| procrastinate 3.12.0, [`procrastinate/psycopg_connector.py:314-366`](https://github.com/procrastinate-org/procrastinate/blob/3.12.0/procrastinate/psycopg_connector.py#L314-L366) | Dynamic names (`LISTEN`) through `psycopg.sql.Identifier`. |
| PGQueuer v2.1.0, [`pgqueuer/ports/driver.py:57-95`](https://github.com/janbjorge/pgqueuer/blob/v2.1.0/pgqueuer/ports/driver.py#L57-L95) | A thin `Driver` Protocol (`fetch`, `execute`, `add_listener`, `notify`). |
| PGQueuer v2.1.0, [`pgqueuer/adapters/drivers/psycopg.py:61-94`](https://github.com/janbjorge/pgqueuer/blob/v2.1.0/pgqueuer/adapters/drivers/psycopg.py#L61-L94) | Native `$n` on psycopg through `AsyncRawCursor`; `SELECT pg_notify($1, $2)`. It quotes `LISTEN` to keep a mixed-case channel; this design folds the name instead (ADR 0008). |
| PGQueuer v2.1.0, [`docs/adr/`](https://github.com/janbjorge/pgqueuer/tree/v2.1.0/docs/adr) ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0024 | Claim by lock contention; notifications signal and never carry job data; delivery is at least once; worker liveness as an application signal; SQL assembled by one composer. |
| SQLAlchemy `rel_2_1_9`, [`lib/sqlalchemy/util/langhelpers.py:475-515`](https://github.com/sqlalchemy/sqlalchemy/blob/rel_2_1_9/lib/sqlalchemy/util/langhelpers.py#L475-L515) | `PluginLoader`: in-process registrations first, then entry points by exact name. It takes the first match; this design refuses an ambiguous name (ADR 0010). |
| PostgreSQL `REL_18_0`, [`src/backend/commands/async.c`](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/commands/async.c#L906-L923) | `PreCommit_Notify` takes `LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)` until commit, serializing every committing transaction that notified; hence `notify=False`. `Async_Notify` refuses a channel name of `NAMEDATALEN` bytes or more. |
| PostgreSQL `REL_18_0`, [`src/backend/commands/indexcmds.c`](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/commands/indexcmds.c), [`src/backend/parser/parse_utilcmd.c`](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/parser/parse_utilcmd.c), [`src/backend/parser/scansup.c`](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/parser/scansup.c) | `makeObjectName` shortens the names and never the label; `ChooseIndexName` names a primary key `<table>_pkey`; `generateSerialExtraStmts` names a sequence from the table, the column and `seq`; `downcase_identifier` folds only `A` to `Z` in a multibyte encoding; `truncate_identifier` cuts at `NAMEDATALEN - 1` bytes. |
| PostgreSQL docs, [`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) (current was 19 when read) | Delivered only at commit; identical channel and payload folded within a transaction; payload under 8000 bytes by default; `pg_notify(text, text)` for non-constant channel and payload. |
| PostgreSQL docs, [`SELECT`, locking clause](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) | `SKIP LOCKED` skips rows it cannot lock at once; an inconsistent view, meant for queue-like tables. |
| [psqlpy-python/psqlpy#180](https://github.com/psqlpy-python/psqlpy/issues/180) | "A cancelled query keeps running on the server and its connection is handed out again", open on 2026-11-25: the next `acquire()` gets that connection and waits behind the query; why psqlpy declares `abort=False`. |

## OWASP map

Top 10:2025 categories and their CWE lists were read on each category's page
at [top10.owasp.org/2025](https://top10.owasp.org/2025/). ASVS 5.0.0
requirements were read in the chapter files of the `v5.0.0_release` tag of
[OWASP/ASVS](https://github.com/OWASP/ASVS/tree/v5.0.0_release/5.0/en), and
are written in the form the standard asks for, `v5.0.0-<chapter>.<section>.<requirement>`
([0x03-What-is-the-ASVS.md](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x03-What-is-the-ASVS.md)).

| Control | Top 10:2025 (CWE) | ASVS 5.0.0 | Lands in | Proven by |
|---|---|---|---|---|
| Server-side binds (`$n`), `pg_notify` with binds, no SQL text in the public API | [A05:2025 Injection](https://top10.owasp.org/2025/A05_2025-Injection/) (CWE-89) | [v5.0.0-1.2.4](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x10-V1-Encoding-and-Sanitization.md), [v5.0.0-1.1.2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x10-V1-Encoding-and-Sanitization.md) | PR2, PR4, PR5 | `pg_stat_activity.query` shows `$1`; no `# noqa: S608` and no f-string SQL outside the catalog |
| `Identifier` allow-list; names only from configuration | [A05:2025 Injection](https://top10.owasp.org/2025/A05_2025-Injection/) | [v5.0.0-2.2.1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x11-V2-Validation-and-Business-Logic.md), [v5.0.0-2.2.2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x11-V2-Validation-and-Business-Logic.md) | PR2 | malicious, reserved, non-ASCII or overlong name refused at construction |
| Codecs decode only from the component's allow-list | [A08:2025 Software or Data Integrity Failures](https://top10.owasp.org/2025/A08_2025-Software_or_Data_Integrity_Failures/) (CWE-502) | [v5.0.0-1.5.2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x10-V1-Encoding-and-Sanitization.md): residual risk, not claimed until the serializer change under "Not decided here" | PR7 | a codec outside the allow-list refused on read |
| Codec context (logical table, column, row key) for an encrypting codec to authenticate | [A04:2025 Cryptographic Failures](https://top10.owasp.org/2025/A04_2025-Cryptographic_Failures/) (CWE-320, CWE-321, CWE-327) | [v5.0.0-11.3.2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md), [v5.0.0-11.3.3](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md), [v5.0.0-11.3.4](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md), [v5.0.0-11.2.2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md): deferred to the encryption record under "Not decided here" | PR7 (interface only) | a test codec receives the context of every read and write |
| Plugins loaded by exact name; bundled names resolved in-process first; an ambiguous name is an error; plugins documented as dangerous functionality | [A08:2025 Software or Data Integrity Failures](https://top10.owasp.org/2025/A08_2025-Software_or_Data_Integrity_Failures/) (CWE-829); [A03:2025 Software Supply Chain Failures](https://top10.owasp.org/2025/A03_2025-Software_Supply_Chain_Failures/) (CWE-1357) | [v5.0.0-15.1.5](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md), [v5.0.0-15.2.4](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md), [v5.0.0-15.2.5](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | PR5, PR7 | a planted entry point that fails on import is never imported; a bundled name never reaches entry points; ambiguity raises |
| Validated limits for batch, lease, heartbeat, `max_attempts` and `max_task_seconds`; a `NOTIFY` payload outside the id range is dropped (`_parse_message_id`), and `notify=False` turns the wake-up off | [A06:2025 Insecure Design](https://top10.owasp.org/2025/A06_2025-Insecure_Design/) (CWE-799) | [v5.0.0-15.1.3](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md), [v5.0.0-15.2.2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md), [v5.0.0-2.4.1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x11-V2-Validation-and-Business-Logic.md) | PR6b, PR6c | `bool`, non-integer and out-of-range limits refused; an invalid `NOTIFY` payload is logged and claims nothing |
| DSN and password redacted in errors and logs | [A09:2025 Security Logging and Alerting Failures](https://top10.owasp.org/2025/A09_2025-Security_Logging_and_Alerting_Failures/) (CWE-532) | [v5.0.0-16.2.5](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x25-V16-Security-Logging-and-Error-Handling.md) | PR4 | conformance: an exception and its log line carry no DSN or password |
| Generic error to the user: no SQL, parameters or stack | [A10:2025 Mishandling of Exceptional Conditions](https://top10.owasp.org/2025/A10_2025-Mishandling_of_Exceptional_Conditions/) (CWE-209) | [v5.0.0-16.5.1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x25-V16-Security-Logging-and-Error-Handling.md) | PR4 | the core's error mapping raises its own exceptions; no raw `str(error)` from a driver |
| External values logged with `repr`, cut to a fixed length | [A09:2025 Security Logging and Alerting Failures](https://top10.owasp.org/2025/A09_2025-Security_Logging_and_Alerting_Failures/) (CWE-117) | [v5.0.0-16.4.1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x25-V16-Security-Logging-and-Error-Handling.md) | PR2, PR4 | a name or payload with a line break logs as one quoted, cut line |

Notes on the map:

- Residual risk under `v5.0.0-1.5.2`: the result serializer stays pickle by
  default until the serializer change under "Not decided here", so a role with `UPDATE` on
  the results table can make a reader unpickle what it wrote; the codec
  allow-list does not cover the serializer (ADR 0010).
- `v5.0.0-11.3.4` (no nonce reused for a key and data element), with the
  cipher, the envelope, the additional authenticated data and key rotation,
  is left to the encryption record under "Not decided here". If that record uses the
  `task_id` as the row key, the binding is to the `task_id`, not the row:
  retry rows share it, and there is no replay protection (ADR 0010).
- `v5.0.0-15.2.5` asks for extra protection around dangerous functionality,
  such as sandboxing or isolation. Loading by exact name with bundled names
  first narrows which code runs; it does not isolate a plugin once loaded.
  The map claims the narrowing only.
- `v5.0.0-11.5.1` says UUIDs do not meet its bar for values meant to be
  non-guessable. `lease_token` (`gen_random_uuid()`) is a fencing token
  against a previous owner, not a secret: a role that can `UPDATE` the table
  can rewrite the row anyway. The requirement is not claimed.
- The least-privilege control (A01:2025, CWE-284, `v5.0.0-13.2.2`) and the
  test code kept out of the production wheel (A03:2025, `v5.0.0-15.2.3`)
  belong to the two records not decided here.

### Ids checked

| Id | Source | What it asks (paraphrased) |
|---|---|---|
| A01:2025 Broken Access Control, CWE-284 | [A01](https://top10.owasp.org/2025/A01_2025-Broken_Access_Control/) | category and mapped CWE |
| A03:2025 Software Supply Chain Failures, CWE-1357 | [A03](https://top10.owasp.org/2025/A03_2025-Software_Supply_Chain_Failures/) | category and mapped CWE (CWE-829 is not mapped here) |
| A04:2025 Cryptographic Failures, CWE-320, CWE-321, CWE-327 | [A04](https://top10.owasp.org/2025/A04_2025-Cryptographic_Failures/) | category and mapped CWEs |
| A05:2025 Injection, CWE-89 | [A05](https://top10.owasp.org/2025/A05_2025-Injection/) | category and mapped CWE |
| A06:2025 Insecure Design, CWE-799 | [A06](https://top10.owasp.org/2025/A06_2025-Insecure_Design/) | category and mapped CWE |
| A08:2025 Software or Data Integrity Failures, CWE-502, CWE-829 | [A08](https://top10.owasp.org/2025/A08_2025-Software_or_Data_Integrity_Failures/) | category and mapped CWEs |
| A09:2025 Security Logging and Alerting Failures, CWE-117, CWE-532 | [A09](https://top10.owasp.org/2025/A09_2025-Security_Logging_and_Alerting_Failures/) | category and mapped CWEs |
| A10:2025 Mishandling of Exceptional Conditions, CWE-209 | [A10](https://top10.owasp.org/2025/A10_2025-Mishandling_of_Exceptional_Conditions/) | category and mapped CWE |
| v5.0.0-1.1.2 | [V1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x10-V1-Encoding-and-Sanitization.md) | output encoding as the last step before the interpreter, or by the interpreter itself |
| v5.0.0-1.2.4 | [V1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x10-V1-Encoding-and-Sanitization.md) | parameterized database queries |
| v5.0.0-1.5.2 | [V1](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x10-V1-Encoding-and-Sanitization.md) | safe deserialization of untrusted data, for example an allow-list of types |
| v5.0.0-2.2.1 | [V2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x11-V2-Validation-and-Business-Logic.md) | positive validation against an allow-list of values, patterns and ranges |
| v5.0.0-2.2.2 | [V2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x11-V2-Validation-and-Business-Logic.md) | validation enforced at a trusted service layer |
| v5.0.0-2.4.1 | [V2](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x11-V2-Validation-and-Business-Logic.md) | anti-automation against excessive calls and costly resource use |
| v5.0.0-11.2.2 | [V11](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md) | crypto agility: algorithms and keys replaceable, data re-encryptable |
| v5.0.0-11.3.2 | [V11](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md) | only approved ciphers and modes, such as AES with GCM |
| v5.0.0-11.3.3 | [V11](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md) | encrypted data protected against modification, preferably by authenticated encryption |
| v5.0.0-11.3.4 | [V11](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md) | a nonce never reused for the same key and data element |
| v5.0.0-11.5.1 | [V11](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x20-V11-Cryptography.md) | non-guessable values from a CSPRNG with 128 bits of entropy; UUIDs do not qualify (cited only to say it is not claimed) |
| v5.0.0-13.2.2 | [V13](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x22-V13-Configuration.md) | backend communication, data layers included, with least-privilege accounts |
| v5.0.0-15.1.3 | [V15](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | documentation names time-consuming or resource-demanding functionality and its defenses |
| v5.0.0-15.1.5 | [V15](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | documentation highlights dangerous functionality |
| v5.0.0-15.2.2 | [V15](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | defenses against loss of availability from that functionality |
| v5.0.0-15.2.3 | [V15](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | production holds no test code or development functionality |
| v5.0.0-15.2.4 | [V15](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | components from the expected repository, no dependency confusion |
| v5.0.0-15.2.5 | [V15](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x24-V15-Secure-Coding-and-Architecture.md) | extra protection around dangerous functionality or risky components |
| v5.0.0-16.2.5 | [V16](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x25-V16-Security-Logging-and-Error-Handling.md) | sensitive data such as credentials not logged, or masked |
| v5.0.0-16.4.1 | [V16](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x25-V16-Security-Logging-and-Error-Handling.md) | log data encoded against log injection |
| v5.0.0-16.5.1 | [V16](https://github.com/OWASP/ASVS/blob/v5.0.0_release/5.0/en/0x25-V16-Security-Logging-and-Error-Handling.md) | a generic message for unexpected errors, without stack traces, queries, keys or tokens |

Every id above was opened in the source linked next to it on 2026-11-25.
