# 13. Result serializer without pickle

- Status: Proposed
- Date: 2026-11-26

## Context

The result backend stores `serializer.dumpb(result)` in one `result` column,
and `PickleSerializer` is its default. A reader that unpickles a row runs what
the row's writer put there. ADR 0010 names the residual risk: its codec
allow-list does not cover the serializer, so a role with `UPDATE` on the
results table can still make a reader execute code. The spike note leaves
that risk open until a serializer change, and this record is that change.

The broker has the same shape for arguments: it stores the bytes that
taskiq's formatter built in one `message` column, opaque to SQL, to a codec
and to a column-level control.

taskiq's own brokers and result backends do the same, so the design here uses
the extension points taskiq already has: its JSON mode, `TaskiqSerializer`,
the receiver's annotation parsing and `TaskiqMiddleware`. It adds no registry
of its own.

## Decision

Decided: fixed by the maintainer before this record. Proposed: open for
review until this ADR is Accepted. What is still open is under "Open
questions".

### Decided

- In 1.0 results and messages are stored as columns, one per field. The
  broker decodes a message on `kick` with the user's own formatter and encodes
  it again on claim with the worker's formatter.
- The move is expand and contract. The last 0.x release writes both the old
  and the new columns. 1.0 reads only the new ones. Converting old rows and
  dropping the old columns are opt-in CLI commands: no migration step removes
  a column and `run_migrations=True` never does (ADR 0011).
- The last 0.x release lands after the codec interface, so it already has the
  codec parameter, with `legacy` as the only codec (ADR 0010).
- A payload column is `BYTEA` and holds UTF-8 JSON or the encryption envelope
  (ADR 0012).
- Task progress is JSON, never pickle, in a column of the result row, from
  0.x on.
- The columns store task arguments in taskiq's JSON mode, that is
  `TaskiqMessage.model_dump(mode="json")`, which is what `ProxyFormatter`
  already does.
- The bytes of an argument column come from `argument_serializer=` on the
  broker (and `with_argument_serializer`), a `TaskiqSerializer` whose default
  is `JSONSerializer()`. Its output is validated as UTF-8 JSON.
- Types come back through the receiver's annotation parsing, which taskiq
  already runs on every task parameter. Nothing is configured for that.
- `TypedArgumentsMiddleware` is optional. It lives in
  `taskiq_postgresql.middlewares`, works with any taskiq broker, and keeps the
  types that annotations cannot restore (`Any`, unions, untyped parameters,
  non-UTF-8 bytes). It is proposed to taskiq upstream as well.
- The result backend keeps `serializer` in 1.0, with the same contract:
  UTF-8 JSON, as in taskiq-redis.
- A message whose arguments the worker cannot decode is dropped with an
  `ERROR` log that carries no payload.
- A claim that meets a message marked by `TypedArgumentsMiddleware` which this
  worker cannot restore drops it with an `ERROR` log, and does not hand
  wrapper values to the task.
- In 1.0 a result row that holds only the legacy pickle is refused with
  `LegacyResultError`, and the error points to `convert-results`. There is no
  switch that unpickles it.

### Proposed

Results:

- New columns on the results table, all nullable and without a default:

  | Column | Type | Holds |
  |---|---|---|
  | `is_err` | `BOOLEAN` | `TaskiqResult.is_err` |
  | `return_value` | `BYTEA` | JSON of the return value, or the envelope |
  | `execution_time` | `DOUBLE PRECISION` | seconds |
  | `labels` | `JSONB` | `TaskiqResult.labels`, never encrypted |
  | `error_type` | `TEXT` | `<module>.<qualname>` of the exception |
  | `error` | `BYTEA` | JSON of the `ExceptionRepr`, or the envelope |
  | `log` | `TEXT` | deprecated; taskiq's receiver writes `None` |

- The expand is one additive step of the results table, which has only step 1
  on `main`. The step number is not fixed here: it follows whatever steps
  the table has when the expand lands. It adds the columns and rewrites
  nothing.
- While both formats are written, `set_result` stores the legacy bytes through
  the configured serializer and the new columns. If the JSON conversion fails
  it stores only the legacy bytes and logs a `WARNING`. A read uses the new
  columns when `is_err IS NOT NULL`.
- The contract has no DDL of its own once a results step relaxes `result`
  (ADR 0011), which the progress column needs anyway: a row can then exist
  without a result. 1.0 writes and reads only the new columns. A result is
  ready when `is_err IS NOT NULL OR result IS NOT NULL`, and a ready row that
  holds only `result` raises `LegacyResultError`, a subclass of
  `ResultIsMissingError`.
- `serializer` is validated like `argument_serializer`, and `PickleSerializer`
  is refused at construction. The default changes from `PickleSerializer()`
  to `JSONSerializer()`.
- Without `pickle.loads` on the read path, someone who can write to the table
  forges values, not code. The exception of a failed task is rebuilt only into
  a `BaseException` class that is already imported, the check taskiq makes.

Messages:

- Step 4 of the broker's table, after the delivery step 3 of ADR 0009, adds
  `labels_types JSONB`, `args BYTEA` and `kwargs BYTEA`, and writes nothing to
  existing rows. `task_id`, `task_name`
  and `labels` are columns already. `message` stays and is the legacy copy.
- While both formats are written, a producer fills `message` and the columns.
  The claim of ADR 0009 returns the columns too. It delivers `message` when it
  is not `NULL` and rebuilds the message from the columns otherwise, which
  readies the last 0.x for rows written by a 1.0 producer.
- The contract is step 5: `ALTER COLUMN message DROP NOT NULL`, a step of the
  kind ADR 0011 allows. 1.0 writes and reads the columns and keeps `message`
  as a fallback for rows in flight. `DROP COLUMN` is only
  `taskiq postgresql migrate --drop-legacy`, which refuses while a row has
  `args IS NULL`, or `convert-messages` runs first.
- On `kick` the broker calls `formatter.loads` on the message bytes, then
  `model_dump(mode="json")` on `args` and `kwargs`, then
  `argument_serializer.dumpb` on each. On claim it runs `loadb`, the guard,
  and rebuilds a `TaskiqMessage`, then hands `formatter.dumps(...).message`
  to the receiver. A row with `args IS NULL` delivers `message`.
- `argument_serializer` is checked on construction: `loadb(dumpb(sample))`
  must equal the sample, and the bytes must read as UTF-8 JSON. A serializer
  that fails raises `ValueError`. `JSONSerializer` passes; `MSGPackSerializer`
  and `PickleSerializer` do not. `CBORSerializer` is expected to fail (open
  question 6).
- `kick` refuses a value that taskiq's JSON mode cannot represent, with a
  `TypeError` that names the task and the argument position (`args[1]` or
  `kwargs['name']`), never the value. In the last 0.x release it stores only
  `message` and logs a `WARNING` instead, so 1.0 is the release that refuses.
- `TypedArgumentsMiddleware` marks its messages with the taskiq label
  `taskiq_postgresql_arguments`, value `typed-json/1`. The claim guard keys
  off that label: it drops the message when the worker has no middleware, or
  when the middleware does not know a type the message holds.

### Fit with ADR 0010

- The layers, from the top: `TaskiqResult`, the serializer (JSON by field),
  the codec per column (`legacy`, which returns its input, or the cipher of
  ADR 0012), the store. ADR 0010 puts a codec below any serializer, so its
  contract does not change.
- Progress goes through the same codec, with
  `CodecContext(table="results", column="progress", key=task_id)`.
- This record closes the residual risk of ADR 0010 in another way than the
  allow-list: in 1.0 no read path unpickles, `PickleSerializer` is refused,
  and a serializer must produce UTF-8 JSON.
- `legacy` is the identity codec and has nothing to do with pickle; no switch
  or parameter in this record is called `legacy`.
- ADR 0010 says that with `legacy`, a release cut from `main` before 1.0 reads
  what 1.0 writes. With columns that holds only for the last 0.x release and
  later: an earlier one finds a message with no `message` and a result with no
  `result`. ADR 0010 should say so once this record is accepted.
- ADR 0009 reserves step 3 of the broker's table for delivery. This record
  proposes the message columns as step 4 and their contract as step 5, so
  the numbers follow the order in which the code lands.
- Any other codec is turned on only after every reader runs 1.0 (ADR 0010),
  which is after the contract.

## Consequences

- **BREAKING** in 1.0, each with a CHANGELOG entry: the result backend's
  default serializer is `JSONSerializer()`, a pickle-only row raises
  `LegacyResultError`, `PickleSerializer` is refused, and the broker refuses
  an argument that JSON mode cannot represent.
- Pickled results that exist at the upgrade must go through
  `convert-results` before 1.0 can read them. That command reads the legacy
  `result` column with the configured legacy serializer, so it unpickles what
  the table holds. It runs on request, by someone with database access, and
  the upgrade guide says to run it only on a table whose writers are trusted.
- Dropping the legacy columns is a destructive, opt-in action and never a
  step, in line with ADR 0011.
- A custom formatter whose `loads` returns values outside JSON, such as a
  pickle formatter that keeps a `tuple` or a `datetime` for an `Any`
  parameter, gets them flattened as the default formatter does. The
  annotation of the parameter restores the type; the middleware restores the
  rest. The last 0.x release warns when a value changes type this way and the
  upgrade guide says so.
- Encryption or compression that a user puts inside a formatter or
  serializer stops protecting the stored row, because the broker now stores
  decoded fields. ADR 0012 is the replacement.
- Producer and worker may use different transport serializers: the worker
  rebuilds the bytes with its own formatter. Today the worker cannot decode a
  message from a producer with another serializer.
- `labels` stay `JSONB` and in clear. Task arguments become queryable with
  `convert_from(args, 'UTF8')::jsonb`. With the default serializer the cast
  fails only on a string that holds U+0000, which `jsonb` rejects even when
  it is ASCII-escaped.
- A retry made by `Context.requeue` does not run `pre_send`, so its values go
  flattened even though the label stays. The middleware passes unwrapped
  values through, so the effect is inexactness, not an error.
- Middleware order matters: taskiq runs `pre_send` and `pre_execute` in the
  same order, so the documentation says to register `TypedArgumentsMiddleware`
  first, or another middleware that reads arguments sees wrappers.
- One upgrade order, written once in the single 1.0 upgrade guide (PR8 of
  `docs/spikes/2026-11-25-v1-data-layer.md`): workers on the last 0.x release
  before any 1.0 producer. ADR 0009 has its own order (every worker to 1.0
  with `at_most_once`, then `at_least_once`), and the guide holds both orders
  in one place.
- The payload is stored twice per row (`message` and the columns) from the
  last 0.x release until a legacy column is dropped, and the formatter runs
  one more time on `kick` and on claim. Neither cost is measured yet.

## Open questions

1. **A dropped message under `at_least_once`.** The decided rule is that a
   message the worker cannot decode is dropped with an `ERROR` log. ADR 0009's
   claim leases a row and does not delete it. What does the drop do to the row?
   - (a) Delete it.
   - (b) Set `dead_at`, so it sits with the dead letters and can be inspected.

   The same question covers a payload that ADR 0012's codec fails to decrypt.
2. **A worker older than the last 0.x reading a 1.0 row.** Such a row has no
   `message`, and the old claim builds a delivery from `message` outside the
   protected part of its code. The effect under `at_least_once` (lease,
   attempts, dead letter) is not yet tested. Until it is, the upgrade guide
   requires every worker to be on the last 0.x release first.
3. **Cost per message.** The extra `formatter.loads`, `model_dump` and
   `formatter.dumps`, three JSON encodes and two decodes. No number exists;
   it is not yet measured, and cannot be until the code exists.
4. **Skipping `model_dump` for taskiq's own formatters.** For
   `ProxyFormatter` with one of the five taskiq serializers, or
   `JSONFormatter`, JSON mode is the identity, so `kick` could skip the pass.
   - (a) Skip it for those exact types.
   - (b) Always run it.

   That the shortcut gives the same bytes is not yet tested (it was only
   read from the code).
5. **Formatter round trip.** The claim relies on
   `formatter.loads(formatter.dumps(formatter.loads(b)))` reproducing the
   message. That holds by construction for `ProxyFormatter` and
   `JSONFormatter`; for a custom formatter it is a taskiq contract with no
   test here, so it is not yet tested.
6. **Which serializers pass the construction check.** `JSONSerializer` passes.
   `ORJSONSerializer` is expected to pass and `CBORSerializer` to fail, from
   their documentation; neither was run. Both are not yet tested, until the
   check runs with `orjson` and `cbor2` installed.
