# 12. Payload encryption: an AES-256-GCM codec

- Status: Proposed
- Date: 2026-11-26

## Context

Payload bytes sit in the database as the code produced them: the broker's
message body, the result backend's serialized result, the scheduler's JSON.
Anyone who can read the database, a backup or a replica reads every argument
and every result.

ADR 0010 fixes where encryption plugs in and what an encrypting codec
receives (`CodecContext`: the logical table, the column and the row key). It
leaves the envelope, the cipher, the additional authenticated data (AAD),
nonce generation and key rotation to this record. A cipher inside a
`TaskiqSerializer` covers too little, for the reasons ADR 0010 gives: it never
sees the `labels` column, it is bypassed by `JSONFormatter` or any formatter
that ignores the serializer, and it gets no table, column or row key to
authenticate.

This record covers the data layer of ADR 0013: one payload column per field,
holding UTF-8 JSON or an encryption envelope.

## Decision

Decided: fixed by the maintainer before this record. Proposed: open for
review until this ADR is Accepted. What is still open is under "Open
questions".

### Decided

- The cipher is AES-256-GCM, with a key id and AAD.
- It comes from an extra named `cryptography`, with a closed range per ADR
  0002.
- A payload column is `BYTEA` and holds UTF-8 JSON or the encryption
  envelope, which carries a marker and the key id.
- Encryption is a codec (ADR 0010), passed through the codec parameter that
  the codec interface adds. It exists in 1.0 and later only: there is no
  `encryption=` parameter in 0.x.
- The last 0.x release, the one that writes both the old and the new columns
  (ADR 0013), lands after the codec interface. No 0.x release writes an
  envelope, so the first release that can is 1.0.
- Encrypted schedules get a new `BYTEA` column, added by a step of the
  scheduler's own table, and that column goes through the codec. ADR 0009
  reserves step 3 of the broker's table; the scheduler's step belongs to the
  scheduler's table, so no step number clashes.

### Proposed

- The columns an encrypting codec covers are `args` and `kwargs` of messages,
  `return_value`, `error` and `progress` of results, and the payload column of
  schedules.
- The envelope is `b"TQE1" || len(kid):u8 || kid || nonce || ciphertext ||
  tag`: a four-byte marker, the length of the key id as one byte, the key id
  (ASCII, `[A-Za-z0-9._-]`, 1 to 64 characters), a 12-byte nonce from
  `os.urandom` and the ciphertext with its 16-byte tag. Keys are 32 bytes,
  used through `cryptography.hazmat.primitives.ciphers.aead.AESGCM`.
- The nonce is random and 96 bits wide. NIST SP 800-38D limits a key to 2^32
  encryptions with random nonces. Nothing counts encryptions across worker,
  producer and scheduler processes, so no process can enforce that limit; the
  documentation tells the operator to rotate keys well before it, and that is
  all this baseline offers. Open question 3 proposes a way to remove the
  limit.
- The AAD is built from these fields, in this order, and no other:
  1. the marker `TQE1`;
  2. the key id;
  3. the logical table (`messages`, `results` or `schedules`);
  4. the column;
  5. the row key (`task_id` or the schedule id);
  6. the physical table name, as configured;
  7. the `task_name` of the row, empty for the results table, which has none.

  Fields 1 to 5 come from `CodecContext` as ADR 0010 defines it. Fields 6 and
  7 are not in it, so they need a new `CodecContext` field each. That is an
  amendment to ADR 0010, to be agreed with the 1.0 data-layer series (the
  spike note) and landed with the codec interface, before any envelope is
  written. Binding `task_name` stops someone with `UPDATE` from renaming it so
  that another function runs the encrypted arguments. Binding the physical
  table name stops a row from being swapped between two tables that share a
  key, at the cost that renaming a table needs `reencrypt`.
- The AAD encoding is one version byte, then each field as a 4-byte
  big-endian length followed by its UTF-8 bytes, in the order above. Plain
  concatenation would be ambiguous, because `column` and `key` are
  variable-length strings that could shift a boundary. The version byte lets a
  later field list change without reusing a layout.
- `Encryption(keys, primary)` builds the codec. `keys` is a mapping from key
  id to 32 bytes, or a callable that returns one, resolved in `startup()` the
  way ADR 0005 resolves a DSN callable. `repr` masks the keys. There is no
  default key and none is generated.
- Rotation: `primary` encrypts and every other key only decrypts. Messages
  drain on their own. `result_ttl` is off by default, so results do not age
  out unless the operator sets it. `taskiq postgresql reencrypt`, a CLI
  command, rewrites results and schedules under `primary`. Retiring a key
  needs `reencrypt`, or `result_ttl`, on every table that holds a payload
  sealed with it.
- A `KeyProvider` protocol can unwrap data keys at startup, so a key
  management service holds the master key. Per-row encryption stays local.
- `labels` and `labels_types` are never encrypted: routing and `delay` read
  them. The documentation says that a secret does not belong in a label.
- One rule covers every read. `decode` raises. `AESGCM.decrypt` raises
  `InvalidTag` and returns no plaintext, and a key id that is not configured
  takes the same path. There is never a plaintext fallback, except under open
  question 1(b). What the caller does with the error:
  - A result read raises a typed error of the package.
  - A message is dropped as one the worker cannot decode (ADR 0013).
  - A schedule is skipped, with a log line.

  Every log line carries the component, the row id, the key id and the
  reason, never the bytes.

### Fit with ADR 0010

- The envelope is a `Codec`. It receives bytes already serialized, below any
  formatter or serializer; for a message that is the output of
  `argument_serializer` (ADR 0013).
- `table`, `column` and `key` of `CodecContext` are AAD fields 3 to 5. `key`
  is the `task_id` for messages and the schedule id for schedules.
- `key` binds a message payload to its `task_id`, not to its row: a retry
  kicks the same `task_id`, so the payload can move between rows of one task
  and there is no replay protection. In the results table `task_id` is the
  primary key, so it binds the row. This is the limit ADR 0010 already states.
- `labels` never pass through a codec (ADR 0010), which agrees with keeping
  them in clear.
- The allow-list holds: a component decodes only with the codecs its own
  configuration names. A component that is not configured with the codec does
  not decrypt a row because it starts with the marker.
- The codec is turned on only after every reader of the table runs 1.0, which
  is where the two decided items on the release line already put it.
- The spike note defers `v5.0.0-11.3.2`, `v5.0.0-11.3.3`, `v5.0.0-11.3.4` and
  `v5.0.0-11.2.2` of the OWASP ASVS to this record. The pull request that
  implements it adds their rows to the map, each with its proof.

## Consequences

- The marker, the envelope layout and the AAD fields become a public format:
  a later layout needs a new marker or a new version byte.
- Someone who can read the tables sees ciphertext plus what stays in clear:
  `task_name`, `labels`, `labels_types`, `channel`, `error_type`, timestamps
  and sizes.
- Without a key, someone who can write to the tables cannot forge a payload or
  move one to another column or another `task_id`. That person can still
  re-insert an old row (replay), delete rows and edit the columns that stay in
  clear. Authenticated encryption does not cover those, and neither does this
  record.
- Until `CodecContext` carries `task_name` and the physical table name, two
  things stay possible: rebinding a `task_name`, and swapping rows between two
  tables that share a key. The documented mitigation is one key per table.
- `NOTIFY` carries only the row id, and the package does not log payloads.
  The claim rebuilds the message bytes in the worker's memory; if the
  worker's formatter cannot read them, taskiq logs them at `WARNING`, the
  same exposure as today, and taskiq's `DEBUG` prints the message. The
  documentation says to keep `taskiq.receiver` at `INFO` or above.
- An encrypted column cannot be queried with SQL such as
  `convert_from(args, 'UTF8')::jsonb`.
- A compromise of the worker process, its host or its keys is out of scope.
- `cryptography` becomes an optional dependency with a closed range per ADR
  0002; its bounds are open question 4, and adding it is a new dependency
  decision.
- A schedule encrypted by this record is a new column, so the scheduler's
  table has a second step. Nothing is removed or renamed (ADR 0011).

## Open questions

1. **`accept_plaintext`.** During the move to encryption, rows written before
   the codec was on are plain. Can the codec read them?
   - (a) Never: a row without the marker is refused, which stops a downgrade.
     ADR 0010 says a component never decodes by "whatever the stored bytes
     suggest", and this option keeps it.
   - (b) An `accept_plaintext` flag on the configured codec, `False` by
     default, with a `WARNING` at startup when it is `True`. The stored bytes
     then decide, but only inside the codec the component names.

   Not yet tested: a custom formatter may produce a body that starts with the
   marker, which would collide on the legacy `message` column under (b).
2. **Name and packaging of the codec.** The registry (ADR 0010) derives its
   table from `pyproject.toml`, so the entry exists even when `cryptography`
   is not installed. The registry name is not chosen here. Which error does a
   user get when the extra is missing, and is the import guarded as in
   `utils/libs_available.py`? A codec that needs keys also cannot be built
   from a bare name; whether the parameter takes an instance, a name or both
   is the signature of the codec interface, which does not exist yet.
3. **Per-message subkeys.** The per-key limit of 2^32 random-nonce
   encryptions cannot be enforced across processes. Derive a subkey for each
   message with HKDF-SHA256 from the key and a random salt carried in the
   envelope, and encrypt with it. That removes the per-key limit and keeps
   random 96-bit nonces as the baseline for the subkey. The envelope grows by
   the salt, whose size is chosen with the layout, and the codec does one
   more derivation per payload, which is not yet measured. Adopt it, or keep
   the baseline and the documented rotation?
4. **Range of `cryptography`.** It is not in `uv.lock`, `pyproject.toml` or
   `scripts/check_pins.py` today. The floor and the ceiling come from
   `uv lock`, `make test-lowest` and `make audit` when the extra is added;
   until then both are not yet tested.
5. **Legacy columns.** The `message` and `result` columns that 0.x wrote stay
   until an opt-in command drops them, and 1.0 reads `message` as a fallback
   for rows in flight. They were written in clear. Does an encrypting codec
   see them?
   - (a) They never pass through a codec other than `legacy`.
   - (b) They pass through the configured codec like the new columns.
6. **The JSONB `schedule` of an encrypted schedule.** What do its `args` and
   `kwargs` hold once the payload lives in the new column? A design that put
   the envelope in `args` made a pre-encryption scheduler fail validation and
   skip the row, instead of running the task with no arguments. Empty values
   would not do that. ADR 0010's rule that the codec is turned on only after
   every reader runs 1.0 makes the case unlikely, not impossible.
