# 9. Delivery modes: at least once by default, at most once on request

- Status: Proposed
- Date: 2026-11-25

## Context

A claim is a `DELETE ... RETURNING` (`ClaimDueQuery`, `ClaimByIdQuery`) and
`ack` does nothing (`_ack` in `broker.py`), so a worker that dies mid-task
loses the message; `max_retry_attempts` is never read. `main` holds the
unreleased `channel` and `not_before` columns; "0.5.x" is a release cut from
`main` before 1.0. A 0.4.x worker only deletes the id a `NOTIFY` names.

taskiq stops a worker differently per version (sources in the spike note).
The design relies on this ordering: in 0.11.7, `broker.shutdown()` runs while
callback tasks are alive and `broker.listen()` is suspended; from 0.11.20 it
runs after `receiver.listen()` returns; in every supported version
`pre_execute` runs before `run_task`, outside any `try`, and an unreadable
message or an unknown task is not acked before 0.13.0.

## Decision

`delivery="at_least_once"` is the default; `delivery="at_most_once"` is opt-in.

- Schema: step 3 of the broker's table adds `attempts INT NOT NULL DEFAULT 0`,
  `lease_token UUID` and `dead_at TIMESTAMPTZ`. The lease lives in
  `not_before`, so claims of `main` and 0.5.x skip a leased row.
- Claim, one autocommitted statement, ordered by the
  `(channel, not_before, id)` index:

  ```sql
  UPDATE <t> SET not_before = now() + make_interval(secs => $2),
         lease_token = gen_random_uuid(), attempts = attempts + 1
   WHERE id IN (SELECT id FROM <t>
                 WHERE channel = $1 AND not_before <= now()
                   AND attempts < <max> AND dead_at IS NULL
                 ORDER BY not_before, id LIMIT <batch> FOR UPDATE SKIP LOCKED)
  RETURNING id, message, labels, attempts, lease_token
  ```

  `<max>` and `<batch>` are validated integers written as literals, since a
  generic plan does not see a bound `LIMIT`: a new exception to the SQL rule
  of `AGENTS.md`, which changes, with the SQL section of
  `docs/architecture.md`, in the PR that adds this claim.
- Claim by id: a `NOTIFY` claims its own id, as today, because rows from
  0.4.x producers have `channel IS NULL` and no batch claim by channel sees
  them. The same `SET` as the batch claim, with the lease in its own
  parameter (`make_interval(secs => $4)`), and `WHERE id = $1 AND (channel =
  $2 OR (channel IS NULL AND task_name = ANY($3))) AND not_before <= now()
  AND attempts < <max> AND dead_at IS NULL`, setting `channel = $2` as
  adoption does. The task-name filter applies only to channel-less rows and
  is dropped with `adopt_legacy=True`; the periodic sweep runs the batch claim.
- Ack: `DELETE ... WHERE id = $1 AND lease_token = $2 AND dead_at IS NULL`;
  zero rows means the lease was lost, which is logged.
- Heartbeat: one `UPDATE ... FROM unnest($1, $2)` per interval for all held
  rows. It stops for a row at its first ack attempt and at a hard cap, the
  `timeout` label plus a slack, else a finite global `max_task_seconds`. The
  cap covers the paths with no ack: an unreadable or unknown message on
  0.11.7 and 0.11.20, a raising `pre_execute`, and on 0.13.0 an invalid
  `ack_type` label or `MANUAL` without `Context.ack`. A buffered row whose
  lease passed without a confirmed heartbeat is dropped, never yielded.
- Limits (`batch`, `lease`, a heartbeat interval below `lease / 2`,
  `max_attempts`, `max_task_seconds`) are an `int`, not a `bool`, within
  ceilings; construction requires `query_timeout < lease - heartbeat interval`.
- Release, in `broker.shutdown()`: every held row with no recorded start,
  buffered or yielded, is first marked released in the process; then its
  heartbeat is cancelled and awaited; then one `UPDATE ... FROM unnest($1, $2)`
  sets `not_before = now()`, `lease_token = NULL` and `attempts = attempts - 1`
  where `id` and `lease_token` match and `dead_at IS NULL`, like heartbeat and
  ack. Every such row gives its attempt back: the broker's middleware records
  a start in `pre_execute`, by `task_id` (retries keep it), and raises for a
  released row, so none of them ran, including a callback that 0.11.7 starts
  during `shutdown()`.
- Dead letter, same table: `FOR UPDATE SKIP LOCKED` over `channel = $1 AND
  attempts >= <max> AND not_before <= now() AND dead_at IS NULL` sets
  `dead_at = now()`, `not_before = 'infinity'`, `lease_token = NULL`. Ack,
  heartbeat and release require `dead_at IS NULL`, so a late one matches
  nothing. `max_retry_attempts` becomes `max_attempts`.
- `channel IS NULL` rows (from 0.4.x) are adopted, by id as above or in the
  sweep with `SET channel = $1`, only when `task_name = ANY($n)` of the tasks
  this worker registered, or all of them with `adopt_legacy=True` for a
  single-channel table.
- `NOTIFY` keeps the integer id as payload during 1.x, for 0.4.x workers;
  `notify=False` turns it off.
- psqlpy declares `abort=False` (ADR 0007): an abandoned query keeps running
  and its connection is handed out again
  ([psqlpy#180](https://github.com/psqlpy-python/psqlpy/issues/180)). The core
  counts deadline expiries, rebuilds the pool at the first, and runs heartbeat
  and ack on a pool of their own; a failed rebuild fails the health check.
- At most once: batch of 1, `DELETE ... RETURNING` in a transaction that
  commits only with the row in hand; giving up before `COMMIT` aborts the
  connection, so the server rolls back. psqlpy has no such deadline.

## Consequences

- A task can run twice when its lease is lost; the ack logs it.
  `max_attempts` counts claims, not failures: taskiq 0.13.0 acks after an
  exception, a timeout or a failed result write.
- A claim the server commits after the worker gave up spends an attempt with
  nobody holding the lease, and the row returns when the lease expires, so a
  slow database can dead-letter a message that never ran. The claim is not
  wrapped in a transaction aborted before `COMMIT`: psqlpy cannot abort, and
  a lost `COMMIT` reply leaves the same doubt. An operator requeues the row.
- The heartbeat updates an indexed column (no HOT update); index growth and
  throughput are measured before a default is promised.
- Upgrade: every worker to 1.0 with `at_most_once`, then `at_least_once`. A
  0.4.x worker deletes a notified id whatever its lease, so it cannot share a
  table with at least once; 0.5.x can.
- **BREAKING:** the default delivery, and `max_retry_attempts`.
- The SIGTERM test with `max_prefetch > 0` runs on taskiq 0.11.7 and 0.13.0.
