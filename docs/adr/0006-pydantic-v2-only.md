# 6. Support pydantic v2 only

- Status: Accepted (0.5.0)
- Date: 2026-11-22

## Context

The package serialized schedules and JSONB values through the pydantic
helpers in `taskiq.compat` (`IS_PYDANTIC2`, `model_dump_json`,
`model_validate_json`), which switched between the pydantic v1 and v2 APIs.
taskiq 0.13 removed those helpers and itself requires `pydantic>=2.5`, so
with taskiq 0.13 installed, importing the scheduler source or the asyncpg and
psycopg drivers failed.

## Decision

- The package calls the pydantic v2 API directly:
  `ScheduledTask.model_validate_json` and `model_dump_json` in
  `scheduler_source.py`, and `pydantic_core.to_json` for JSONB values in
  `drivers/_asyncpg.py` and `drivers/_psycopg.py`. These are the calls the
  removed helpers made on v2, so the stored JSON does not change.
- `pydantic>=2.5,<3` is a declared runtime dependency, matching taskiq 0.13's
  own floor, and the taskiq range is `>=0.11.7,<0.14`.

## Consequences

- Breaking change in 0.5.0: applications still on pydantic v1 stay on 0.4.x.
- Schedules written by 0.4.x under pydantic v2 read back unchanged.
- The package no longer depends on taskiq's internal compatibility module.
