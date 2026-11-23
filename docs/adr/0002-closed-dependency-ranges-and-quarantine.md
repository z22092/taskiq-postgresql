# 2. Closed dependency ranges and a 14-day quarantine

- Status: Accepted (recorded after the fact)
- Date: 2026-11-22

## Context

This is a library: applications install it next to their own pins. An exact
runtime pin (`==`) would make their resolver fail on the first conflict. An
open range (`>=`) lets in upstream releases nobody tested; taskiq 0.13
removed helpers that earlier versions of this package imported, so an open
range broke it at import time. Separately, a release published minutes ago
can be broken or malicious, and resolving to it immediately gives no time for
that to be noticed.

## Decision

- Runtime dependencies (`[project] dependencies` and the driver extras) are
  closed ranges `>=floor,<ceiling`. The ceiling is the next minor of the
  locked version, or the next major for packages that keep their API within
  a major (`pydantic`, `typing-extensions`; `NEXT_MAJOR` in
  `scripts/check_pins.py`).
- Tooling is exact: every dependency group, the build backend (`hatchling`,
  with sha256 hashes for it and its dependencies in
  `[tool.uv] build-constraint-dependencies`), pre-commit hooks, GitHub
  Actions (commit SHA) and container images (digest).
- `[tool.uv] exclude-newer = "14 days"` keeps releases younger than 14 days
  out of `uv.lock`, and Dependabot's `cooldown` waits the same 14 days.
- `scripts/check_pins.py`, a pre-commit hook, fails when a range or a pin has
  the wrong shape.
- CI tests the floor (`test (py3.10, lowest-direct)`) and the locked
  versions.

## Consequences

- Users get a new upstream minor only after a pull request raises the ceiling
  and the suite passes on it.
- Every upstream minor needs that explicit bump, which is routine work.
- A fix released upstream reaches the lock 14 days later at the earliest,
  unless someone lowers the quarantine for that package on purpose.
