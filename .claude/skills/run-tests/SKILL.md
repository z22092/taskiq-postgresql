---
name: run-tests
description: Run the taskiq-postgresql test suite with Docker or Podman, pick the PostgreSQL major or an existing server, and read the result. Use before claiming a change works, when a test fails for setup reasons, or when asked to run the tests for one driver or one PostgreSQL version.
---

# Run the tests

The commands, the test layout and the PostgreSQL options are described once,
in `AGENTS.md`; this skill is the order to apply them in.

## 1. Environment

1. `make sync`, once.
2. Container engine: Docker needs nothing. For rootless Podman, export the
   two variables listed under "PostgreSQL for the tests" in `AGENTS.md`, in
   the shell only.
3. If a test fails with a connection error before any assertion, check the
   engine first (`docker info` or `podman info`).

## 2. Pick what to run

- As CI does: a target from `make help` (`make test`, `make test-lowest`).
- While iterating: a narrower pytest run from the "Commands" section of
  `AGENTS.md` (unit tests only, one file, one driver). Leave `--cov` off
  those runs: the coverage gate applies to any run with `--cov`.

## 3. Pick the PostgreSQL

Follow "PostgreSQL for the tests" in `AGENTS.md`: the default container, a
major chosen with `TEST_POSTGRES_MAJOR=<major>`, or your own server through
`POSTGRESQL_URL` (taken from your environment, never written into a file).
With a DSN that carries no password, the `wrong_password` case of the
connection-failure tests skips itself.

## 4. Read the result

- Every integration test runs once per installed driver; a failure on only
  one driver id points at that driver module in `taskiq_postgresql/drivers/`.
- A real failure is fixed in the code. Never delete or loosen an assertion,
  add `skip` or `xfail`, or lower `fail_under` to get a green run.
- When reporting, quote the pytest summary line and the exit code.
