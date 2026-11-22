---
name: run-tests
description: Run the taskiq-postgresql test suite with Docker or Podman, pick the PostgreSQL major or an existing server, and read the result. Use before claiming a change works, when a test fails for setup reasons, or when asked to run the tests for one driver or one PostgreSQL version.
---

# Run the tests

The integration tests need a real PostgreSQL; the fixture in
`tests/conftest.py` starts one with testcontainers. Unit tests in
`tests/unit/` need nothing.

## 1. Environment

```bash
make sync    # once: project, all three driver extras, all dependency groups
```

Container engine:

- Docker: nothing to set.
- Rootless Podman: export these in the shell, never in a committed file.

  ```bash
  export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
  export TESTCONTAINERS_RYUK_DISABLED=true
  ```

If a test fails with a connection error before any assertion, check the
engine first (`docker info` or `podman info`).

## 2. Pick what to run

| Goal | Command |
|---|---|
| Everything, with the coverage gate, as CI | `make test` |
| Fast loop, no database | `uv run --no-sync pytest tests/unit` |
| One file | `uv run --no-sync pytest tests/test_broker.py` |
| One driver (the name is in the test id) | `uv run --no-sync pytest -k asyncpg` |
| Lowest direct dependencies on Python 3.10, as CI | `make test-lowest` |

The coverage gate (`fail_under` in `pyproject.toml`) applies to any run with
`--cov`, so add `--cov` only to the full suite.

## 3. Pick the PostgreSQL

- Default: the newest major in the image map of `tests/conftest.py`.
- Another supported major: `TEST_POSTGRES_MAJOR=<major> make test`. An
  unsupported value stops pytest with a usage error listing the supported
  majors.
- A server you already run: `POSTGRESQL_URL=<dsn> make test`. It wins over
  `TEST_POSTGRES_MAJOR`. Take the DSN from your environment; never write one
  with a password into a file. With a DSN that carries no password, the
  `wrong_password` case of the connection-failure tests skips itself.

## 4. Read the result

- Every integration test runs once per installed driver; a failure on only
  one driver id points at that driver module in `taskiq_postgresql/drivers/`.
- A real failure is fixed in the code. Never delete or loosen an assertion,
  add `skip` or `xfail`, or lower `fail_under` to get a green run.
- When reporting, quote the pytest summary line and the exit code.
