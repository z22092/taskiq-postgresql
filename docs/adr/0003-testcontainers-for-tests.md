# 3. Tests run against a real PostgreSQL started by testcontainers

- Status: Accepted (recorded after the fact)
- Date: 2026-11-22

## Context

The package's behaviour lives in PostgreSQL: `LISTEN`/`NOTIFY`, the
`DELETE ... RETURNING` claim, upserts, and three drivers that each talk to
the server differently. Mocking the drivers would test the mocks. Depending
on a database the developer starts by hand made the suite fail for setup
reasons (wrong URL, wrong credentials, a stale volume) rather than code
reasons.

## Decision

- The session-scoped `postgresql_dsn` fixture in `tests/conftest.py` starts a
  throwaway PostgreSQL container through testcontainers, from an image pinned
  by tag and digest, and builds the DSN from the credentials the container
  holds.
- `POSTGRESQL_URL`, when set, points the suite at an existing server instead.
- `docker-compose.yml` stays for manual work; the tests never use it.
- Tests that need no server (SQL text, parameter shaping, wiring) live in
  `tests/unit/` and run in well under a second.

## Consequences

- Running the integration tests needs Docker or Podman; with rootless Podman,
  `DOCKER_HOST` and `TESTCONTAINERS_RYUK_DISABLED` are set in the shell.
- The first run pulls the image.
- CI disables Ryuk, the testcontainers reaper, because the runner is thrown
  away after the job.
- The image pin is repeated in `docker-compose.yml`; `scripts/check_pins.py`
  keeps the copies equal.
