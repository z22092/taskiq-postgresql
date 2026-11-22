# AGENTS.md

Guide for coding agents (and humans) working on this repository. `CLAUDE.md`
imports this file, so every tool reads the same rules. Contributor-facing
detail lives in [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md); design
lives in [`docs/architecture.md`](docs/architecture.md) and
[`docs/adr/`](docs/adr/).

## What this is

`taskiq-postgresql` is a PostgreSQL integration for
[taskiq](https://github.com/taskiq-python/taskiq), published on PyPI. It ships
three components: a broker (`PostgresqlBroker`), a result backend
(`PostgresqlResultBackend`) and a schedule source
(`PostgresqlSchedulerSource`). Each one runs on any of three drivers, installed
as extras: `asyncpg`, `psqlpy` and `psycopg`.

## Layout

| Path | What it holds |
|---|---|
| `taskiq_postgresql/broker.py` | Broker: INSERT + `NOTIFY` on kick, `LISTEN` + `DELETE ... RETURNING` claim on listen |
| `taskiq_postgresql/result_backend.py` | Result backend: upsert, select, delete of serialized results (pickle by default) |
| `taskiq_postgresql/scheduler_source.py` | Schedule source: schedules stored as JSONB |
| `taskiq_postgresql/abc/query.py` | SQL builders for the DDL and every row query |
| `taskiq_postgresql/abc/driver.py` | `QueryDriver` and `ListenDriver` interfaces |
| `taskiq_postgresql/drivers/` | One module per driver, each implementing both interfaces |
| `taskiq_postgresql/utils/` | Driver lookup by name, DSN helpers, optional-import checks |
| `tests/` | Integration tests against a real PostgreSQL |
| `tests/unit/` | Tests that need no database (SQL text, parameter shaping, wiring) |
| `scripts/check_pins.py` | Checks that pins declared in several places agree |
| `docs/` | Architecture, ADRs; `docs/spikes/` holds dated planning notes |

## Commands

Run `make sync` once, then use the targets. Each wraps the command CI runs.

| Target | Command | CI job |
|---|---|---|
| `make sync` | `uv sync --locked --all-extras --all-groups` | - |
| `make lint` | `uv run --no-sync pre-commit run --all-files --show-diff-on-failure` | `lint` |
| `make typecheck` | `uv run --no-sync ty check` | part of `lint` |
| `make test` | `uv run --no-sync pytest --cov=taskiq_postgresql --cov-report=term-missing` | `test` |
| `make test-lowest` | Python 3.10, `--resolution lowest-direct`, throwaway env, `uv.lock` put back | `test-lowest` |
| `make build` | `uv build` | CD `build` |
| `make audit` | `uv export` with hashes, then `pip-audit --strict --require-hashes` (network) | `pip-audit` |
| `make pins` | `uv run --no-sync python scripts/check_pins.py` | part of `lint` |

Narrower runs while iterating:

```bash
uv run --no-sync pytest tests/unit          # no database, under a second
uv run --no-sync pytest tests/test_broker.py
uv run --no-sync pytest -k psycopg          # one driver: its name is in the test id
```

The coverage gate (`[tool.coverage.report] fail_under` in `pyproject.toml`)
applies whenever pytest runs with `--cov`, so use `--cov` only on the full
suite. Raise `fail_under` when coverage rises; never lower it to pass.

## PostgreSQL for the tests

- By default the `postgresql_dsn` fixture in `tests/conftest.py` starts one
  throwaway container per session with testcontainers, from an image pinned by
  digest. Nothing else is needed but a container engine.
- `POSTGRESQL_URL` points the suite at a server you already have instead. The
  DSN comes from your environment, never from the repository.
- `TEST_POSTGRES_MAJOR` picks the PostgreSQL major the container runs (the
  supported majors are the keys of the image map in `tests/conftest.py`;
  unset or empty means the newest). `POSTGRESQL_URL` wins over it.
- Docker works as is. With rootless Podman, export these in your shell (they
  are machine specific; never commit them):

  ```bash
  export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
  export TESTCONTAINERS_RYUK_DISABLED=true
  ```

- `docker-compose.yml` is for manual work only; the tests never use it.

## Conventions

- **Driver parity.** A change to one driver lands in all three. Tests take the
  `driver` fixture (or the `broker`, `result_backend` and `scheduler_source`
  fixtures built on it) so they run once per driver.
- **SQL.** DDL and row queries come from the builders in `abc/query.py`, and
  their values travel as bind parameters (`$1`, `$2`, ...), never formatted
  into the SQL text. The exceptions are `LISTEN` (channel name) and `NOTIFY`,
  whose payload is the integer id the broker's own INSERT returned. Table,
  column and channel names come from configuration and are emitted unquoted.
- **Schema.** Tables are created with `CREATE TABLE IF NOT EXISTS` when
  `run_migrations=True`, so existing deployments keep their tables. Changing a
  column is a schema change for them; `tests/unit/test_components.py` pins the
  current DDL.
- **Types.** `ty` checks `taskiq_postgresql/` at the Python 3.10 floor and must
  stay clean. Fix the code; a `# ty: ignore[<rule>]` is allowed only with a
  comment saying why, and never as a blanket ignore.
- **Comments** explain why, not what. Docstrings follow the Google style.
  Ruff is the only formatter and linter.
- **Dependencies.** Runtime dependencies are closed ranges `>=floor,<next-minor`
  (next major for `pydantic` and `typing-extensions`). Tooling, the build
  backend and every dependency group are exact `==` pins.
  `scripts/check_pins.py` enforces both. `[tool.uv] exclude-newer = "14 days"`
  keeps releases younger than 14 days out of the lock; Dependabot waits the
  same 14 days.
- **Lock.** Change `uv.lock` only through `uv lock` or `uv add`, then check it
  with `uv lock --check`.
- **Pins outside Python.** GitHub Actions are pinned by full commit SHA with a
  version comment; container images by digest; pre-commit hooks by SHA with a
  `# frozen: vX` comment.
- **Commits** follow Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`,
  `chore:`, `ci:`, `build:`), subject under 72 characters, body says why.
- **CHANGELOG.md** (Keep a Changelog) gets an entry for every user-visible
  change.
- **Tests prove changes.** A behaviour change comes with a test that fails
  without it and passes with it.

## Gates a pull request must pass

- `CI / lint`: every pre-commit hook on every file: ruff check and format,
  `ty`, `check_pins.py`, gitleaks (staged changes and full history),
  detect-secrets against `.secrets.baseline`, typos, zizmor (workflow audit)
  and the standard file checks.
- `CI / test (py3.X)`: the full suite with the coverage gate, once per
  supported Python version, all extras installed.
- `CI / test (py3.10, lowest-direct)`: the suite on the lowest version of
  every direct dependency.
- `CD / build` and `CD / smoke`: the wheel is built, installed from hashed
  locked requirements and imported.
- `Supply chain / dependency-review` and `Supply chain / pip-audit`.

`make lint`, `make test`, `make test-lowest`, `make build` and `make audit`
reproduce these locally.

## Releases

A release is a GitHub Release with tag `vX.Y.Z` on `main`, created by the
maintainer:

1. A pull request moves the CHANGELOG entries under the new version. Keep
   `version` in `pyproject.toml` in step with it; the build overwrites it
   from the tag either way.
2. Publishing the release runs `cd.yml`: the version is taken from the tag, the
   sdist and wheel are built, attested and smoke-tested.
3. The `publish-pypi` job waits for approval on the `pypi` environment, then
   publishes through PyPI trusted publishing (no stored token).

`workflow_dispatch` on `cd.yml` publishes to TestPyPI only. The
`prepare-release` skill in `.claude/skills/` walks through the checklist.

## Never

- Publish anything: no `uv publish`, `twine`, GitHub Release, `cd.yml` dispatch
  or tag push. Releases are the maintainer's.
- Push to `main` or force-push a shared branch.
- Edit `uv.lock` by hand, or pin a release younger than 14 days.
- Weaken a test to get green: delete or loosen an assertion, add `skip` or
  `xfail`, or lower `fail_under`.
- Commit credentials or a DSN with a password; tests build DSNs at run time
  from fixtures.
- Unpin an action, image or hook, or widen a workflow's `permissions`.
