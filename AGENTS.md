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
| `tests/conftest.py`, `tests/helpers.py` | Fixtures (`postgresql_dsn`, drivers, teardown) and shared helpers: teardown, scratch databases, DSN and port utilities |
| `tests/test_readme_examples.py` | Runs the README's examples and checks its tables |
| `tests/unit/` | Tests that need no database (SQL text, parameter shaping, wiring) |
| `scripts/check_pins.py` | Checks that pins declared in several places agree |
| `docs/` | Architecture, ADRs; `docs/spikes/` holds dated planning notes |

## Commands

Run `make sync` once, then use the targets. Each wraps the command its CI job
runs; `make help` lists them, and the `Makefile` holds the exact commands.

| Target | Purpose | CI job |
|---|---|---|
| `make sync` | Install the project, the three driver extras and every dependency group from `uv.lock` | - |
| `make lint` | Run every pre-commit hook on every file | `lint` |
| `make typecheck` | Type-check `taskiq_postgresql/` with `ty` | part of `lint` |
| `make test` | Run the full suite with the coverage gate | `test` |
| `make test-lowest` | Run the suite on Python 3.10 with the lowest version of every direct dependency, in a throwaway environment; `uv.lock` is put back however the run ends | `test-lowest` |
| `make build` | Build the sdist and wheel into `dist/` | CD `build` |
| `make audit` | Audit every locked dependency with `pip-audit`, hashes required (needs network) | `pip-audit` |
| `make pins` | Check that pins declared in more than one place agree | part of `lint` |

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
- `TEST_POSTGRES_MAJOR` picks the PostgreSQL major the container runs. The
  supported majors are the keys of the image map in `tests/conftest.py`;
  unset or empty means the newest of them, and any other value stops pytest
  before the first test with one usage error that lists them (exit code 4).
  `POSTGRESQL_URL` wins over it.
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
  column names come from configuration and are emitted unquoted. Channel
  names are unquoted in `NOTIFY` and in psycopg's and psqlpy's `LISTEN`, but
  asyncpg sends `LISTEN "<channel>"` quoted, so a channel name with upper case
  letters is lost on asyncpg and psqlpy: keep `channel_name` lower case.
- **Schema.** Tables are created with `CREATE TABLE IF NOT EXISTS` when
  `run_migrations=True`, so existing deployments keep their tables. Changing a
  column is a schema change for them; `tests/unit/test_components.py` pins the
  current DDL. The migration's advisory lock key (`MigrationLockQuery` in
  `abc/query.py`) is a contract between releases;
  `tests/test_migrations.py::test_migration_lock_key_is_pinned` pins it.
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
- **Commits** follow Conventional Commits: `feat`, `fix`, `docs`, `test`,
  `refactor`, `style`, `build`, `ci` or `chore`, an optional scope
  (`fix(psycopg): ...`) and `!` for a breaking change (`feat!: ...`); subject
  under 72 characters, body says why.
- **CHANGELOG.md** (Keep a Changelog) gets an entry for every user-visible
  change; the rules are under "Changelog" in
  [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md).
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
maintainer. The version comes from the tag: there is no version bump commit,
and neither `version` in `pyproject.toml` nor `uv.lock` changes for a release.

1. A pull request moves the `[Unreleased]` entries of `CHANGELOG.md` under
   the new version and updates the compare links at the bottom of the file.
2. Publishing the release runs `cd.yml`: it sets the version from the tag,
   builds and attests the sdist and wheel, and smoke-tests the wheel.
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
