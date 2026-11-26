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
| `taskiq_postgresql/broker.py` | Broker: INSERT, plus `NOTIFY` when due at once, on kick; `LISTEN` and a periodic sweep, each claiming with `DELETE ... RETURNING`, on listen; reopens a lost `LISTEN` connection |
| `taskiq_postgresql/result_backend.py` | Result backend: upsert, select, delete of serialized results (pickle by default) |
| `taskiq_postgresql/scheduler_source.py` | Schedule source: schedules stored as JSONB |
| `taskiq_postgresql/core/sql/` | The SQL core. `identifier.py`: `Identifier` (the rule for a table or channel name, with its fold and its warning), `derived_names` and `check_table_names` (the derived names of a table, and the refusal of a clash) and `RESERVED_WORDS`; `statement.py`: `Statement` and `Script` (SQL text with the `ParamType` of each bind and result column); `catalog/` (a package: `base.py`, `columns.py`, `ddl.py`, `rows.py`, `locks.py`, `introspection.py`): every DDL statement, row query, the `pg_notify` statement, the `LISTEN` script and the catalog read, as builders over checked names; `check_table_objects` is the derived-name check callers use (it reads a table's declaration into `check_table_names`) |
| `taskiq_postgresql/abc/query.py` | Re-exports the catalog's builders, so third-party drivers that import it keep working |
| `taskiq_postgresql/migrations.py` | The tables described as columns and indexes (`TableSchema`) and as the ordered statements `on_startup` runs (`MigrationStep`, whose `objects` name what each step creates), built from a `TableDeclaration` by `declared_schema` and the catalog's builders; `render_sql`, `migration_statements`, `step_statements` (steps inside a tool's own transaction, their lock waits bounded by `scoped_lock_timeout`) and the migration formats registered under the `taskiq_postgresql.migration_formats` entry-point group, with the pipeline the file formats share (`render_steps` lays out each step as a `RenderedStep` after `check_comment`, `check_plain_identifier` and the option check) and `collect_schemas` and `parse_target` for targets; the comparison engine: `catalog_query` (one read-only catalog query, which `read_catalog` runs and reads), `CatalogState`, `plan_migration` (a `MigrationPlan` of `PendingStep` and `SchemaProblem`) |
| `taskiq_postgresql/contrib/sqlalchemy.py`, `contrib/sqlmodel.py` | Extras `sqlalchemy` and `sqlmodel`: `add_taskiq_tables` and `taskiq_metadata` build SQLAlchemy `Table` objects from `TableSchema` and record the descriptions under `MetaData.info[METADATA_INFO_KEY]` |
| `taskiq_postgresql/contrib/alembic.py` | Extra `alembic`: the autogenerate comparator `compare_taskiq_steps` (registered on import) and its `TaskiqStepOp`/`TaskiqStepNoopOp`, `upgrade_taskiq_tables` for hand-written revisions, `exclude_taskiq_tables` and the `alembic` migration format (`alembic_format`); each step bounds its own lock waits (`LOCK_TIMEOUT_OPTION`) |
| `taskiq_postgresql/contrib/django/` | Extra `django`: the app `taskiq_postgresql.contrib.django` (label `taskiq_postgresql`, no models) whose `migrations/` hold one `RunTaskiqStep` per migration step (`operations.py`), the settings `TASKIQ_POSTGRESQL_TARGETS` and `TASKIQ_POSTGRESQL_LOCK_TIMEOUT` and their system check (`conf.py`); a new step needs a new migration file |
| `taskiq_postgresql/contrib/django_format.py` | The `django` migration format (`django_format`), which imports no Django, so `generate --format django` needs no extra |
| `taskiq_postgresql/cli.py` | `taskiq postgresql generate` (prints a migration script), `migrate` (creates the tables once), `plan` (prints the SQL one database still lacks) and `check` (reports it, exit 3 or 4), a `taskiq_cli` entry point |
| `taskiq_postgresql/abc/driver.py` | `QueryDriver` and `ListenDriver` interfaces |
| `taskiq_postgresql/drivers/` | One module per driver, each implementing both interfaces; `_params.py` (bind parameters), `_health.py` (the `LISTEN` health check) are shared; each driver's `on_startup` runs `start_driver` from `core/stores/schema.py` on its own `Session` |
| `taskiq_postgresql/core/stores/` | The layer between a component and its driver: a component calls only its store, and the store calls the `QueryDriver`. `declaration.py`: `TableDeclaration` (what a component hands its driver, and `of_driver` to read it back); `schema.py`: `SchemaStore` (builds, binds, starts, migrates and closes the driver; `schema_store` is how the CLI and `migrations.py` reach it), `read_database`, and `start_table`, `start_driver`, `Session` (the one `on_startup` flow); `messages.py`: `MessageStore` (insert, NOTIFY, the two claims); `results.py`: `ResultStore` (set, exists, get, delete, delete by date); `schedules.py`: `ScheduleStore` (rows, add, delete). No module there imports a driver or `migrations` at module level |
| `taskiq_postgresql/utils/` | Driver lookup by name, DSN helpers, optional-import checks |
| `tests/` | Integration tests against a real PostgreSQL |
| `tests/conftest.py`, `tests/helpers.py` | Fixtures (`postgresql_dsn`, drivers, teardown, `django_configured` and `django_db`, which import Django only when a test asks) and shared helpers: teardown, scratch databases, DSN and port utilities, the 0.4.x broker table, the expected bounded statements, catalogs and plans |
| `tests/django_support.py` | Django for the tests: `settings.configure()` without pytest-django, the default database pointed at a test's own, `migrate` and the app's recorded migrations |
| `tests/test_readme_examples.py` | Runs the README's examples and checks its tables |
| `tests/test_listen_recovery.py`, `tests/silent_proxy.py` | Lost and silent connections (reopening `LISTEN`, bounded waits), through a TCP proxy that makes a connection go silent, refuse or drop |
| `tests/unit/` | Tests that need no database (SQL text, parameter shaping, wiring); `test_identifier.py`, `test_statement.py` and `test_catalog.py` cover the core/sql modules, `test_sql_source.py` fails on a `Statement` or `Script` built, or a string that starts with an upper-case SQL command word (`SELECT`, `CREATE`, `SET` ...), outside `core/sql/`, and a `# noqa: S608` written outside `core/sql/catalog/`; lower-case DML is ruff's S608, not that test's; `test_component_driver_calls.py` pins the driver calls each component makes (a recording driver), and `test_store_layering.py` fails when a component, `cli.py` or `migrations.py` reaches `.driver` outside the components' `driver` property |
| `tests/test_sql_catalog.py`, `tests/test_components_names.py` | The catalog's declared types and names against a real server; the name rule, the warning and the NOTIFY of a mixed-case channel through each component |
| `tests/test_migration_plan.py`, `tests/golden/migration_steps.sql` | The comparison engine against real catalogs, and the released steps, which must never change |
| `tests/relax_schema.py`, `tests/unit/test_migration_relax.py`, `tests/unit/test_alembic_relax.py` | Test-only tables whose steps relax `NOT NULL` (no released step does), and the engine's and the Alembic comparator's handling of them without a database |
| `tests/test_startup_check.py`, `tests/test_cli_plan.py` | The table check of `startup()` without `run_migrations`, and `taskiq postgresql plan` and `check`, against real catalogs |
| `tests/test_contrib_*.py` | The SQLAlchemy, SQLModel, Alembic and Django integrations; the Alembic ones carry the `alembic` mark (`pytest -m alembic`), which CI also runs on the oldest Alembic the extra allows; the Django ones configure Django once per session with `settings.configure()` (no pytest-django) and point its default database at a database of their own |
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
- **SQL.** All SQL text comes from the catalog in `core/sql/catalog/`
  (`abc/query.py` only re-exports it); nothing else builds SQL with an
  f-string, `+`, `%` or `.format`. Table and channel names are checked by the
  `Identifier` rule when a component is built (ADR 0008), and values travel as
  bind parameters (`$1`, `$2`, ...), never formatted into the SQL text. The
  broker's NOTIFY is `SELECT FROM pg_notify($1, $2)` with the channel and the
  id as binds. The only names written into runtime SQL text are the checked
  table name, as configured, and the channel in `LISTEN`, folded to lower case
  so all three drivers subscribe to the channel `pg_notify` sends to. Column
  names come from configuration and are emitted unquoted.
- **Schema.** Tables are created with `CREATE TABLE IF NOT EXISTS` when
  `run_migrations=True`, so existing deployments keep their tables. Columns
  added later (the broker's `channel` and `not_before` and their index) come
  from `AddColumnsQuery` with `ADD COLUMN IF NOT EXISTS`, so a table created by
  an older release gets them too; DDL is additive, or relaxes `NOT NULL`
  (`DropNotNullQuery`, a step's `StepObjects.nullable_columns`, ADR 0011);
  nothing is removed or renamed. Changing a column is a schema change for
  existing deployments; `tests/unit/test_components.py` and
  `tests/test_migrations.py::test_add_claim_columns_query_is_the_upgrade_sql`
  pin the current DDL. `migrations.py` builds its steps from the same builders,
  and
  `tests/test_migration_script.py::test_the_steps_are_the_sql_run_migrations_executes`
  checks that they are the statements the drivers build for `on_startup`. The
  bundled drivers' `on_startup` is one flow (`start_table` in
  `core/stores/schema.py`, reached through the component's store) that starts with the engine's one read-only catalog
  query: with `run_migrations=True` it skips the lock and the DDL when no step
  is pending; without it, it runs no DDL and raises `SchemaNotReadyError` when
  a table or column is missing, or a column a step relaxes is still `NOT NULL`.
  Both log what no step fixes. A driver without `component_kind` keeps the
  `RelationsExistQuery` probe when it migrates. The migration's advisory lock
  key (`MigrationLockQuery` in `core/sql/catalog/`) is a contract between releases;
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
