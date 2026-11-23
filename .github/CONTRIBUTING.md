# Contributing to TaskIQ PostgreSQL

Thank you for your interest in contributing to TaskIQ PostgreSQL! This document provides guidelines and information about contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Dependency Policy](#dependency-policy)
- [Changelog](#changelog)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)
- [Release Process](#release-process)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Set up the development environment
4. Make your changes
5. Run tests and linting
6. Submit a pull request

## Development Setup

### Prerequisites

- A supported Python version (see `requires-python` in `pyproject.toml`)
- [UV](https://github.com/astral-sh/uv) within the `required-version` range in `pyproject.toml`
- Docker or Podman (the tests start PostgreSQL through testcontainers)
- Git

### Setup Instructions

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/taskiq-postgresql.git
cd taskiq-postgresql

# Install UV if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install everything (all driver extras and all dependency groups)
uv sync --locked --all-extras --all-groups

# Set up pre-commit hooks (optional but recommended)
uv run pre-commit install
```

### Database Setup

The test suite starts a throwaway PostgreSQL container by itself
(testcontainers, image pinned by digest), so no setup is needed. It runs on
the newest pinned major (18) by default; set `TEST_POSTGRES_MAJOR=17` to run on
17. An empty value means the default. An unsupported value stops the run once,
before any test, with a single usage error (exit code 4) that names the
supported majors. CI also runs a `test (py3.13, pg17)` leg on 17.

To run the tests against a database you already have, set `POSTGRESQL_URL`
(credentials come from your environment, never from the repository):

```bash
export POSTGRESQL_URL="postgresql://<user>@<host>:5432/<database>"
```

For manual work, `docker-compose.yml` starts a local PostgreSQL 18 bound to
`127.0.0.1:5432` with trust authentication (development only), so the DSN has
no password:

```bash
docker compose up -d --wait
export POSTGRESQL_URL="postgresql://postgres@localhost:5432/postgres"
```

`POSTGRES_HOST_AUTH_METHOD` only applies when the data volume is initialized. If
you ran an earlier compose file (password authentication), run
`docker compose down -v` once, or the passwordless DSN fails with
`fe_sendauth: no password supplied`.

The previous major is available behind a compose profile, on `127.0.0.1:5433`
with its own volume:

```bash
docker compose --profile pg17 up -d --wait postgres17
export POSTGRESQL_URL="postgresql://postgres@localhost:5433/postgres"
```

Moving from the PostgreSQL 17 compose file to this one needs the same
`docker compose down -v`: the 18 image keeps its data under
`/var/lib/postgresql/18/` on a volume mounted at `/var/lib/postgresql`, and it
refuses to start over a volume that still holds the 17 data format.

Port 5432 must be free on the host. The tests do not need the compose
database; the `postgresql_dsn` fixture uses `POSTGRESQL_URL` when it is set and
starts a container otherwise.

#### Podman

With rootless Podman, point testcontainers at the Podman socket and disable Ryuk
(these values are machine specific, do not commit them):

```bash
export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
```

## Running Tests

```bash
# Run all tests (all three drivers)
uv run pytest

# Run tests with coverage
uv run pytest --cov=taskiq_postgresql --cov-report=html

# Run one test file
uv run pytest tests/test_broker.py

# Run a single driver (the driver is part of the test id)
uv run pytest -k asyncpg
```

### Lowest supported dependency versions

CI runs `test (py3.10, lowest-direct)` against the floor of every direct
dependency. Run it locally with `make test-lowest`, which uses a throwaway
environment and puts `uv.lock` back however the run ends; see "Commands" in
[`AGENTS.md`](../AGENTS.md).

## Code Style

Ruff is the only formatter and linter. The configuration lives in
`pyproject.toml`.

```bash
# Format code
uv run ruff format

# Check for linting issues
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix
```

CI runs the same checks through pre-commit (`pre-commit run --all-files` over
the `lint` group). The ruff hooks are `repo: local` and call
`uv run --no-sync ruff ...`, so the version is the one pinned in `pyproject.toml`
and not a second copy in `.pre-commit-config.yaml`. `scripts/check_pins.py` is
also a local pre-commit hook: it fails when a pinned version is declared
differently in two places or a dependency range has the wrong shape.

### Code Style Guidelines

- Follow [PEP 8](https://pep8.org/) for Python code style
- Use type hints for all public APIs
- Write docstrings for all public functions and classes
- Use Google-style docstrings
- Keep line length to 88 characters (the ruff default)
- Use descriptive variable names
- Comments explain why, not what

### Example Code Style

```python
from typing import Any, Optional


async def process_task(
    task_id: str,
    data: dict[str, Any],
    timeout: Optional[float] = None,
) -> TaskResult:
    """Process a task with the given parameters.

    Args:
        task_id: Unique identifier for the task.
        data: Task data to process.
        timeout: Optional timeout in seconds.

    Returns:
        The result of task processing.

    Raises:
        TaskProcessingError: If task processing fails.
    """
    # Implementation here
    pass
```

## Dependency Policy

- Runtime dependencies use closed ranges, `>=floor,<next-minor`, so a consumer's
  resolver is never blocked by an exact pin and a new upstream minor is never
  picked up untested. The exception is `typing-extensions` and `pydantic`,
  whose projects promise no breaking changes within a major version: their
  ceiling is the next major (`<5`, `<3`), because a next-minor cap would block
  consumers from every compatible release. `NEXT_MAJOR` in
  `scripts/check_pins.py` lists them. Raising a ceiling is a deliberate change
  that needs the suite green on the new version.
- Tooling, GitHub Actions (full commit SHA), container images (digest) and the
  lock are exact.
- Supply-chain scanning has its own `audit` dependency group (pip-audit).
  pip-audit pulls `filelock` through `cachecontrol[filecache]`, and the fixed
  `filelock` needs Python >= 3.11. So the group is limited to 3.11+ and kept out
  of `dev`, which keeps a vulnerable `filelock` out of the 3.10 test
  environment. Known limit: CI runs pip-audit on 3.13 and it skips pins whose
  markers exclude that interpreter, so pins used only on 3.10 are not audited
  there.
- Dependabot waits 14 days before proposing a release, matching
  `exclude-newer = "14 days"` in `[tool.uv]`.
- `tests/conftest.py` pins one Postgres image per supported major
  (`POSTGRES_IMAGES`; the newest key is the default), and `docker-compose.yml`
  declares the same set of images (the 17 one under the `pg17` profile).
  `scripts/check_pins.py` fails when the sets differ, when the compose
  `postgres` service does not run the newest major, or when a workflow names an
  image or a literal major (quoted or not) conftest does not declare. A
  Dependabot docker PR
  needs the matching manual bump in `tests/conftest.py`; Dependabot ignores
  Postgres major bumps, which are added by hand in both files.

### Regenerating the build-constraint hashes

`[tool.uv] build-constraint-dependencies` in `pyproject.toml` pins the build
backend (`hatchling`) and everything it pulls in, each with its sha256 set, so
`uv build` cannot be fed a swapped build dependency. When you bump the
`hatchling` pin under `[build-system]`:

1. List the new build requirements, one exact pin per line, in a scratch
   file outside the repository (`build-pins.in`). Resolve `hatchling==<new>`
   without hashes first (`uv pip compile --universal --python-version 3.10`)
   to see which dependencies it needs, then pin each one exactly. Keep any
   environment marker (for example `tomli==<version>; python_version < "3.11"`).
2. Generate the hashes:

   ```bash
   uv pip compile build-pins.in --generate-hashes --universal \
       --python-version 3.10 --no-header --no-annotate
   ```

   `--python-version 3.10` is required: with the default interpreter, uv
   evaluates the `python_version < "3.11"` marker and drops `tomli` from the output.
3. Copy each `--hash=sha256:...` set into the matching `{ requirement, hashes }`
   entry, and keep the `requirement` strings identical to the pins.
4. Check the result with `uv lock --check` and `uv build`.

The sha256 sets for the pins checked into the repository were reproduced
with this exact command, with an empty diff for all eight entries, including
`hatchling` and `tomli`.

## Changelog

`CHANGELOG.md` follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

- Every PR that changes what a user sees or has to do (a feature, a fix, a
  deprecation, a removal, a changed default, a changed dependency range) adds an
  entry under `## [Unreleased]`, in the group that fits: Added, Changed,
  Deprecated, Removed, Fixed or Security. Docs-only, test-only, CI and tooling
  changes need no entry.
- Write for the person who installs the package: say what changed and, for a
  breaking change, what to do about it. Start a breaking entry with
  `**BREAKING:**`.
- The release moves the `## [Unreleased]` entries under a new
  `## [X.Y.Z] - YYYY-MM-DD` heading, leaves an empty `## [Unreleased]` above it
  and updates the comparison links at the bottom. That change goes in the PR
  that precedes the release.

## Submitting Changes

### Before Submitting

1. **Run all tests**: Ensure all tests pass
2. **Check code style**: Run `pre-commit run --all-files` (or the ruff commands above)
3. **Update documentation**: Update README, docstrings, etc.
4. **Add tests**: For new features or bug fixes
5. **Check typing**: Ensure type hints are correct

### Pull Request Process

1. **Create a branch**: Use a descriptive branch name
   ```bash
   git checkout -b feature/add-connection-pooling
   git checkout -b fix/broker-memory-leak
   ```

2. **Make atomic commits**: One logical change per commit
   ```bash
   git commit -m "feat: add connection pooling for asyncpg driver"
   git commit -m "fix: resolve memory leak in broker cleanup"
   ```

3. **Write good commit messages**: Conventional Commits, with the types,
   scope and breaking-change mark listed under "Conventions" in
   [`AGENTS.md`](../AGENTS.md); keep the first line under 72 characters and
   say why in the body.

4. **Update CHANGELOG**: Add an entry under `## [Unreleased]` for a
   user-visible change (see [Changelog](#changelog))

5. **Submit PR**: Use the pull request template

### Commit Message Examples

```
feat(broker): add support for connection pooling

- Implement connection pool configuration
- Add pool size and timeout options
- Update documentation with examples

Closes #123
```

```
fix(result_backend): resolve race condition in result retrieval

The previous implementation had a race condition when multiple
workers tried to retrieve the same result simultaneously.

Fixes #456
```

## Reporting Issues

### Before Reporting

1. **Check existing issues**: Search for similar issues
2. **Use latest version**: Ensure you're using the latest release
3. **Minimal reproduction**: Create a minimal example that reproduces the issue

### Bug Reports

Use the bug report template and include:

- **Environment details**: Python version, PostgreSQL version, driver
- **Expected behavior**: What should happen
- **Actual behavior**: What actually happens
- **Steps to reproduce**: Clear steps to reproduce the issue
- **Code example**: Minimal code that demonstrates the problem
- **Error messages**: Full error messages and stack traces

### Feature Requests

Use the feature request template and include:

- **Problem description**: What problem does this solve?
- **Proposed solution**: How should it work?
- **Use case**: When would this be useful?
- **Alternatives**: What alternatives have you considered?

## Development Guidelines

### Adding New Features

1. **Discuss first**: Open an issue to discuss major features
2. **Start small**: Break large features into smaller PRs
3. **Write tests**: Include comprehensive tests
4. **Update docs**: Update README and docstrings
5. **Consider all drivers**: Ensure compatibility with all supported drivers

### Adding New Drivers

1. **Create driver module**: Add to `taskiq_postgresql/drivers/`
2. **Implement interfaces**: Inherit from `QueryDriver` and `ListenDriver`
3. **Add detection**: Update `libs_available.py`
4. **Write tests**: Test all functionality
5. **Update documentation**: Add to README and examples

### Performance Considerations

- **Memory usage**: Consider memory implications
- **Connection handling**: Ensure proper connection cleanup
- **Async best practices**: Follow asyncio best practices

## Testing Guidelines

### Test Structure

[`AGENTS.md`](../AGENTS.md) describes the layout under "Layout": `tests/`
holds the integration tests, which run against a real PostgreSQL through
testcontainers or `POSTGRESQL_URL`, and `tests/unit/` the tests that need no
database.

### Writing Tests

1. **Use descriptive names**: Test names should describe what they test
2. **Test edge cases**: Include boundary conditions and error cases
3. **Use fixtures**: Leverage pytest fixtures for setup
4. **Prefer the real database**: Use the `postgresql_dsn` fixture rather than mocking the driver
5. **Test all drivers**: Ensure tests work with all supported drivers

### Test Example

The `driver` fixture is parametrized over `asyncpg`, `psqlpy` and `psycopg`
(it skips a driver whose package is not installed) and feeds `broker`,
`result_backend` and `scheduler_source`, so a test that takes one of them runs
once per driver. Tests are async and use the anyio mark.

```python
import asyncio
import uuid

import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_postgresql import PostgresqlBroker

pytestmark = pytest.mark.anyio


async def test_when_message_is_kicked__then_listener_receives_it(
    broker: PostgresqlBroker,
) -> None:
    """The `broker` fixture runs this test once per installed driver."""
    message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"my_msg",
        labels={},
    )

    async def first_message() -> AckableMessage | None:
        async for received in broker.listen():
            return received
        return None

    listener = asyncio.create_task(first_message())
    await asyncio.sleep(0.2)  # let the listener subscribe before kicking
    await broker.kick(message)

    received = await asyncio.wait_for(listener, timeout=5)
    assert received is not None
    assert received.data == message.message
```

This mirrors `test_when_broker_deliver_message__then_worker_receive_message`
in `tests/test_broker.py`, which is the maintained version.

## Release Process

Releases are the maintainer's. The steps, and the rule that the version
comes from the tag with no version bump commit, are under "Releases" in
[`AGENTS.md`](../AGENTS.md); the changelog step follows
[Changelog](#changelog).

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion

## Recognition

Contributors will be recognized in the project:

- **Release notes**: Acknowledgment in release notes
- **GitHub**: Contributor graph and statistics

Thank you for contributing to TaskIQ PostgreSQL!
