# Each target wraps the command CI runs, so a green target here predicts the
# matching CI job. Start with `make sync`. Targets that start PostgreSQL need
# Docker or Podman (see AGENTS.md).

UV ?= uv
# Where `make release` takes the new section's body from; see scripts/release.py.
SOURCE ?= cliff
# The release targets read these as shell variables ($$VERSION in a recipe), so
# a value is never pasted into the recipe text and cannot add shell syntax to it.
export VERSION SOURCE DATE ALLOW_MISMATCH

.DEFAULT_GOAL := help
.PHONY: help sync lint typecheck test test-lowest build audit pins changelog release release-notes

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "%-14s %s\n", $$1, $$2}'

sync: ## Install the project, every driver extra and every group from uv.lock
	$(UV) sync --locked --all-extras --all-groups

lint: ## Run every pre-commit hook on every file (CI job: lint)
	$(UV) run --no-sync pre-commit run --all-files --show-diff-on-failure

typecheck: ## Type-check the package with ty (also a pre-commit hook)
	$(UV) run --no-sync ty check

test: ## Run the full suite with the coverage gate (CI job: test)
	$(UV) run --no-sync pytest --cov=taskiq_postgresql --cov-report=term-missing

# Recipes that make temporary files clean up in an EXIT trap. dash, the usual
# /bin/sh, skips that trap when a signal kills it, so INT and TERM are turned
# into a plain exit first. Ctrl-C reaches pytest too, which stops on its own.
EXIT_ON_SIGNAL = trap 'exit 130' INT; trap 'exit 143' TERM

# uv re-resolves the lowest direct versions and rewrites uv.lock on disk, so
# the lock is copied aside and put back however the recipe ends. The
# environment lives in the same temporary directory, so the project .venv
# keeps its Python. The copy is renamed into place only once complete, so the
# trap never puts back a partial lock.
#
# pytest runs in the background and the shell forwards INT and TERM to it as
# TERM, which tests/conftest.py turns into a KeyboardInterrupt so the fixtures
# tear down and the PostgreSQL container is removed (with Ryuk disabled
# nothing else removes it). TERM is forwarded instead of INT because a
# background job of a non-interactive sh starts with SIGINT ignored. The shell
# then waits for pytest to finish, so the lock is put back after the teardown.
test-lowest: ## Run the suite on Python 3.10, lowest direct deps (CI job: test-lowest)
	@tmp="$$(mktemp -d)" || exit 1; \
	trap '[ ! -f "$$tmp/uv.lock" ] || cp "$$tmp/uv.lock" uv.lock; rm -rf "$$tmp"' EXIT; \
	$(EXIT_ON_SIGNAL); \
	cp uv.lock "$$tmp/uv.lock.part" && mv "$$tmp/uv.lock.part" "$$tmp/uv.lock" || exit 1; \
	pid=; stop=; \
	trap 'stop=1; [ -z "$$pid" ] || kill -TERM "$$pid" 2>/dev/null' INT TERM; \
	UV_PROJECT_ENVIRONMENT="$$tmp/venv" $(UV) run --python 3.10 --resolution lowest-direct --all-extras --group dev pytest & \
	pid=$$!; \
	[ -z "$$stop" ] || kill -TERM "$$pid" 2>/dev/null; \
	wait "$$pid"; rc=$$?; \
	while kill -0 "$$pid" 2>/dev/null; do wait "$$pid"; rc=$$?; done; \
	exit $$rc

build: ## Build the sdist and wheel into dist/ (CI job: CD build)
	$(UV) build

# Needs network: pip-audit queries the PyPI and OSV advisory databases.
audit: ## Audit every locked dependency, hashes required (CI job: pip-audit)
	@req="$$(mktemp)" || exit 1; \
	trap 'rm -f "$$req"' EXIT; \
	$(EXIT_ON_SIGNAL); \
	$(UV) export --quiet --locked --all-extras --all-groups --no-emit-project --output-file "$$req" \
		&& $(UV) run --locked --only-group audit pip-audit --strict --disable-pip --require-hashes --requirement "$$req"

pins: ## Check that pins declared in more than one place agree
	$(UV) run --no-sync python scripts/check_pins.py

# The release targets run through `uv run --only-group release`, which puts the
# pinned git-cliff on PATH for scripts/release.py. They write no git state: no
# commit, branch, tag or push. Their variables travel in the environment and
# the shell expands them inside double quotes, so make never pastes them into
# the command; scripts/release.py then checks each one (it is the only
# validation). Run `git fetch --tags` first: the script sees local tags only.
changelog: ## Preview the next CHANGELOG section from the commits; writes nothing
	$(UV) run --locked --only-group release git-cliff --unreleased --bump --strip all

release: ## Add the CHANGELOG section: VERSION=X.Y.Z [SOURCE=unreleased] [DATE=YYYY-MM-DD] [ALLOW_MISMATCH=1]
	$(UV) run --locked --only-group release python scripts/release.py prepare --version "$$VERSION" --source "$$SOURCE" $${DATE:+--date "$$DATE"} $${ALLOW_MISMATCH:+--allow-version-mismatch}

release-notes: ## Print the CHANGELOG section of VERSION=X.Y.Z for the GitHub Release
	@$(UV) run --locked --only-group release python scripts/release.py notes --version "$$VERSION"
