# Each target wraps the command CI runs, so a green target here predicts the
# matching CI job. Start with `make sync`. Targets that start PostgreSQL need
# Docker or Podman (see AGENTS.md).

UV ?= uv

.DEFAULT_GOAL := help
.PHONY: help sync lint typecheck test test-lowest build audit pins

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "%-12s %s\n", $$1, $$2}'

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
test-lowest: ## Run the suite on Python 3.10, lowest direct deps (CI job: test-lowest)
	@tmp="$$(mktemp -d)" || exit 1; \
	trap '[ ! -f "$$tmp/uv.lock" ] || cp "$$tmp/uv.lock" uv.lock; rm -rf "$$tmp"' EXIT; \
	$(EXIT_ON_SIGNAL); \
	cp uv.lock "$$tmp/uv.lock.part" && mv "$$tmp/uv.lock.part" "$$tmp/uv.lock" || exit 1; \
	UV_PROJECT_ENVIRONMENT="$$tmp/venv" $(UV) run --python 3.10 --resolution lowest-direct --all-extras --group dev pytest

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
