# Contributing to TaskIQ PostgreSQL

Thank you for your interest in contributing to TaskIQ PostgreSQL! This document provides guidelines and information about contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)

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

- Python 3.9 or higher
- PostgreSQL 10 or higher
- [UV](https://github.com/astral-sh/uv) (recommended) or pip
- Git

### Setup Instructions

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/taskiq-postgresql.git
cd taskiq-postgresql

# Install UV if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync --dev --all-extras

# Set up pre-commit hooks (optional but recommended)
uv run pre-commit install
```

### Database Setup

You can use Docker Compose for easy PostgreSQL setup:

```bash
docker-compose up -d
```

Or set up PostgreSQL manually and create a test database:

```sql
CREATE DATABASE taskiq_test;
```

## Running Tests

### All Tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=taskiq_postgresql --cov-report=html

# Run tests for specific driver
TEST_DRIVER=asyncpg uv run pytest
```

### Specific Test Categories

```bash
# Unit tests only
uv run pytest tests/ -m "not integration"

# Integration tests only
uv run pytest tests/ -m "integration"

# Performance tests
uv run pytest tests/ -m "performance"

# Tests for specific component
uv run pytest tests/test_broker.py
```

### Testing with Different Drivers

```bash
# Test with asyncpg
uv sync --group asyncpg
TEST_DRIVER=asyncpg uv run pytest

# Test with psycopg3
uv sync --group psycopg
TEST_DRIVER=psycopg uv run pytest

# Test with psqlpy
uv sync --group psqlpy
TEST_DRIVER=psqlpy uv run pytest


```

## Code Style

This project uses several tools to maintain code quality:

### Linting and Formatting

```bash
# Format code with ruff
uv run ruff format

# Check for linting issues
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix



### Code Style Guidelines

- Follow [PEP 8](https://pep8.org/) for Python code style
- Use type hints for all public APIs
- Write docstrings for all public functions and classes
- Use Google-style docstrings
- Keep line length to 88 characters (Black default)
- Use descriptive variable names
- Add comments for complex logic

### Example Code Style

```python
from typing import Optional

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

## Submitting Changes

### Before Submitting

1. **Run all tests**: Ensure all tests pass
2. **Check code style**: Run linting and formatting tools
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

3. **Write good commit messages**:
   - Use conventional commit format: `type(scope): description`
   - Types: feat, fix, docs, style, refactor, test, chore
   - Keep the first line under 50 characters
   - Add more details in the body if needed

4. **Update CHANGELOG**: Add entry for your changes (if applicable)

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

- **Benchmark changes**: Run performance tests for significant changes
- **Memory usage**: Consider memory implications
- **Connection handling**: Ensure proper connection cleanup
- **Async best practices**: Follow asyncio best practices

## Testing Guidelines

### Test Structure

```
tests/
├── test_broker.py              # Broker functionality tests
├── test_result_backend.py      # Result backend tests
├── test_scheduler_source.py    # Scheduler tests
├── drivers/                    # Driver-specific tests
│   ├── test_asyncpg.py
│   ├── test_psycopg.py
│   └── ...
├── integration/                # Integration tests
└── performance/               # Performance benchmarks
```

### Writing Tests

1. **Use descriptive names**: Test names should describe what they test
2. **Test edge cases**: Include boundary conditions and error cases
3. **Use fixtures**: Leverage pytest fixtures for setup
4. **Mock external dependencies**: Use mocks for external services
5. **Test all drivers**: Ensure tests work with all supported drivers

### Test Example

```python
import pytest
from taskiq_postgresql import PostgresqlBroker

class TestPostgresqlBroker:
    """Test PostgreSQL broker functionality."""
    
    async def test_broker_startup_and_shutdown(
        self,
        postgres_dsn: str,
    ) -> None:
        """Test broker can start up and shut down properly."""
        broker = PostgresqlBroker(dsn=postgres_dsn)
        
        await broker.startup()
        assert broker.is_ready
        
        await broker.shutdown()
        assert not broker.is_ready

    @pytest.mark.parametrize("driver", ["asyncpg", "psycopg", "psqlpy"])
    async def test_broker_with_different_drivers(
        self,
        postgres_dsn: str,
        driver: str,
    ) -> None:
        """Test broker works with different drivers."""
        broker = PostgresqlBroker(dsn=postgres_dsn, driver=driver)
        # Test implementation
```

## Release Process

Releases are automated through GitHub Actions, but here's the process:

1. **Update version**: Use bump2version or update manually
2. **Update CHANGELOG**: Add release notes
3. **Create PR**: Submit changes for review
4. **Merge to main**: After approval
5. **Create tag**: GitHub Actions will handle the release
6. **Verify release**: Check PyPI and GitHub releases

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Discord/Slack**: [Add links if available]

## Recognition

Contributors will be recognized in the project:

- **CONTRIBUTORS.md**: List of all contributors
- **Release notes**: Acknowledgment in release notes
- **GitHub**: Contributor graph and statistics

Thank you for contributing to TaskIQ PostgreSQL! 🚀