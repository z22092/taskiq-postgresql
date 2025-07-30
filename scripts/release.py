#!/usr/bin/env python3
"""Release automation script for taskiq-postgresql."""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def get_current_version() -> str:
    """Get the current version from pyproject.toml."""
    pyproject_path = Path("pyproject.toml")
    content = pyproject_path.read_text()

    version_match = re.search(r'version = "([^"]+)"', content)
    if not version_match:
        raise ValueError("Could not find version in pyproject.toml")

    return version_match.group(1)


def update_version(new_version: str) -> None:
    """Update version in pyproject.toml."""
    pyproject_path = Path("pyproject.toml")
    content = pyproject_path.read_text()

    # Update version
    content = re.sub(
        r'version = "[^"]+"',
        f'version = "{new_version}"',
        content,
    )

    pyproject_path.write_text(content)
    print(f"Updated version to {new_version} in pyproject.toml")


def validate_version(version: str) -> bool:
    """Validate version format (semantic versioning)."""
    pattern = r"^\d+\.\d+\.\d+(?:-[a-zA-Z0-9]+(?:\.\d+)?)?$"
    return bool(re.match(pattern, version))


def check_git_status() -> None:
    """Check if git working directory is clean."""
    result = run_command(["git", "status", "--porcelain"])
    if result.stdout.strip():
        print("Error: Git working directory is not clean")
        print("Please commit or stash your changes before releasing")
        sys.exit(1)


def run_tests() -> None:
    """Run the test suite."""
    print("Running tests...")
    result = run_command(["uv", "run", "pytest", "tests/", "-v"], check=False)
    if result.returncode != 0:
        print("Tests failed! Please fix them before releasing.")
        sys.exit(1)
    print("All tests passed!")


def run_linting() -> None:
    """Run linting and formatting checks."""
    print("Running linting checks...")

    # Check formatting
    result = run_command(["uv", "run", "ruff", "format", "--check", "."], check=False)
    if result.returncode != 0:
        print("Code formatting issues found. Run 'uv run ruff format .' to fix.")
        sys.exit(1)

    # Check linting
    result = run_command(["uv", "run", "ruff", "check", "."], check=False)
    if result.returncode != 0:
        print("Linting issues found. Please fix them before releasing.")
        sys.exit(1)

    print("All linting checks passed!")


def build_package() -> None:
    """Build the package."""
    print("Building package...")
    run_command(["uv", "build"])
    print("Package built successfully!")


def create_git_tag(version: str) -> None:
    """Create a git tag for the release."""
    tag_name = f"v{version}"
    run_command(["git", "add", "pyproject.toml"])
    run_command(["git", "commit", "-m", f"Bump version to {version}"])
    run_command(["git", "tag", "-a", tag_name, "-m", f"Release {tag_name}"])
    print(f"Created git tag: {tag_name}")


def push_release(version: str) -> None:
    """Push the release to GitHub."""
    tag_name = f"v{version}"
    run_command(["git", "push", "origin", "main"])
    run_command(["git", "push", "origin", tag_name])
    print(f"Pushed release {tag_name} to GitHub")


def main() -> None:
    """Main release function."""
    parser = argparse.ArgumentParser(
        description="Release automation for taskiq-postgresql",
    )
    parser.add_argument("version", help="New version to release (e.g., 1.0.0)")
    parser.add_argument("--skip-tests", action="store_true", help="Skip running tests")
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip linting checks",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run (don't actually release)",
    )

    args = parser.parse_args()

    # Validate version format
    if not validate_version(args.version):
        print(f"Error: Invalid version format: {args.version}")
        print("Version should follow semantic versioning (e.g., 1.0.0)")
        sys.exit(1)

    current_version = get_current_version()
    print(f"Current version: {current_version}")
    print(f"New version: {args.version}")

    if args.version == current_version:
        print("Error: New version is the same as current version")
        sys.exit(1)

    if args.dry_run:
        print("DRY RUN: Would perform the following actions:")
        print(f"1. Update version from {current_version} to {args.version}")
        print("2. Run tests and linting")
        print("3. Build package")
        print("4. Create git tag")
        print("5. Push to GitHub")
        return

    # Check git status
    check_git_status()

    # Run tests
    if not args.skip_tests:
        run_tests()

    # Run linting
    if not args.skip_checks:
        run_linting()

    # Update version
    update_version(args.version)

    # Build package
    build_package()

    # Create git tag and push
    create_git_tag(args.version)

    # Push to GitHub (this will trigger CD pipeline)
    push_release(args.version)

    print(f"✅ Release {args.version} completed successfully!")
    print("GitHub Actions will now build and publish the package to PyPI.")


if __name__ == "__main__":
    main()
