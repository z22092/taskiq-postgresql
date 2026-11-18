#!/usr/bin/env python3
"""
Check that every place that declares the same pin agrees on it.

The checks, each an exact comparison:

* ruff: the ``lint`` dependency group against the ``ruff-pre-commit`` rev
  comment (``# frozen: vX.Y.Z``).
* uv: the ceiling in ``[tool.uv] required-version`` against every ``version:``
  of every ``astral-sh/setup-uv`` step in ``.github/workflows``.
* PostgreSQL image: ``POSTGRES_IMAGE`` in ``tests/conftest.py`` against the
  compose file and any ``postgres:`` image reference in the workflows.
* Python versions: the trove classifiers against the CI test matrix and the
  ``requires-python`` bounds.

Exits 1 when any check fails. Standard library only; run it with Python 3.13.
"""

import re
import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
PRE_COMMIT = ROOT / ".pre-commit-config.yaml"
CONFTEST = ROOT / "tests" / "conftest.py"
COMPOSE = ROOT / "docker-compose.yml"
WORKFLOWS = ROOT / ".github" / "workflows"

IMAGE_RE = r"postgres:[\w.\-]+@sha256:[0-9a-f]{64}"
LOOSE_IMAGE_RE = r"postgres:[\w.\-]+(?:@sha256:[0-9a-f]+)?"


def rel(path: Path) -> str:
    """Return a repo-relative path for messages."""
    return str(path.relative_to(ROOT))


def load_pyproject() -> dict:
    """Parse pyproject.toml."""
    return tomllib.loads(PYPROJECT.read_text())


def check_ruff(pyproject: dict, errors: list[str]) -> None:
    """Compare the ruff pin in the lint group with the pre-commit rev."""
    pins = [
        item.split("==", 1)[1].split(";", 1)[0].strip()
        for item in pyproject["dependency-groups"]["lint"]
        if re.match(r"ruff\s*==", item)
    ]
    if len(pins) != 1:
        errors.append(
            f"{rel(PYPROJECT)}: expected exactly one 'ruff==X' in the lint "
            f"group, found {pins}"
        )
        return
    group_version = pins[0]

    text = PRE_COMMIT.read_text()
    match = re.search(
        r"repo:\s*https://github\.com/astral-sh/ruff-pre-commit\s*\n\s*rev:\s*([0-9a-f]{40})\s*#\s*frozen:\s*v(\S+)",
        text,
    )
    if not match:
        errors.append(
            f"{rel(PRE_COMMIT)}: ruff-pre-commit rev must be '<sha40> # frozen: vX.Y.Z'"
        )
        return
    if match.group(2) != group_version:
        errors.append(
            f"ruff: {rel(PYPROJECT)} lint group pins {group_version}, "
            f"{rel(PRE_COMMIT)} frozen comment says {match.group(2)}",
        )


def uv_ceiling(pyproject: dict) -> str | None:
    """Return the upper bound of ``[tool.uv] required-version``."""
    required = pyproject.get("tool", {}).get("uv", {}).get("required-version", "")
    match = re.search(r"<=\s*([0-9][^\s,]*)", required)
    return match.group(1) if match else None


def setup_uv_versions(path: Path) -> list[tuple[int, str | None]]:
    """Return ``(line, version)`` for each setup-uv step of a workflow."""
    lines = path.read_text().splitlines()
    found: list[tuple[int, str | None]] = []
    for index, line in enumerate(lines):
        if "astral-sh/setup-uv@" not in line:
            continue
        indent = len(line) - len(line.lstrip(" -"))
        version: str | None = None
        for follow in lines[index + 1 :]:
            stripped = follow.strip()
            if not stripped:
                continue
            follow_indent = len(follow) - len(follow.lstrip(" "))
            if follow_indent < indent or follow.lstrip().startswith("- "):
                break
            match = re.match(r"version:\s*[\"']?([^\"'\s#]+)", stripped)
            if match:
                version = match.group(1)
                break
        found.append((index + 1, version))
    return found


def check_uv(pyproject: dict, errors: list[str]) -> None:
    """Compare the uv ceiling with every setup-uv ``version:``."""
    ceiling = uv_ceiling(pyproject)
    if ceiling is None:
        errors.append(
            f"{rel(PYPROJECT)}: [tool.uv] required-version needs a '<=X' ceiling"
        )
        return
    total = 0
    for workflow in sorted(WORKFLOWS.glob("*.y*ml")):
        for line, version in setup_uv_versions(workflow):
            total += 1
            if version != ceiling:
                errors.append(
                    f"uv: {rel(workflow)}:{line} setup-uv version is {version!r}, "
                    f"required-version ceiling is {ceiling!r}",
                )
    if total == 0:
        errors.append("uv: no setup-uv step found in .github/workflows")


def check_postgres(errors: list[str]) -> None:
    """Compare the PostgreSQL image across conftest, compose and workflows."""
    refs: dict[str, str] = {}

    conftest = re.search(rf'POSTGRES_IMAGE\s*=\s*"({IMAGE_RE})"', CONFTEST.read_text())
    if conftest:
        refs[f"{rel(CONFTEST)} POSTGRES_IMAGE"] = conftest.group(1)
    else:
        errors.append(
            f"{rel(CONFTEST)}: POSTGRES_IMAGE must be 'postgres:<tag>@sha256:<digest>'"
        )

    compose = re.search(rf"image:\s*({LOOSE_IMAGE_RE})", COMPOSE.read_text())
    if compose:
        refs[f"{rel(COMPOSE)} image"] = compose.group(1)
    else:
        errors.append(f"{rel(COMPOSE)}: no postgres image found")

    for workflow in sorted(WORKFLOWS.glob("*.y*ml")):
        for number, line in enumerate(workflow.read_text().splitlines(), start=1):
            for ref in re.findall(rf"(?<![\w/.-]){LOOSE_IMAGE_RE}", line):
                refs[f"{rel(workflow)}:{number}"] = ref

    if len(set(refs.values())) > 1:
        detail = "; ".join(f"{where}={ref}" for where, ref in refs.items())
        errors.append(f"postgres image: references differ: {detail}")


def minor_of(version: str) -> int:
    """Return the minor number of a ``3.X`` version."""
    return int(version.split(".")[1])


def check_python(pyproject: dict, errors: list[str]) -> None:
    """Compare classifiers, the CI matrix and requires-python."""
    classifiers = {
        match.group(1)
        for item in pyproject["project"]["classifiers"]
        if (match := re.fullmatch(r"Programming Language :: Python :: (3\.\d+)", item))
    }

    ci_text = (WORKFLOWS / "ci.yml").read_text()
    matrix = re.search(r"python-version:\s*\[([^\]]+)\]", ci_text)
    if not matrix:
        errors.append(
            ".github/workflows/ci.yml: no 'python-version: [...]' matrix found"
        )
        return
    versions = {item.strip().strip("\"'") for item in matrix.group(1).split(",")}
    if versions != classifiers:
        errors.append(
            f"python: classifiers {sorted(classifiers)} != "
            f"ci.yml matrix {sorted(versions)}",
        )

    requires = pyproject["project"]["requires-python"]
    lower = re.search(r">=\s*(3\.\d+)", requires)
    upper = re.search(r"<\s*(3)\.(\d+)", requires)
    if not lower or not upper or not classifiers:
        errors.append(f"python: cannot read requires-python {requires!r}")
        return
    if lower.group(1) != min(classifiers, key=minor_of):
        errors.append(
            f"python: requires-python floor {lower.group(1)} != lowest classifier"
        )
    if f"3.{int(upper.group(2)) - 1}" != max(classifiers, key=minor_of):
        errors.append(
            f"python: requires-python ceiling {requires!r} != highest classifier"
        )


def main() -> int:
    """Run every check and report all mismatches."""
    pyproject = load_pyproject()
    errors: list[str] = []
    check_ruff(pyproject, errors)
    check_uv(pyproject, errors)
    check_postgres(errors)
    check_python(pyproject, errors)
    for error in errors:
        print(f"MISMATCH: {error}", file=sys.stderr)
    if errors:
        return 1
    print("check_pins: ruff, uv, postgres image and python versions agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
