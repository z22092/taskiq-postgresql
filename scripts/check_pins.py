#!/usr/bin/env python3
"""
Check that every place that declares the same pin agrees on it.

The checks:

* uv: every ``astral-sh/setup-uv`` step in ``.github/workflows`` sets
  ``version:`` to the ceiling of ``[tool.uv] required-version`` and carries
  the same ``checksum:`` (the sha256 of that uv release's linux x86_64
  archive).
* PostgreSQL image: ``POSTGRES_IMAGE`` in ``tests/conftest.py`` against the
  compose file and any ``postgres:`` image reference in the workflows.
* Python versions: the trove classifiers against the CI test matrix and the
  ``requires-python`` bounds; ``.python-version`` and every other literal
  Python version in the workflows must be one of the classifiers.
* Ranges: runtime dependencies (``[project]`` dependencies and extras) are
  closed ranges ``>=X,<Y`` whose ceiling is the next minor of the locked
  version (the next major for ``NEXT_MAJOR`` packages); dependency groups
  and the build backend are exact ``==X`` pins.

Exits 1 when any check fails. Standard library only; run it with Python 3.13.
"""

import re
import sys
from dataclasses import dataclass
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
LOCK = ROOT / "uv.lock"
PYTHON_VERSION_FILE = ROOT / ".python-version"
CONFTEST = ROOT / "tests" / "conftest.py"
COMPOSE = ROOT / "docker-compose.yml"
WORKFLOWS = ROOT / ".github" / "workflows"

IMAGE_RE = r"postgres:[\w.\-]+@sha256:[0-9a-f]{64}"
LOOSE_IMAGE_RE = r"postgres:[\w.\-]+(?:@sha256:[0-9a-f]+)?"
VERSION_RE = r"[0-9][\w.+!-]*"
PYTHON_LITERAL_RE = (
    r"(?:python-version:\s*[\"']?|--python\s+|python-version\s*==\s*[\"'])"
    r"(3\.\d+)\b"
)

# typing-extensions removes APIs only in major releases, so the next major is
# the first release that can break it. pydantic follows semantic versioning
# too: v2 minors keep the v2 API, and v3 is the breaking line.
NEXT_MAJOR = frozenset({"pydantic", "typing-extensions"})


def rel(path: Path) -> str:
    """Return a repo-relative path for messages."""
    return str(path.relative_to(ROOT))


@dataclass(frozen=True)
class Workflow:
    """A workflow file, read once and shared by every check."""

    path: Path
    lines: list[str]

    @property
    def name(self) -> str:
        """Return the repo-relative path."""
        return rel(self.path)


def load_workflows() -> list[Workflow]:
    """Read every workflow under ``.github/workflows``."""
    return [
        Workflow(path, path.read_text().splitlines())
        for path in sorted(WORKFLOWS.glob("*.y*ml"))
    ]


def step_inputs(lines: list[str], index: int) -> dict[str, str]:
    """Return the keys under the step whose ``uses:`` line is ``index``."""
    line = lines[index]
    indent = len(line) - len(line.lstrip(" -"))
    inputs: dict[str, str] = {}
    for follow in lines[index + 1 :]:
        stripped = follow.strip()
        if not stripped or stripped.startswith("#"):
            continue
        follow_indent = len(follow) - len(follow.lstrip(" "))
        if follow_indent < indent or follow.lstrip().startswith("- "):
            break
        match = re.match(r"([\w-]+):\s*[\"']?([^\"'\s#]*)", stripped)
        if match:
            inputs[match.group(1)] = match.group(2)
    return inputs


def uv_ceiling(pyproject: dict) -> str | None:
    """Return the upper bound of ``[tool.uv] required-version``."""
    required = pyproject.get("tool", {}).get("uv", {}).get("required-version", "")
    match = re.search(r"<=\s*([0-9][^\s,]*)", required)
    return match.group(1) if match else None


def check_uv(pyproject: dict, workflows: list[Workflow], errors: list[str]) -> None:
    """Every setup-uv step pins the required uv version and one checksum."""
    ceiling = uv_ceiling(pyproject)
    if ceiling is None:
        errors.append(
            f"{rel(PYPROJECT)}: [tool.uv] required-version needs a '<=X' ceiling"
        )
        return
    steps = [
        (f"{workflow.name}:{index + 1}", step_inputs(workflow.lines, index))
        for workflow in workflows
        for index, line in enumerate(workflow.lines)
        if "astral-sh/setup-uv@" in line
    ]
    if not steps:
        errors.append("uv: no setup-uv step found in .github/workflows")
        return
    errors.extend(
        f"uv: {where} setup-uv version is {inputs.get('version')!r}, "
        f"required-version ceiling is {ceiling!r}"
        for where, inputs in steps
        if inputs.get("version") != ceiling
    )
    checksums = {inputs.get("checksum") for _, inputs in steps}
    single = next(iter(checksums)) if len(checksums) == 1 else None
    if single is None or not re.fullmatch(r"[0-9a-f]{64}", single):
        errors.append(
            "uv: every setup-uv step needs the same 64-hex 'checksum:' (sha256 "
            f"of uv-x86_64-unknown-linux-gnu.tar.gz {ceiling}), found "
            f"{sorted(str(value) for value in checksums)}",
        )


def check_postgres(workflows: list[Workflow], errors: list[str]) -> None:
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

    for workflow in workflows:
        for number, line in enumerate(workflow.lines, start=1):
            for ref in re.findall(rf"(?<![\w/.-]){LOOSE_IMAGE_RE}", line):
                refs[f"{workflow.name}:{number}"] = ref

    if len(set(refs.values())) > 1:
        detail = "; ".join(f"{where}={ref}" for where, ref in refs.items())
        errors.append(f"postgres image: references differ: {detail}")


def minor_of(version: str) -> int:
    """Return the minor number of a ``3.X`` version."""
    return int(version.split(".")[1])


def classified_pythons(pyproject: dict) -> set[str]:
    """Return the ``3.X`` versions named by the trove classifiers."""
    return {
        match.group(1)
        for item in pyproject["project"]["classifiers"]
        if (match := re.fullmatch(r"Programming Language :: Python :: (3\.\d+)", item))
    }


def check_python(
    pyproject: dict,
    classifiers: set[str],
    workflows: list[Workflow],
    errors: list[str],
) -> None:
    """Compare the classifiers with the CI matrix and requires-python."""
    ci = next((w for w in workflows if w.path.name == "ci.yml"), None)
    matrix = next(
        (
            match
            for line in (ci.lines if ci else [])
            if (match := re.search(r"python-version:\s*\[([^\]]+)\]", line))
        ),
        None,
    )
    if not matrix:
        errors.append(
            ".github/workflows/ci.yml: no 'python-version: [...]' matrix found"
        )
    else:
        versions = {item.strip().strip("\"'") for item in matrix.group(1).split(",")}
        if versions != classifiers:
            errors.append(
                f"python: classifiers {sorted(classifiers)} != "
                f"ci.yml matrix {sorted(versions)}",
            )

    requires = pyproject["project"]["requires-python"]
    lower = re.search(r">=\s*(3\.\d+)", requires)
    upper = re.search(r"<\s*(3)\.(\d+)", requires)
    if not lower or not upper:
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


def check_python_literals(
    classifiers: set[str],
    workflows: list[Workflow],
    errors: list[str],
) -> None:
    """Every pinned interpreter must be a supported (classified) version."""
    pinned = [(rel(PYTHON_VERSION_FILE), PYTHON_VERSION_FILE.read_text().strip())]
    for workflow in workflows:
        for number, line in enumerate(workflow.lines, start=1):
            if re.search(r"python-version:\s*\[", line):
                continue  # the matrix is compared as a set in check_python
            pinned.extend(
                (f"{workflow.name}:{number}", version)
                for version in re.findall(PYTHON_LITERAL_RE, line)
            )
    errors.extend(
        f"python: {where} uses {version}, not in classifiers {sorted(classifiers)}"
        for where, version in pinned
        if version not in classifiers
    )


def normalize(name: str) -> str:
    """Return the PEP 503 normalized form of a distribution name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def release_of(version: str) -> tuple[int, ...]:
    """Return the numeric release segment of a version (``1.2.3rc1`` -> 1,2,3)."""
    match = re.match(r"\d+(?:\.\d+)*", version)
    return tuple(int(part) for part in match.group(0).split(".")) if match else ()


def locked_versions() -> dict[str, tuple[int, ...]]:
    """Return the highest locked release of every package in uv.lock."""
    locked: dict[str, tuple[int, ...]] = {}
    for package in tomllib.loads(LOCK.read_text()).get("package", []):
        name = normalize(package["name"])
        release = release_of(package.get("version", ""))
        locked[name] = max(locked.get(name, ()), release)
    return locked


def requirement_specifier(requirement: str) -> tuple[str, str]:
    """Split ``name[extras]>=1,<2; marker`` into ``(name, specifier)``."""
    bare = requirement.split(";", 1)[0].strip()
    match = re.match(r"([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?\s*(.*)", bare)
    if not match:
        return bare, ""
    return normalize(match.group(1)), match.group(2).replace(" ", "")


def expected_ceiling(name: str, locked: tuple[int, ...]) -> str:
    """Return the ceiling decision 3 asks for, given the locked release."""
    major, minor = (*locked, 0, 0)[:2]
    return f"{major + 1}" if name in NEXT_MAJOR else f"{major}.{minor + 1}"


def check_ranges(pyproject: dict, errors: list[str]) -> None:
    """Runtime dependencies are closed next-minor ranges; tooling is exact."""
    project = pyproject["project"]
    runtime = list(project.get("dependencies", []))
    for extra in project.get("optional-dependencies", {}).values():
        runtime.extend(extra)
    locked = locked_versions()
    for requirement in runtime:
        name, spec = requirement_specifier(requirement)
        match = re.fullmatch(rf">={VERSION_RE},<({VERSION_RE})", spec)
        if not match:
            errors.append(
                f"range: {rel(PYPROJECT)} runtime dependency {name!r} is "
                f"{spec!r}, expected a closed range '>=X,<Y'",
            )
            continue
        if name not in locked:
            errors.append(f"range: {name!r} is not in {rel(LOCK)}")
            continue
        expected = expected_ceiling(name, locked[name])
        if match.group(1) != expected:
            errors.append(
                f"range: {name!r} ceiling is <{match.group(1)}, expected "
                f"<{expected} (next {'major' if name in NEXT_MAJOR else 'minor'} "
                f"of the locked {'.'.join(map(str, locked[name]))})",
            )

    exact = list(pyproject.get("build-system", {}).get("requires", []))
    for group in pyproject.get("dependency-groups", {}).values():
        exact.extend(item for item in group if isinstance(item, str))
    for requirement in exact:
        name, spec = requirement_specifier(requirement)
        if not re.fullmatch(rf"=={VERSION_RE}", spec):
            errors.append(
                f"range: {rel(PYPROJECT)} tooling pin {name!r} is {spec!r}, "
                "expected an exact '==X'",
            )


def main() -> int:
    """Run every check and report all mismatches."""
    pyproject = tomllib.loads(PYPROJECT.read_text())
    workflows = load_workflows()
    errors: list[str] = []
    check_uv(pyproject, workflows, errors)
    check_postgres(workflows, errors)
    classifiers = classified_pythons(pyproject)
    if classifiers:
        check_python(pyproject, classifiers, workflows, errors)
        check_python_literals(classifiers, workflows, errors)
    else:
        errors.append(f"{rel(PYPROJECT)}: no 'Python :: 3.X' classifier found")
    check_ranges(pyproject, errors)
    for error in errors:
        print(f"MISMATCH: {error}", file=sys.stderr)
    if errors:
        return 1
    print("check_pins: uv, postgres image, python versions and ranges agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
