"""
``scripts/release.py``: the three commands that turn a CHANGELOG into a release.

Nothing here needs a database, a network or a git repository. The script runs
every external program through one function, ``run``, and these tests replace
it with a fake that answers for ``git`` and ``git-cliff``. The CHANGELOG text
is written in each test, never read from the repository: the live file changes
with every release.

The script is imported by path inside each test, so a missing script fails
every test on its own and collection still succeeds. The Makefile tests run
``make`` with ``UV=echo``, so they print the command a target would run
instead of running uv.
"""

import importlib.util
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Any, NamedTuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "release.py"
MAKEFILE = ROOT / "Makefile"

ARGPARSE_USAGE_ERROR = 2
MAX_ERROR_CHARS = 1000
BASE_URL = "https://github.com/example/project"
DATE = "2026-12-01"
HEAD = "# Changelog\n\nAll notable changes are documented in this file.\n\n"
FOOTER = (
    "Earlier versions are not described in this file; see the\n"
    "[releases page](https://example.test/releases).\n"
)
CLIFF_BODY = (
    "### Added\n"
    "\n"
    "- **cli:** Add a generate command ([#1](https://example.test/pull/1))\n"
    "\n"
    "### Fixed\n"
    "\n"
    "- Fix the sweep ([#2](https://example.test/pull/2))\n"
)
# The defaults of the fake: one place, used by every test that does not say
# otherwise.
DEFAULT_TAGS = ("v0.4.1", "v0.1.0")
DEFAULT_BUMPED = "v0.5.0\n"
# Typical hand-written content: a prose block with SQL, then Keep a Changelog
# groups. The trailing spaces, the tab and the non-ASCII text must all survive.
HAND_WRITTEN = (
    "\n"
    "Upgrading from 0.4.x:\n"
    "\n"
    "- Pass `dsn` to every component.  \n"
    "- Run this once:\n"
    "\n"
    "  ```sql\n"
    "  ALTER TABLE jobs\n"
    "  \tADD COLUMN IF NOT EXISTS channel VARCHAR;\n"
    "  ```\n"
    "\n"
    "### Added\n"
    "\n"
    "- Naïve text with a [link](https://example.test) and a [ref]: not a link def.\n"
    "\n"
    "### Fixed\n"
    "\n"
    "- Something broke.\n"
    "\n"
)
UNRELEASED = ("--source", "unreleased")


def load() -> ModuleType:
    """Import ``scripts/release.py`` afresh, so a patch never leaks across tests."""
    spec = importlib.util.spec_from_file_location("release_script", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["release_script"] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop("release_script", None)
        raise
    return module


def changelog(
    unreleased: str,
    *,
    sections: str = "",
    links: str = f"[unreleased]: {BASE_URL}/compare/v0.4.1...HEAD\n",
) -> str:
    """Build a CHANGELOG shaped like the repository's."""
    return f"{HEAD}## [Unreleased]\n{unreleased}{sections}{FOOTER}\n{links}"


class Tools:
    """A fake ``run`` that answers for git and git-cliff and keeps its calls."""

    def __init__(self, tags: Sequence[str], bumped: str, cliff_body: str) -> None:
        self.tags = list(tags)
        self.bumped = bumped
        self.cliff_body = cliff_body
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str]) -> str:
        """Answer one call the way the real program would."""
        self.calls.append(list(argv))
        program = Path(argv[0]).name
        if program == "git":
            assert list(argv[1:]) == ["tag", "--list", "v*", "--sort=-v:refname"]
            return "".join(f"{tag}\n" for tag in self.tags)
        assert program == "git-cliff"
        if "--bumped-version" in argv:
            return self.bumped
        return self.cliff_body

    def cliff_calls(self) -> list[list[str]]:
        """Return the calls made to git-cliff."""
        return [call for call in self.calls if Path(call[0]).name == "git-cliff"]


def install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    missing: Sequence[str] = (),
    tags: Sequence[str] = DEFAULT_TAGS,
    bumped: str = DEFAULT_BUMPED,
    cliff_body: str = CLIFF_BODY,
) -> tuple[ModuleType, Tools]:
    """Load the script, and put the fake built from these settings in ``run``."""
    module = load()
    tools = Tools(tags, bumped, cliff_body)
    monkeypatch.setattr(module, "run", tools)
    monkeypatch.setattr(
        module.shutil,
        "which",
        lambda name: None if name in missing else f"/usr/bin/{name}",
    )
    return module, tools


def prepare_args(
    path: Path, version: str = "0.5.0", *extra: str, date: str = DATE
) -> list[str]:
    return [
        "prepare",
        "--version",
        version,
        "--date",
        date,
        "--changelog",
        str(path),
        *extra,
    ]


def write(path: Path, text: str) -> str:
    path.write_bytes(text.encode("utf-8"))
    return text


def read(path: Path) -> str:
    return path.read_bytes().decode("utf-8")


class Outcome(NamedTuple):
    """What one ``prepare`` run did."""

    code: int
    err: str
    original: str
    after: str
    tools: Tools


def run_prepare(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
    *,
    text: str | None = None,
    version: str = "0.5.0",
    date: str = DATE,
    args: Sequence[str] = (),
    **settings: Any,
) -> Outcome:
    """Write a CHANGELOG, run ``prepare`` on it against the fake, and report."""
    release, tools = install(monkeypatch, **settings)
    original = write(path, changelog("\n") if text is None else text)
    code = release.main(prepare_args(path, version, *args, date=date))
    return Outcome(code, capsys.readouterr().err, original, read(path), tools)


def section_headings(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("## ")]


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / "CHANGELOG.md"


# prepare, from the hand-written [Unreleased]


def test_when_the_source_is_unreleased__then_the_body_moves_byte_for_byte(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    outcome = run_prepare(
        monkeypatch,
        capsys,
        path,
        text=changelog(HAND_WRITTEN),
        args=UNRELEASED,
    )

    assert outcome.code == 0
    assert outcome.after == (
        f"{HEAD}## [Unreleased]\n"
        "\n"
        f"## [0.5.0] - {DATE}\n{HAND_WRITTEN}"
        f"{FOOTER}\n"
        f"[unreleased]: {BASE_URL}/compare/v0.5.0...HEAD\n"
        f"[0.5.0]: {BASE_URL}/compare/v0.4.1...v0.5.0\n"
    )
    # The hand-written text is never given to git-cliff: only the version is.
    assert [call[1:] for call in outcome.tools.cliff_calls()] == [["--bumped-version"]]


def test_when_the_source_is_unknown__then_argparse_refuses_it(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, tools = install(monkeypatch)
    original = write(path, changelog("\n"))

    with pytest.raises(SystemExit) as raised:
        release.main(prepare_args(path, "0.5.0", "--source", "git"))

    assert raised.value.code == ARGPARSE_USAGE_ERROR
    assert tools.calls == []
    assert read(path) == original


# prepare, from git-cliff


def test_when_the_source_is_cliff__then_its_output_becomes_the_body(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    outcome = run_prepare(monkeypatch, capsys, path)

    assert outcome.code == 0
    assert outcome.after == (
        f"{HEAD}## [Unreleased]\n"
        "\n"
        f"## [0.5.0] - {DATE}\n"
        "\n"
        f"{CLIFF_BODY}"
        "\n"
        f"{FOOTER}\n"
        f"[unreleased]: {BASE_URL}/compare/v0.5.0...HEAD\n"
        f"[0.5.0]: {BASE_URL}/compare/v0.4.1...v0.5.0\n"
    )
    # The body starts at the first group: only the script writes a heading, so
    # there is exactly one `## [0.5.0]`, however the template is edited.
    assert section_headings(outcome.after) == [
        "## [Unreleased]",
        f"## [0.5.0] - {DATE}",
    ]
    assert [call[1:] for call in outcome.tools.cliff_calls()] == [
        ["--bumped-version"],
        ["--unreleased", "--tag", "v0.5.0", "--strip", "all"],
    ]


def test_when_the_source_is_cliff_and_unreleased_has_text__then_git_cliff_never_runs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    outcome = run_prepare(monkeypatch, capsys, path, text=changelog(HAND_WRITTEN))

    assert outcome.code != 0
    assert "--source unreleased" in outcome.err
    assert "Upgrade notes" in outcome.err
    assert outcome.after == outcome.original
    assert outcome.tools.cliff_calls() == []


def test_when_the_section_exists__then_a_second_prepare_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    release, _ = install(monkeypatch)
    write(path, changelog("\n"))
    assert release.main(prepare_args(path)) == 0
    after_first = read(path)
    capsys.readouterr()

    code = release.main(prepare_args(path))

    assert code != 0
    assert "## [0.5.0]" in capsys.readouterr().err
    assert read(path) == after_first
    assert after_first.count("## [0.5.0]") == 1


# prepare, refusals: each says why and leaves the file as it was

SECTION_BY_HAND = changelog(
    "\n", sections="## [0.5.0] - 2026-11-01\n\n### Added\n\n- A.\n\n"
)


def refusal(
    case: str,
    fragments: Sequence[str],
    *,
    quiet: bool = False,
    **kwargs: Any,
) -> Any:
    """One refusal: ``quiet`` means nothing may run before it."""
    return pytest.param(kwargs, fragments, quiet, id=case)


REFUSALS = [
    *(
        refusal(f"version {value!r}", ["version"], quiet=True, version=value)
        for value in ["0.5.0\n", "v0.5.0", "0.5", "01.2.3"]
    ),
    *(
        refusal(f"date {value!r}", ["date"], quiet=True, date=value)
        for value in ["2026-13-01", "20261201", "2026-12-1", "tomorrow"]
    ),
    refusal("tag exists", ["v0.5.0", "git fetch --tags"], tags=("v0.5.0", "v0.4.1")),
    refusal("section by hand", ["## [0.5.0]"], text=SECTION_BY_HAND),
    refusal("git-cliff missing", ["git-cliff"], quiet=True, missing=("git-cliff",)),
    refusal("git missing", ["git"], quiet=True, missing=("git",)),
    refusal(
        "below the newest tag",
        ["v0.6.0", "backport"],
        tags=("v0.6.0", "v0.4.1"),
    ),
    refusal(
        "below the newest tag, mismatch allowed",
        ["v0.6.0", "backport"],
        tags=("v0.6.0", "v0.4.1"),
        args=("--allow-version-mismatch",),
    ),
    refusal("no tag at all", ["previous"], tags=()),
    refusal(
        "version git-cliff does not expect",
        ["v0.6.0", "--allow-version-mismatch"],
        bumped="v0.6.0\n",
    ),
    refusal(
        "version git-cliff does not expect, from [Unreleased]",
        ["v0.6.0", "--allow-version-mismatch"],
        bumped="v0.6.0\n",
        text=changelog(HAND_WRITTEN),
        args=UNRELEASED,
    ),
    refusal("empty [Unreleased] to move", ["[Unreleased]"], args=UNRELEASED),
    refusal("git-cliff prints nothing", ["no entries"], cliff_body="\n"),
    refusal(
        "no [unreleased] link",
        ["[unreleased]"],
        text=changelog("\n", links=""),
    ),
]


@pytest.mark.parametrize(("kwargs", "fragments", "quiet"), REFUSALS)
def test_when_prepare_must_refuse__then_it_says_why_and_leaves_the_file(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
    kwargs: dict[str, Any],
    fragments: Sequence[str],
    quiet: bool,
) -> None:
    outcome = run_prepare(monkeypatch, capsys, path, **kwargs)

    assert outcome.code != 0
    for fragment in fragments:
        assert fragment in outcome.err
    assert outcome.after == outcome.original
    if quiet:
        assert outcome.tools.calls == []


@pytest.mark.parametrize("command", ["notes", "check"])
@pytest.mark.parametrize("version", ["0.5.0\n", "v0.5.0", "0.5", "01.2.3"])
def test_when_the_version_is_malformed__then_notes_and_check_refuse_it_too(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    command: str,
    version: str,
) -> None:
    release, tools = install(monkeypatch)
    write(path, changelog("\n"))

    code = release.main([command, "--version", version, "--changelog", str(path)])

    assert code != 0
    assert tools.calls == []


@pytest.mark.parametrize("source", ["cliff", "unreleased"])
def test_when_the_mismatch_is_allowed__then_prepare_goes_through_and_still_asks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
    source: str,
) -> None:
    outcome = run_prepare(
        monkeypatch,
        capsys,
        path,
        text=changelog(HAND_WRITTEN if source == "unreleased" else "\n"),
        bumped="v0.6.0\n",
        args=("--source", source, "--allow-version-mismatch"),
    )

    assert outcome.code == 0
    assert f"## [0.5.0] - {DATE}" in outcome.after
    # git-cliff is still asked, so the maintainer sees the version it expects.
    assert ["--bumped-version"] in [call[1:] for call in outcome.tools.cliff_calls()]


# prepare, links


def test_when_a_version_follows_another__then_the_links_use_the_previous_tag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    # 0.10.0 is below 0.10.1 and above 0.9.0 and 0.2.0, which a text sort would
    # get wrong; a release candidate and a branch-like tag are not versions.
    outcome = run_prepare(
        monkeypatch,
        capsys,
        path,
        version="0.10.1",
        text=changelog(
            "\n",
            sections="## [0.10.0] - 2026-10-01\n\n### Added\n\n- A.\n\n",
            links=(
                f"[unreleased]: {BASE_URL}/compare/v0.10.0...HEAD\n"
                f"[0.10.0]: {BASE_URL}/compare/v0.9.0...v0.10.0\n"
            ),
        ),
        tags=("v0.10.0", "v0.9.0rc1", "v0.9.0", "vnext", "v0.2.0"),
        bumped="v0.10.1\n",
    )

    assert outcome.code == 0
    assert outcome.after.endswith(
        f"[unreleased]: {BASE_URL}/compare/v0.10.1...HEAD\n"
        f"[0.10.1]: {BASE_URL}/compare/v0.10.0...v0.10.1\n"
        f"[0.10.0]: {BASE_URL}/compare/v0.9.0...v0.10.0\n"
    )
    assert section_headings(outcome.after) == [
        "## [Unreleased]",
        f"## [0.10.1] - {DATE}",
        "## [0.10.0] - 2026-10-01",
    ]


# notes


def test_when_a_section_exists__then_notes_prints_exactly_its_body(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, tools = install(monkeypatch)
    write(
        path,
        changelog(
            "\n",
            sections=(
                f"## [0.5.0] - {DATE}\n"
                f"{HAND_WRITTEN}"
                "## [0.4.1] - 2026-11-01\n"
                "\n"
                "### Fixed\n"
                "\n"
                "- Older.\n"
                "\n"
            ),
        ),
    )

    code = release.main(["notes", "--version", "0.5.0", "--changelog", str(path)])

    assert code == 0
    # Neither the heading, nor the next section, nor the footer and its links.
    assert capsys.readouterr().out == HAND_WRITTEN.strip("\n") + "\n"
    assert tools.calls == []


def test_when_the_last_section_is_read__then_the_footer_is_not_in_the_notes(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, _ = install(monkeypatch)
    write(path, changelog("\n", sections=f"## [0.5.0] - {DATE}\n\n{CLIFF_BODY}\n"))

    assert release.main(["notes", "--version", "0.5.0", "--changelog", str(path)]) == 0

    assert capsys.readouterr().out == CLIFF_BODY


def test_when_the_section_is_missing__then_notes_exits_non_zero(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, _ = install(monkeypatch)
    write(path, changelog(HAND_WRITTEN))

    code = release.main(["notes", "--version", "0.5.0", "--changelog", str(path)])

    captured = capsys.readouterr()
    assert code != 0
    assert captured.out == ""
    assert "0.5.0" in captured.err


# check


def test_when_the_section_and_its_link_exist__then_check_returns_zero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    release, _ = install(monkeypatch)
    assert run_prepare(monkeypatch, capsys, path).code == 0

    assert release.main(["check", "--version", "0.5.0", "--changelog", str(path)]) == 0


@pytest.mark.parametrize("what", ["section", "link", "both"])
def test_when_the_section_or_its_link_is_missing__then_check_returns_one(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
    what: str,
) -> None:
    release, _ = install(monkeypatch)
    text = run_prepare(monkeypatch, capsys, path).after
    if what in {"section", "both"}:
        text = text.replace(f"## [0.5.0] - {DATE}", f"## [0.5.1] - {DATE}")
    if what in {"link", "both"}:
        text = text.replace("[0.5.0]: ", "[0.5.1]: ")
    write(path, text)

    code = release.main(["check", "--version", "0.5.0", "--changelog", str(path)])

    assert code == 1


# the real run: argv lists, stdout, and the reason a program failed


class Completed:
    stdout = "out\n"


def test_when_the_real_run_is_used__then_it_takes_an_argv_list_and_returns_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = load()
    seen: dict[str, object] = {}

    def fake_run(argv: Sequence[str], **kwargs: object) -> Completed:
        seen["argv"] = list(argv)
        seen.update(kwargs)
        return Completed()

    monkeypatch.setattr(release.subprocess, "run", fake_run)

    assert release.run(["tool", "--flag"]) == "out\n"
    assert seen["argv"] == ["tool", "--flag"]
    assert seen["check"] is True
    assert seen["capture_output"] is True
    assert seen.get("shell", False) is False


def failing_run(stderr: str) -> Any:
    def fake_run(argv: Sequence[str], **kwargs: object) -> None:
        raise subprocess.CalledProcessError(2, list(argv), output="", stderr=stderr)

    return fake_run


def test_when_a_program_fails__then_the_error_has_the_end_of_its_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: Path,
) -> None:
    release = load()
    monkeypatch.setattr(release.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(
        release.subprocess,
        "run",
        failing_run("noise\nfatal: not a git repository\n"),
    )
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    err = capsys.readouterr().err
    assert code != 0
    assert "git" in err
    assert "exited 2" in err
    assert "fatal: not a git repository" in err
    assert read(path) == original


def test_when_stderr_is_long__then_the_error_keeps_only_its_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = load()
    monkeypatch.setattr(
        release.subprocess,
        "run",
        failing_run("early line\n" + "x" * 5000 + "\nthe last line"),
    )

    with pytest.raises(release.ReleaseError) as raised:
        release.run(["tool"])

    message = str(raised.value)
    assert "the last line" in message
    assert "early line" not in message
    assert len(message) < MAX_ERROR_CHARS


# the Makefile: values reach the script through the environment, not the recipe


def make_output(*args: str) -> list[str]:
    """Run ``make`` with ``UV=echo`` and return the lines of what it printed."""
    make = shutil.which("make")
    assert make is not None, "the release targets need make"
    # Nothing from the caller's environment may leak into the targets' variables.
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {"VERSION", "SOURCE", "DATE", "ALLOW_MISMATCH", "MAKEFLAGS", "MFLAGS"}
    }
    completed = subprocess.run(  # noqa: S603  # argv list, resolved make, fixed target names
        [make, "--silent", "--no-print-directory", "UV=echo", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.splitlines()


PREPARE_PREFIX = "run --locked --only-group release python scripts/release.py"


def test_when_release_gets_only_a_version__then_the_source_is_cliff() -> None:
    assert make_output("release", "VERSION=0.5.1") == [
        f"{PREPARE_PREFIX} prepare --version 0.5.1 --source cliff"
    ]


def test_when_release_gets_every_variable__then_each_one_reaches_the_script() -> None:
    assert make_output(
        "release",
        "VERSION=0.5.0",
        "SOURCE=unreleased",
        f"DATE={DATE}",
        "ALLOW_MISMATCH=1",
    ) == [
        (
            f"{PREPARE_PREFIX} prepare --version 0.5.0 --source unreleased"
            f" --date {DATE} --allow-version-mismatch"
        )
    ]


def test_when_the_version_holds_shell_syntax__then_make_does_not_run_it() -> None:
    hostile = '1.2.3"; echo PWNED; "'

    lines = make_output("release", f"VERSION={hostile}")

    # One line, the value as one argument: a second `echo` would print PWNED
    # on a line of its own.
    assert lines == [f"{PREPARE_PREFIX} prepare --version {hostile} --source cliff"]
    assert make_output("release-notes", f"VERSION={hostile}") == [
        f"{PREPARE_PREFIX} notes --version {hostile}"
    ]


def test_when_a_recipe_is_read__then_no_release_variable_is_pasted_into_it() -> None:
    recipes = [
        line for line in MAKEFILE.read_text().splitlines() if line.startswith("\t")
    ]
    pasted = re.compile(r"\$\((?:VERSION|SOURCE|DATE|ALLOW_MISMATCH)\)")

    # The release recipes exist and read the shell variable, so the check
    # below cannot pass on a Makefile that has no release target at all.
    assert any('--version "$$VERSION"' in line for line in recipes)
    assert [line for line in recipes if pasted.search(line)] == []
