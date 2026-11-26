"""
``scripts/release.py``: the three commands that turn a CHANGELOG into a release.

Nothing here needs a database, a network or a git repository. The script runs
every external program through one function, ``run``, and these tests replace
it with a fake that answers for ``git`` and ``git-cliff``. The CHANGELOG text
is written in each test, never read from the repository: the live file changes
with every release.

The script is imported by path inside each test, so a missing script fails
every test on its own and collection still succeeds.
"""

import importlib.util
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "release.py"

ARGPARSE_USAGE_ERROR = 2
BASE_URL = "https://github.com/example/project"
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

    def __init__(
        self,
        *,
        tags: Sequence[str] = ("v0.4.1", "v0.1.0"),
        bumped: str = "v0.5.0\n",
        cliff_body: str = CLIFF_BODY,
    ) -> None:
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
    tags: Sequence[str] = ("v0.4.1", "v0.1.0"),
    bumped: str = "v0.5.0\n",
    cliff_body: str = CLIFF_BODY,
) -> tuple[ModuleType, Tools]:
    """Load the script and put the fake in place of ``run``."""
    module = load()
    tools = Tools(tags=tags, bumped=bumped, cliff_body=cliff_body)
    monkeypatch.setattr(module, "run", tools)
    monkeypatch.setattr(
        module.shutil,
        "which",
        lambda name: None if name in missing else f"/usr/bin/{name}",
    )
    return module, tools


def prepare_args(path: Path, version: str = "0.5.0", *extra: str) -> list[str]:
    return [
        "prepare",
        "--version",
        version,
        "--date",
        "2026-12-01",
        "--changelog",
        str(path),
        *extra,
    ]


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / "CHANGELOG.md"


def write(path: Path, text: str) -> str:
    path.write_bytes(text.encode("utf-8"))
    return text


def read(path: Path) -> str:
    return path.read_bytes().decode("utf-8")


def section_headings(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("## ")]


# prepare --source unreleased


def test_when_the_source_is_unreleased__then_the_body_moves_byte_for_byte(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, tools = install(monkeypatch)
    write(path, changelog(HAND_WRITTEN))

    code = release.main(prepare_args(path, "0.5.0", "--source", "unreleased"))

    assert code == 0
    assert read(path) == (
        f"{HEAD}## [Unreleased]\n"
        "\n"
        f"## [0.5.0] - 2026-12-01\n{HAND_WRITTEN}"
        f"{FOOTER}\n"
        f"[unreleased]: {BASE_URL}/compare/v0.5.0...HEAD\n"
        f"[0.5.0]: {BASE_URL}/compare/v0.4.1...v0.5.0\n"
    )
    # The hand-written text is never given to git-cliff: only the version is.
    assert [call[1:] for call in tools.cliff_calls()] == [["--bumped-version"]]


def test_when_the_source_is_unreleased_and_it_is_empty__then_it_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, _ = install(monkeypatch)
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path, "0.5.0", "--source", "unreleased"))

    assert code != 0
    assert "[Unreleased]" in capsys.readouterr().err
    assert read(path) == original


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


# prepare --source cliff


def test_when_the_source_is_cliff__then_its_output_becomes_the_body(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, tools = install(monkeypatch)
    write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    assert code == 0
    text = read(path)
    assert text == (
        f"{HEAD}## [Unreleased]\n"
        "\n"
        "## [0.5.0] - 2026-12-01\n"
        "\n"
        f"{CLIFF_BODY}"
        "\n"
        f"{FOOTER}\n"
        f"[unreleased]: {BASE_URL}/compare/v0.5.0...HEAD\n"
        f"[0.5.0]: {BASE_URL}/compare/v0.4.1...v0.5.0\n"
    )
    # The body starts at the first group: only the script writes a heading, so
    # there is exactly one `## [0.5.0]`, however the template is edited.
    assert section_headings(text) == ["## [Unreleased]", "## [0.5.0] - 2026-12-01"]
    assert [call[1:] for call in tools.cliff_calls()] == [
        ["--bumped-version"],
        ["--unreleased", "--tag", "v0.5.0", "--strip", "all"],
    ]


def test_when_the_source_is_cliff_and_unreleased_has_text__then_it_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, tools = install(monkeypatch)
    original = write(path, changelog(HAND_WRITTEN))

    code = release.main(prepare_args(path))

    assert code != 0
    err = capsys.readouterr().err
    assert "--source unreleased" in err
    assert "Upgrade notes" in err
    assert read(path) == original
    assert tools.cliff_calls() == []


def test_when_git_cliff_prints_nothing__then_no_section_is_written(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, _ = install(monkeypatch, cliff_body="\n")
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    assert code != 0
    assert read(path) == original


# prepare: refusals shared by both sources


def test_when_the_section_exists__then_a_second_prepare_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, _ = install(monkeypatch)
    write(path, changelog("\n"))
    assert release.main(prepare_args(path)) == 0
    after_first = read(path)

    code = release.main(prepare_args(path))

    assert code != 0
    assert "## [0.5.0]" in capsys.readouterr().err
    assert read(path) == after_first
    assert after_first.count("## [0.5.0]") == 1


def test_when_the_section_was_written_by_hand__then_prepare_refuses_it(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, _ = install(monkeypatch)
    original = write(
        path,
        changelog("\n", sections="## [0.5.0] - 2026-11-01\n\n### Added\n\n- A.\n\n"),
    )

    assert release.main(prepare_args(path)) != 0
    assert read(path) == original


def test_when_the_tag_exists__then_prepare_is_refused_and_says_to_fetch_tags(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, _ = install(monkeypatch, tags=("v0.5.0", "v0.4.1"))
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    assert code != 0
    err = capsys.readouterr().err
    assert "v0.5.0" in err
    assert "git fetch --tags" in err
    assert read(path) == original


@pytest.mark.parametrize("version", ["0.5.0\n", "v0.5.0", "0.5", "01.2.3"])
def test_when_the_version_is_malformed__then_nothing_runs_and_nothing_is_written(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
    version: str,
) -> None:
    release, tools = install(monkeypatch)
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path, version))

    assert code != 0
    assert "version" in capsys.readouterr().err
    assert tools.calls == []
    assert read(path) == original


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


def test_when_git_cliff_expects_another_version__then_prepare_exits_non_zero(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, _ = install(monkeypatch, bumped="v0.6.0\n")
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    assert code != 0
    err = capsys.readouterr().err
    assert "v0.6.0" in err
    assert "--allow-version-mismatch" in err
    assert read(path) == original


@pytest.mark.parametrize("source", ["cliff", "unreleased"])
def test_when_the_mismatch_is_allowed__then_prepare_goes_through(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    source: str,
) -> None:
    release, _ = install(monkeypatch, bumped="v0.6.0\n")
    write(path, changelog(HAND_WRITTEN if source == "unreleased" else "\n"))

    code = release.main(
        prepare_args(path, "0.5.0", "--source", source, "--allow-version-mismatch")
    )

    assert code == 0
    assert "## [0.5.0] - 2026-12-01" in read(path)


@pytest.mark.parametrize("source", ["cliff", "unreleased"])
def test_when_the_mismatch_is_not_allowed__then_both_sources_check_it(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    source: str,
) -> None:
    release, _ = install(monkeypatch, bumped="v0.6.0\n")
    original = write(path, changelog(HAND_WRITTEN if source == "unreleased" else "\n"))

    code = release.main(prepare_args(path, "0.5.0", "--source", source))

    assert code != 0
    assert read(path) == original


# The links that prepare writes


def test_when_a_version_follows_another__then_the_links_use_the_previous_tag(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    # 0.10.0 is newer than 0.9.0 and 0.2.0, which a text sort would get wrong;
    # a release candidate and a branch-like tag are not versions.
    release, _ = install(
        monkeypatch,
        tags=("v0.10.0", "v0.9.0rc1", "v0.9.0", "vnext", "v0.2.0"),
        bumped="v0.9.1\n",
    )
    write(
        path,
        changelog(
            "\n",
            sections="## [0.9.0] - 2026-10-01\n\n### Added\n\n- A.\n\n",
            links=(
                f"[unreleased]: {BASE_URL}/compare/v0.9.0...HEAD\n"
                f"[0.9.0]: {BASE_URL}/compare/v0.2.0...v0.9.0\n"
            ),
        ),
    )

    assert release.main(prepare_args(path, "0.9.1")) == 0

    text = read(path)
    assert text.endswith(
        f"[unreleased]: {BASE_URL}/compare/v0.9.1...HEAD\n"
        f"[0.9.1]: {BASE_URL}/compare/v0.9.0...v0.9.1\n"
        f"[0.9.0]: {BASE_URL}/compare/v0.2.0...v0.9.0\n"
    )
    assert section_headings(text) == [
        "## [Unreleased]",
        "## [0.9.1] - 2026-12-01",
        "## [0.9.0] - 2026-10-01",
    ]


@pytest.mark.parametrize("tags", [(), ("v0.6.0", "vnext")])
def test_when_no_tag_is_below_the_version__then_prepare_exits_non_zero(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
    tags: tuple[str, ...],
) -> None:
    release, _ = install(monkeypatch, tags=tags)
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    assert code != 0
    assert "previous" in capsys.readouterr().err
    assert read(path) == original


def test_when_the_unreleased_link_is_missing__then_prepare_is_refused(
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, _ = install(monkeypatch)
    original = write(path, changelog("\n", links=""))

    assert release.main(prepare_args(path)) != 0
    assert read(path) == original


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
                "## [0.5.0] - 2026-12-01\n"
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
    write(
        path,
        changelog("\n", sections=f"## [0.5.0] - 2026-12-01\n\n{CLIFF_BODY}\n"),
    )

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
    monkeypatch: pytest.MonkeyPatch, path: Path
) -> None:
    release, _ = install(monkeypatch)
    write(path, changelog("\n"))
    assert release.main(prepare_args(path)) == 0

    assert release.main(["check", "--version", "0.5.0", "--changelog", str(path)]) == 0


@pytest.mark.parametrize("what", ["section", "link", "both"])
def test_when_the_section_or_its_link_is_missing__then_check_returns_one(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    what: str,
) -> None:
    release, _ = install(monkeypatch)
    write(path, changelog("\n"))
    assert release.main(prepare_args(path)) == 0
    text = read(path)
    if what in {"section", "both"}:
        text = text.replace("## [0.5.0] - 2026-12-01", "## [0.5.1] - 2026-12-01")
    if what in {"link", "both"}:
        text = text.replace("[0.5.0]: ", "[0.5.1]: ")
    write(path, text)

    code = release.main(["check", "--version", "0.5.0", "--changelog", str(path)])

    assert code == 1


# the tools


@pytest.mark.parametrize("missing", ["git-cliff", "git"])
def test_when_a_tool_is_missing__then_the_error_names_it(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    capsys: pytest.CaptureFixture[str],
    missing: str,
) -> None:
    release, tools = install(monkeypatch, missing=(missing,))
    original = write(path, changelog("\n"))

    code = release.main(prepare_args(path))

    assert code != 0
    assert missing in capsys.readouterr().err
    assert tools.calls == []
    assert read(path) == original


def test_when_the_real_run_is_used__then_it_takes_an_argv_list_and_returns_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = load()
    seen: dict[str, object] = {}

    class Completed:
        stdout = "out\n"

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
