#!/usr/bin/env python3
"""
Prepare a release section of CHANGELOG.md, and read it back.

Three commands, all on the CHANGELOG and none of them a git write (no commit,
branch, tag or push: the release PR, the tag and the GitHub Release are the
maintainer's):

* ``prepare --version X.Y.Z``: add ``## [X.Y.Z] - <date>`` under an empty
  ``## [Unreleased]`` and move the compare links. The body comes from
  git-cliff (``--source cliff``, the default) or, once, from the hand-written
  ``[Unreleased]`` (``--source unreleased``, for the 0.5.0 transition).
* ``notes --version X.Y.Z``: print the body of ``## [X.Y.Z]``, for the GitHub
  Release.
* ``check --version X.Y.Z``: exit 0 only when the section and its compare link
  both exist.

The tag checks see local tags only: run ``git fetch --tags`` first. Standard
library only; ``git`` and ``git-cliff`` are resolved from ``PATH``, and ``uv
run --only-group release`` (what ``make release`` does) puts the pinned
``git-cliff`` there.
"""

from __future__ import annotations

import argparse
import datetime
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple, get_args

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parent.parent
CHANGELOG = ROOT / "CHANGELOG.md"

# fullmatch, not search: it refuses "0.5.0\n", "v0.5.0", "0.5" and "01.2.3",
# and nothing else is ever put in a command line or a regular expression.
VERSION_RE = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
# Where a section stops: the next section, the closing paragraph of the file or
# the link block. Only our own link lines end it (`[unreleased]` and versions),
# so a link written inside a body does not.
SECTION_END_RE = re.compile(
    r"^(?:## |Earlier versions are not described|\[(?:unreleased|[0-9][^\]\n]*)\]: )",
    re.MULTILINE,
)
UNRELEASED_LINK_RE = re.compile(
    r"^\[unreleased\]: (?P<base>\S+)/compare/\S+\.\.\.HEAD\n", re.MULTILINE
)
# One definition: the type checks `source`, and argparse takes its choices.
Source = Literal["cliff", "unreleased"]
SOURCES = get_args(Source)
# How much of a failed program's stderr goes into the error.
STDERR_LINES = 5
STDERR_CHARS = 400


class ReleaseError(Exception):
    """A refusal the maintainer can act on; ``main`` prints it and exits 1."""


class Version(NamedTuple):
    """A release version; tuples compare numerically, so 0.10.0 > 0.9.0."""

    major: int
    minor: int
    patch: int

    @property
    def text(self) -> str:
        """The version as ``X.Y.Z``."""
        return f"{self.major}.{self.minor}.{self.patch}"

    @property
    def tag(self) -> str:
        """The tag of the version, ``vX.Y.Z``, rebuilt from the numbers."""
        return f"v{self.text}"


def parse_version(value: str) -> Version | None:
    """
    Parse ``X.Y.Z``.

    Args:
        value: The version, without a leading ``v``.

    Returns:
        The version, or None when the value is not exactly ``X.Y.Z`` (digits
        only, no leading zero).
    """
    match = VERSION_RE.fullmatch(value)
    if match is None:
        return None
    major, minor, patch = match.groups()
    return Version(int(major), int(minor), int(patch))


def check_version(value: str) -> Version:
    """
    Parse a version the maintainer typed.

    Raises:
        ReleaseError: The value is not exactly ``X.Y.Z``.
    """
    version = parse_version(value)
    if version is None:
        msg = f"version {value!r} is not X.Y.Z (digits only, no leading zero, no v)"
        raise ReleaseError(msg)
    return version


def check_date(value: str) -> None:
    """
    Validate a date given as ``YYYY-MM-DD``.

    Raises:
        ReleaseError: The value is not a calendar date in that form.
    """
    # fromisoformat alone also takes "20261201" on Python 3.11 and later.
    if DATE_RE.fullmatch(value):
        try:
            datetime.date.fromisoformat(value)
        except ValueError:
            pass
        else:
            return
    msg = f"date {value!r} is not a YYYY-MM-DD calendar date"
    raise ReleaseError(msg)


def run(argv: Sequence[str]) -> str:
    """
    Run one program and return its stdout.

    Every external call goes through here, so the tests replace this one
    function. git-cliff logs to stderr, so stdout is only the answer.

    Args:
        argv: The program and its arguments, already resolved and validated.

    Returns:
        What the program wrote to stdout.

    Raises:
        ReleaseError: The program exited non-zero; the message ends with the
            last lines of its stderr.
    """
    # argv is a list (no shell), its first item a binary resolved by `tool`,
    # and the only value interpolated into it is a version `check_version`
    # accepted.
    try:
        completed = subprocess.run(  # noqa: S603
            list(argv),
            check=True,
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
    except subprocess.CalledProcessError as error:
        lines = (error.stderr or "").strip().splitlines()[-STDERR_LINES:]
        reason = "\n".join(lines)[-STDERR_CHARS:]
        command = " ".join([Path(argv[0]).name, *argv[1:]])
        msg = f"{command} exited {error.returncode}"
        raise ReleaseError(f"{msg}: {reason}" if reason else msg) from error
    return completed.stdout


def tool(name: str) -> str:
    """
    Resolve a program from ``PATH``.

    Args:
        name: ``git`` or ``git-cliff``.

    Returns:
        The path of the program.

    Raises:
        ReleaseError: The program is not on ``PATH``.
    """
    found = shutil.which(name)
    if found is None:
        hint = (
            " Run through `make release`, or `uv run --only-group release`,"
            " so the pinned one is on PATH."
            if name == "git-cliff"
            else ""
        )
        msg = f"{name} is not on PATH.{hint}"
        raise ReleaseError(msg)
    return found


def local_versions() -> list[Version]:
    """
    Read the versions of the local ``v*`` tags.

    Returns:
        The version of every tag named ``v`` and ``X.Y.Z``; other tags (release
        candidates, branch-like names) are not versions.
    """
    output = run([tool("git"), "tag", "--list", "v*", "--sort=-v:refname"])
    versions = []
    for line in output.splitlines():
        version = parse_version(line[1:]) if line.startswith("v") else None
        if version is not None:
            versions.append(version)
    return versions


def find_section(text: str, label: str) -> tuple[int, int] | None:
    """
    Find the body of a section.

    Args:
        text: The CHANGELOG.
        label: What is inside the brackets of the heading: ``Unreleased`` or a
            version. The heading line may carry a date after the bracket.

    Returns:
        The start and the end of the body (the text after the heading line, up
        to where the next section, the closing paragraph or the links begin),
        or None when there is no such heading.
    """
    heading = re.search(rf"^## \[{re.escape(label)}\][^\n]*\n", text, re.MULTILINE)
    if heading is None:
        return None
    following = SECTION_END_RE.search(text, heading.end())
    return heading.end(), following.start() if following else len(text)


def refuse_if_taken(text: str, version: Version, tags: Sequence[Version]) -> None:
    """
    Refuse a version that already has a section or a tag.

    Raises:
        ReleaseError: The section or the tag exists.
    """
    if find_section(text, version.text) is not None:
        msg = f"CHANGELOG.md already has a '## [{version.text}]' section"
        raise ReleaseError(msg)
    if version in tags:
        msg = (
            f"tag {version.tag} exists. The check sees local tags only: run"
            " `git fetch --tags` if you have not, then pick the next version."
        )
        raise ReleaseError(msg)


def refuse_backport(version: Version, tags: Sequence[Version]) -> None:
    """
    Refuse a version that is not above the newest tag.

    The compare links, the ``[unreleased]`` link and git-cliff's range all
    assume the new release is the newest one.

    Raises:
        ReleaseError: Some tag is at or above the version.
    """
    newest = max(tags, default=None)
    if newest is not None and version <= newest:
        msg = (
            f"{version.tag} is not above the newest tag {newest.tag}: a release"
            " below it is a backport, which this script does not handle, and"
            " --allow-version-mismatch does not lift that."
        )
        raise ReleaseError(msg)


def previous_tag(tags: Sequence[Version]) -> Version:
    """
    Find the tag the new release is compared with: the newest one.

    Raises:
        ReleaseError: There is no tag at all, so there is nothing to compare.
    """
    newest = max(tags, default=None)
    if newest is None:
        msg = "no previous tag: the compare link needs one. Run `git fetch --tags`."
        raise ReleaseError(msg)
    return newest


def check_bumped(version: Version, *, allow_mismatch: bool) -> None:
    """
    Compare the version with the one git-cliff derives from the commits.

    It is asked even when the mismatch is allowed, so the maintainer sees the
    version git-cliff expects.

    Raises:
        ReleaseError: They differ and the mismatch is not allowed.
    """
    bumped = run([tool("git-cliff"), "--bumped-version"]).strip()
    if bumped == version.tag:
        return
    if not allow_mismatch:
        msg = (
            f"git-cliff would release {bumped} from the commits, not {version.tag}."
            " Fix the commit types, or pass --allow-version-mismatch."
        )
        raise ReleaseError(msg)
    sys.stderr.write(f"warning: git-cliff expects {bumped}, not {version.tag}\n")


def old_unreleased(text: str, source: Source) -> tuple[int, int]:
    """
    Locate ``[Unreleased]`` and check it is the way the source needs it.

    Args:
        text: The CHANGELOG.
        source: ``cliff`` (it must be empty) or ``unreleased`` (it must not be).

    Returns:
        The start and the end of its body.

    Raises:
        ReleaseError: The heading is missing or the body is the wrong way round.
    """
    bounds = find_section(text, "Unreleased")
    if bounds is None:
        msg = "CHANGELOG.md has no '## [Unreleased]' heading"
        raise ReleaseError(msg)
    has_text = bool(text[bounds[0] : bounds[1]].strip())
    if source == "unreleased" and not has_text:
        msg = "[Unreleased] is empty: nothing to move. Use --source cliff."
        raise ReleaseError(msg)
    if source == "cliff" and has_text:
        msg = (
            "[Unreleased] has text. Since 0.5.1 the list is generated from the"
            " commits, and only an 'Upgrade notes' block is written by hand, in"
            " the release PR. For the 0.5.0 transition use --source unreleased."
        )
        raise ReleaseError(msg)
    return bounds


def cliff_body(version: Version) -> str:
    """
    Generate the body of the section from the commits.

    Returns:
        git-cliff's output in the file's layout: a blank line, the groups, a
        blank line.

    Raises:
        ReleaseError: No commit since the last tag makes an entry.
    """
    # `version.tag` is rebuilt from the parsed integers, so nothing from the
    # command line reaches the argument list as text.
    body = run(
        [tool("git-cliff"), "--unreleased", "--tag", version.tag, "--strip", "all"]
    ).strip("\n")
    if not body:
        msg = "git-cliff printed no entries: no commit since the last tag counts"
        raise ReleaseError(msg)
    return f"\n{body}\n\n"


def update_links(text: str, version: Version, previous: Version) -> str:
    """
    Point ``[unreleased]`` at the new tag and add the new compare link.

    Raises:
        ReleaseError: The ``[unreleased]`` link is missing.
    """
    found = UNRELEASED_LINK_RE.search(text)
    if found is None:
        msg = "CHANGELOG.md has no '[unreleased]: .../compare/...HEAD' link"
        raise ReleaseError(msg)
    base = found.group("base")
    links = (
        f"[unreleased]: {base}/compare/{version.tag}...HEAD\n"
        f"[{version.text}]: {base}/compare/{previous.tag}...{version.tag}\n"
    )
    return text[: found.start()] + links + text[found.end() :]


def prepare(
    path: Path,
    value: str,
    *,
    source: Source,
    date: str,
    allow_mismatch: bool,
) -> None:
    """
    Add the section of a release to the CHANGELOG.

    Everything is checked before the file is written, so a refusal leaves it
    as it was.

    Args:
        path: The CHANGELOG.
        value: The version as typed, ``X.Y.Z``; it is parsed once, here.
        source: ``cliff`` or ``unreleased``.
        date: ``YYYY-MM-DD``.
        allow_mismatch: Go ahead when git-cliff derives another version.

    Raises:
        ReleaseError: Any refusal.
    """
    version = check_version(value)
    check_date(date)
    # Both programs up front, so a missing one stops the run before any call.
    tool("git")
    tool("git-cliff")
    text = path.read_bytes().decode("utf-8")
    tags = local_versions()
    refuse_if_taken(text, version, tags)
    refuse_backport(version, tags)
    start, end = old_unreleased(text, source)
    previous = previous_tag(tags)
    check_bumped(version, allow_mismatch=allow_mismatch)
    body = text[start:end] if source == "unreleased" else cliff_body(version)
    # The heading line, an empty [Unreleased], then the new section; the rest
    # of the file (older sections, the closing paragraph, the links) is kept.
    updated = f"{text[:start]}\n## [{version.text}] - {date}\n{body}{text[end:]}"
    path.write_bytes(update_links(updated, version, previous).encode("utf-8"))


def notes(path: Path, value: str) -> str:
    """
    Read the body of a release section, for the GitHub Release.

    Args:
        path: The CHANGELOG.
        value: The version, ``X.Y.Z``.

    Returns:
        The body without the heading and without the blank lines around it,
        ending in one newline.

    Raises:
        ReleaseError: The section is missing or empty.
    """
    version = check_version(value)
    text = path.read_bytes().decode("utf-8")
    bounds = find_section(text, version.text)
    if bounds is None:
        msg = f"CHANGELOG.md has no '## [{version.text}]' section"
        raise ReleaseError(msg)
    body = text[bounds[0] : bounds[1]].strip("\n")
    if not body.strip():
        msg = f"the '## [{version.text}]' section is empty"
        raise ReleaseError(msg)
    return f"{body}\n"


def check(path: Path, value: str) -> list[str]:
    """
    Find what a release lacks in the CHANGELOG.

    Args:
        path: The CHANGELOG.
        value: The version, ``X.Y.Z``.

    Returns:
        What is missing, one message each; empty when the release is ready.
    """
    version = check_version(value)
    text = path.read_bytes().decode("utf-8")
    missing = []
    if find_section(text, version.text) is None:
        missing.append(f"no '## [{version.text}]' section")
    link = rf"^\[{re.escape(version.text)}\]: \S+$"
    if not re.search(link, text, re.MULTILINE):
        missing.append(f"no '[{version.text}]:' compare link")
    return missing


def run_prepare(args: argparse.Namespace) -> int:
    """Handle ``prepare``."""
    prepare(
        args.changelog,
        args.version,
        source=args.source,
        date=args.date or datetime.date.today().isoformat(),
        allow_mismatch=args.allow_version_mismatch,
    )
    sys.stderr.write(f"added '## [{args.version}]' to {args.changelog}\n")
    return 0


def run_notes(args: argparse.Namespace) -> int:
    """Handle ``notes``."""
    sys.stdout.write(notes(args.changelog, args.version))
    return 0


def run_check(args: argparse.Namespace) -> int:
    """Handle ``check``."""
    missing = check(args.changelog, args.version)
    for message in missing:
        sys.stderr.write(f"{args.changelog}: {message}\n")
    return 1 if missing else 0


def build_parser() -> argparse.ArgumentParser:
    """Build the command line: each subcommand carries its own handler."""
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--version", required=True, help="X.Y.Z, without a leading v")
    common.add_argument(
        "--changelog", type=Path, default=CHANGELOG, help="the file to read and write"
    )
    parser = argparse.ArgumentParser(description="Prepare and read a release section.")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser(
        "prepare", parents=[common], help="add the section of a release"
    )
    prepare_parser.set_defaults(func=run_prepare)
    prepare_parser.add_argument(
        "--source",
        choices=SOURCES,
        default="cliff",
        help="cliff: generate the body; unreleased: move the hand-written one",
    )
    prepare_parser.add_argument("--date", help="YYYY-MM-DD (default: today)")
    prepare_parser.add_argument(
        "--allow-version-mismatch",
        action="store_true",
        help="go ahead when git-cliff derives another version from the commits",
    )
    commands.add_parser(
        "notes", parents=[common], help="print the body of a release section"
    ).set_defaults(func=run_notes)
    commands.add_parser(
        "check", parents=[common], help="exit 0 when the section and its link exist"
    ).set_defaults(func=run_check)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run a command.

    Args:
        argv: The command line without the program name; defaults to ``sys.argv``.

    Returns:
        The exit code: 0 on success, 1 on a refusal or a failed check.
    """
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ReleaseError as error:
        sys.stderr.write(f"error: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
