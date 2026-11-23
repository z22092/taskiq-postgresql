---
name: prepare-release
description: Prepare a taskiq-postgresql release up to the point where a maintainer publishes it - CHANGELOG, version, release pull request and release notes. Use when asked to cut, prepare or check a release. It never tags, creates the GitHub Release or publishes to PyPI.
---

# Prepare a release

Publishing is the maintainer's: a GitHub Release with tag `vX.Y.Z` on `main`
starts `.github/workflows/cd.yml`, and the `pypi` environment approval lets
it publish. This skill prepares everything before that and stops.

## Checklist

1. **Start from `main`.** `git fetch origin` and branch from `origin/main`.
   Its latest CI run must be green.
2. **Pick the version.** Read the unreleased entries in `CHANGELOG.md`.
   While the major is 0, a breaking change bumps the minor
   (`0.5.x` -> `0.6.0`); anything else bumps the patch.
3. **CHANGELOG.** Move the entries under `## [X.Y.Z] - YYYY-MM-DD`, keep the
   Keep a Changelog sections (Added, Changed, Deprecated, Removed, Fixed,
   Security), mark breaking changes with **BREAKING:**, and update the
   compare links at the bottom of the file.
4. **Version.** Leave `version` in `pyproject.toml` and `uv.lock` alone. The
   version comes from the tag: `cd.yml` writes it into the build, and there
   is no version bump commit (see "Releases" in `AGENTS.md`).
5. **Verify locally.**

   ```bash
   make lint
   make test
   make build      # builds with the version in pyproject.toml, not X.Y.Z
   ```

6. **Open the release pull request** (`chore: release X.Y.Z`), which
   changes only `CHANGELOG.md`, with the new section as its description.
7. **Hand over.** Give the maintainer:
   - the tag to create, `vX.Y.Z`, on the merge commit on `main`;
   - the release notes (the CHANGELOG section);
   - the reminder that `publish-pypi` waits for approval on the `pypi`
     environment, and that the result is checked on PyPI together with its
     provenance attestation.

To rehearse the pipeline without a release, the maintainer can run
`cd.yml` by `workflow_dispatch`, which publishes to TestPyPI only.

## Never

- Pushing a tag (`git push --tags`, `git push origin vX.Y.Z`),
  `gh release create`, `gh workflow run`.
- `uv publish`, `twine upload`, or any upload to PyPI or TestPyPI.
- Editing `uv.lock` by hand.
