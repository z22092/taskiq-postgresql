# 4. Publish to PyPI through trusted publishing from a GitHub Release

- Status: Accepted (recorded after the fact)
- Date: 2026-11-22

## Context

An API token stored as a repository secret can leak and then publish
anything under the project's name. A version typed by hand into
`pyproject.toml` can drift from the git tag, so PyPI and GitHub disagree
about what a release contains.

## Decision

- `.github/workflows/cd.yml` runs on a published GitHub Release. It takes the
  version from the tag (`vX.Y.Z` -> `X.Y.Z`), builds the sdist and wheel,
  attests their build provenance and smoke-tests the wheel installed from the
  locked, hash-checked requirements.
- The `publish-pypi` job runs in the `pypi` environment, which needs a
  maintainer's approval, and publishes through PyPI trusted publishing (an
  OIDC token issued for that job) with PEP 740 attestations. No PyPI token is
  stored.
- `workflow_dispatch` publishes the same build to TestPyPI only, through the
  `testpypi` environment, to rehearse the pipeline.

## Consequences

- The pipeline publishes only after a release is published and the
  environment approves it; publishing from a workstation is not part of the
  process, for people or agents.
- The trusted publisher has to be registered on PyPI and TestPyPI for this
  repository, workflow and environment, outside the repository.
- Every pull request also runs the build and smoke jobs, so a packaging
  break shows up before a release.
