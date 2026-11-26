## What and why

<!-- What changes, and the reason for it. Link the issue: "Closes #123". -->

## How it was tested

<!-- Commands you ran and, for a bug fix, how you saw the test fail before the change.
     Name the driver(s) and PostgreSQL major version(s) if they matter. -->

## Checklist

- [ ] `pre-commit run --all-files` passes
- [ ] Tests cover the change (the `driver` fixture runs each one against all three drivers)
- [ ] The PR title is a Conventional Commit (it becomes the changelog entry)
- [ ] README and docstrings are updated if behaviour or public arguments changed

## Breaking change

<!-- Delete this section if there is none. Otherwise say what breaks, who is
     affected and how to migrate, and mark the PR title with `!`; the release
     PR turns this section into the "Upgrade notes" of the changelog. -->
