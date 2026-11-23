# 1. Record architecture decisions

- Status: Accepted
- Date: 2026-11-22

## Context

Several decisions in this repository are only visible as code: dependency
ranges, how the tests get a database, how releases reach PyPI. A contributor,
human or agent, who does not know the reason for a constraint is likely to
undo it.

## Decision

Significant decisions are recorded as Architecture Decision Records in
`docs/adr/`, one Markdown file per decision, numbered in order
(`NNNN-short-title.md`). Each record has a status, a date, and three
sections: Context, Decision, Consequences. A record that is replaced stays in
place with its status set to `Superseded by NNNN`.

Records 0002 to 0006 were written after the fact, for decisions the code
already implemented.

## Consequences

- A change that reverses a recorded decision adds a new record that
  supersedes the old one, in the same pull request.
- `AGENTS.md` and `docs/architecture.md` link here instead of repeating the
  reasons.
