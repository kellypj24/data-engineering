# Architecture decision records

One file per decision that shapes how this toolkit is built: what was decided,
why, and what it costs. They record decisions already made, so a newcomer (or a
model) does not reopen them by accident, and a deliberate reversal has
something concrete to supersede.

## Format

`NNNN-short-title.md`, numbered in order, never renumbered:

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by NNNN | Deprecated
- Date: YYYY-MM-DD

## Context
The forces at play: what problem, what constraints.

## Decision
What we do. One paragraph, imperative.

## Consequences
What follows, including the costs and what it rules out.
```

## Lifecycle

- **Proposed** in the PR that introduces the decision; **Accepted** when it merges.
- A decision is never edited to say something else. Write a new ADR that
  supersedes it and set the old one's status to `Superseded by NNNN`.
- **Deprecated** when the thing it governs is removed.

## Index

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-uv-for-python.md) | `uv` for every Python tool | Accepted |
| [0002](0002-just-modules-per-tool.md) | `just` modules, one per tool | Accepted |
| [0003](0003-duckdb-default-target.md) | duckdb as the credential-free default dbt target | Accepted |
| [0004](0004-dbt-env-independent-of-target.md) | `dbt_env` var, independent of `target.name` | Accepted |
| [0005](0005-skills-are-procedure.md) | Skills are procedure, `CLAUDE.md` is convention | Accepted |
