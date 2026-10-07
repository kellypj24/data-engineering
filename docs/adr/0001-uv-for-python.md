# 0001. `uv` for every Python tool

- Status: Accepted
- Date: 2026-10-07

## Context
Each tool is independent and carries its own dependencies. pip with
requirements files gives no lock, so builds drift; poetry locks but is slow and
has its own resolver quirks. Dependabot's `pip` ecosystem edited
`pyproject.toml` without touching the lock, so the declared floors and the
locked versions drifted apart and PR titles misreported versions.

## Decision
Every Python tool has a `pyproject.toml` and a committed `uv.lock`. All commands
run through `uv` (`uv sync`, `uv run`, `uv lock`). Dependabot uses the `uv`
ecosystem, and CI's `lockfiles` job runs `uv lock --check` for every tool.

## Consequences
Installs are fast and reproducible, and a lock that disagrees with its
`pyproject.toml` fails CI. Contributors need `uv` installed. There is no
`requirements.txt` anywhere, so tools that expect one need an export step.
