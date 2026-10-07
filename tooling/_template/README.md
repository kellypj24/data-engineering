# Tooling Component Template

> **This is a specification, not a skeleton.** It states what a tool of this
> role must provide. Read it as a requirements checklist, then create the
> files. `tooling/secret-guard/` (E16) is the model of what "finished" looks like.

## What This Role Does

Tooling is developer-side: checks and helpers a downstream project adopts
alongside a stack, running on engineers' machines and in CI rather than in the
data path. Examples: a sensitive-data commit guard, a pre-push review, a
dataset diff between releases.

## What a New Tooling Tool Must Provide

- **A `pyproject.toml`** with a console script, managed by `uv` like every
  other tool, plus `mod.just`, `README.md`, `CLAUDE.md`, and `tests/`.
- **At least one integration point a downstream project can copy:**
  - a pre-commit hook entry (`.pre-commit-hooks.yaml` in the tool directory,
    plus a snippet for the adopter's `.pre-commit-config.yaml`), and/or
  - a GitHub Actions snippet in the README.
- **Configuration as data.** Rules, patterns, and limits live in a config file
  the adopter edits, not in code.
- **Safe failure.** State whether the tool fails open (advisory: a crash
  allows the action and warns) or fails closed (a gate: a crash blocks), and
  test that behaviour. Advisory tools must never block on their own errors.
- **Nothing leaves the machine by accident.** A tool that sends data anywhere
  (an API, a model) states exactly what is sent and bounds it.
- **Tests that need no network.** Mock subprocesses and remote calls.
- **Stdlib-first** where the tool runs as a git hook, so it works on a laptop
  without a project venv.
