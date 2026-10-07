# 0002. `just` modules, one per tool

- Status: Accepted
- Date: 2026-10-07

## Context
Each tool has its own commands (test, lint, format, tool-specific tasks), and
the repo needs aggregate commands across all of them. Make's syntax and
implicit rules fit badly; per-tool shell scripts duplicate argument handling.

## Decision
Each tool has a `mod.just`; the root `justfile` imports each as a module
(`just dagster::test`) and defines the aggregates (`just test`, `lint`, `fmt`,
`fmt-check`). Recipes run in the module's directory. CI calls `uv` directly
rather than going through `just`.

## Consequences
`just --list` documents every command, and a tool's recipes live beside it.
Aggregates must list every tool, so missing wiring is a real risk:
`.github/scripts/check_wiring.py` fails CI when a tool is missing from any
aggregate. Because CI does not go through `just`, a broken recipe is caught
only locally; the `verify-tool` skill covers that.
