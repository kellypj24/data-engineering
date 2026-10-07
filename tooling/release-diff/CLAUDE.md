# release-diff

## Role
Tooling: diff two captures of a dataset. See README.md.

## Key Files

- `src/release_diff/core.py` — pure logic, **stdlib only, no I/O**: `Profile`, `validate_headers`, `resolve_pair`, `build_queries`, `check_invariants`, `to_markdown`. Its docstring holds the SQL trust model
- `src/release_diff/engine.py` — duckdb execution and file output; checks invariants **before** writing anything
- `profiles/` — committed dataset profiles (TOML)
- `tests/fixtures/` — captures, plus a duplicate-key capture (trips the invariant) and a renamed-column capture (header mismatch)

## Patterns

- Identifiers interpolated into SQL come only from a validated profile (`quote()`); values only through `literal()`. Never build a profile from user input
- Read captures with `all_varchar` so values compare as delivered
- Look key columns up by name, never by position
