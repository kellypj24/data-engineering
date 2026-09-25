# file-export

## Role
Delivery: config-driven file exports out of the warehouse. See README.md for the
config contract and failure behaviour.

## Key Files

- `src/file_export/config.py` — pydantic models for the YAML contract. **All validation lives here**; add a rule here, not in the engine
- `src/file_export/sql.py` — pure SQL builder (no I/O). Values go through `literal()`; identifiers are validated by the config
- `src/file_export/engine.py` — modes, window resolution, materialise → tenant check → `.partial` writes → rename → run log
- `src/file_export/runlog.py` — run log DDL + watermark reads (`last_successful_end`)
- `src/file_export/dialects.py` — duckdb executes; Snowflake is statement generation only
- `src/file_export/dagster.py` — optional adapter; **the core must never import dagster**
- `configs/` — example exports; every file is validated by `tests/test_config.py` in CI

## Commands

```bash
just file-export::test       # pytest (includes validating configs/)
just file-export::validate   # file-export validate configs
just file-export::lint
```

## Patterns

- Shared exports: `tenant_keys` are **asserted on the result**, independent of how recipients are selected. Never weaken this to "filtered, so it must be fine"
- Resolve window bounds to literals before querying; record exactly those in the run log
- Tests use an injected `Clock` (tests/conftest.py) so windows and file names are deterministic
