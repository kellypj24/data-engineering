-- One row per run lifecycle event, appended by src/telemetry/sensors.py.
-- Read in dbt as source('raw', 'orchestrator_run_events') and summarised by
-- mart_orchestrator_run_summary. Rows are appended, never updated: a run may
-- have duplicate STARTED rows, and the model takes the first.
CREATE TABLE IF NOT EXISTS orchestrator_run_events (
    run_id VARCHAR NOT NULL,
    job_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,        -- STARTED | SUCCESS | FAILURE
    trigger_source VARCHAR,         -- schedule | sensor | manual
    trigger_name VARCHAR,           -- the schedule or sensor name, if any
    dbt_command VARCHAR,            -- e.g. `dbt build`, if the run invoked dbt
    warehouse VARCHAR,
    tests_total INTEGER,            -- completion rows only
    tests_failed INTEGER,           -- completion rows only
    error VARCHAR,                  -- FAILURE rows only
    event_at TIMESTAMP NOT NULL     -- UTC
)
