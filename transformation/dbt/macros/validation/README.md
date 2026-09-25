# Validation framework

Rule sets that need more than dbt tests offer: a **severity** per rule, a
**PASS / WARN / FAIL** decision per record, **notification routing** by
severity, and a **failure history** that outlives the run.

A dbt test passes or fails, and its evidence is gone after the run. Operations
needs to know whether a failure stops the run, warns, or is only recorded,
who gets told, and whether failures are trending.

## Pieces

| File | What it does |
|---|---|
| `validate_data_source.sql` | `validate_data_source(name, source_model, record_id_column, rules, timestamp_column=none)`: one result row per record |
| `validation_config.sql` | `get_validation_config`, `should_send_notification`, `get_notification_channel`, `validation_severity_ranks` |
| `purge_validation_log.sql` | Post-hook for `validation_log`: deletes rows past each validation's retention |
| `no_validation_failures.sql` | Generic test that ties a severity tier to a dbt test severity |
| `models/validation/val_orders.sql` | Example rule set, one model per rule set |
| `models/validation/validation_log.sql` | Incremental, append-only history of every WARN/FAIL row |
| `models/validation/validation_summary.sql` | Daily failure counts for dashboards |

## Writing a rule set

```sql
-- models/validation/val_orders.sql
{{ validate_data_source(
    name='orders',
    source_model=ref('stg_orders'),
    record_id_column='order_id',
    timestamp_column='created_at',
    rules={
        'order_id_present':    {'logic': 'order_id IS NOT NULL', 'severity': 'CRITICAL'},
        'amount_non_negative': {'logic': 'amount >= 0',          'severity': 'HIGH'},
        'status_known':        {'logic': "status IN ('active', 'pending')", 'severity': 'MEDIUM'},
        'amount_under_limit':  {'logic': 'amount < 1000',        'severity': 'LOW'},
    }
) }}
```

- `logic` is TRUE when the row **passes**. NULL counts as a failure.
- Add the model to `validation_models` in `validation_log.sql`.

Each record gets `<rule>_passed` and `<rule>_severity` for every rule, plus
`failed_rules`, `max_failed_severity_rank` / `max_failed_severity`,
`validation_result`, `should_notify`, `notification_channel`, and a
deterministic `validation_key` minted with `mint_surrogate_key` from the
validation name, source table, record id, and run time.

## Severity → result → notification

| Worst failed rule | `validation_result` | Notifies |
|---|---|---|
| CRITICAL | FAIL | always |
| HIGH | FAIL | yes |
| MEDIUM | FAIL | only if the validation is in `high_priority_validations` |
| LOW | WARN | never |
| none | PASS | no |

Notifications also require `notification_enabled` (default true). The channel
is the validation's `notification_channel` if set, otherwise the severity's
entry in `validation_notification_channels`. The framework **decides and
records**. Sending is the orchestrator's job: read `validation_log` where
`should_notify`.

## Tiers and dbt test severity

Attach `no_validation_failures` to each validation model with the dbt severity
that matches the tier:

| Tier | Typical rule severities | dbt test | Effect |
|---|---|---|---|
| **Blocking** | CRITICAL | `min_severity: CRITICAL`, `severity: error` | Stops the pipeline |
| **Conditional** | HIGH | `min_severity: HIGH`, `severity: error` or `warn`, decided per rule set | Stops or reports, by choice |
| **Quality** | MEDIUM, LOW | `min_severity: MEDIUM`, `severity: warn` | Reported and logged, run continues |

```yaml
models:
  - name: val_orders
    tests:
      - no_validation_failures:
          min_severity: CRITICAL
          config:
            severity: error
      - no_validation_failures:
          min_severity: MEDIUM
          config:
            severity: warn
```

## Configuration (vars only, no code changes)

```yaml
vars:
  validation_configs:
    orders:
      enabled: true              # false: the rule set emits zero rows
      lookback_days: 7           # only rows this recent (needs timestamp_column)
      notification_enabled: true
      notification_channel: '#orders-oncall'   # overrides severity routing
      retention_days: 30         # validation_log purge horizon (default 90)
  high_priority_validations: [orders]
  validation_notification_channels:
    CRITICAL: '#data-alerts'
    HIGH: '#data-alerts'
    MEDIUM: '#data-quality'
    LOW: '#data-quality'
```

An unknown key under `validation_configs.<name>` fails compilation, so a typo
cannot silently fall back to a default.

## Tests

- `tests/macros/assert_should_send_notification_branches.sql` covers every
  branch of the notification decision over literal rows.
- `tests/python/test_validation.py` covers results and routing, each config
  switch, and `validation_log` accumulating across runs and purging beyond
  retention.
