{#-
    Example rule set: one validation model per rule set. Declare the rules here;
    validate_data_source does the rest. See macros/validation/README.md.
-#}

{{ validate_data_source(
    name='orders',
    source_model=ref('stg_example'),
    record_id_column='order_id',
    timestamp_column='created_at',
    rules={
        'order_id_present': {
            'logic': 'order_id IS NOT NULL',
            'severity': 'CRITICAL'
        },
        'amount_non_negative': {
            'logic': 'amount >= 0',
            'severity': 'HIGH'
        },
        'status_known': {
            'logic': 'status IN (SELECT status_code FROM ' ~ ref('order_status_codes') ~ ')',
            'severity': 'MEDIUM'
        },
        'amount_under_review_limit': {
            'logic': 'amount < 1000',
            'severity': 'LOW'
        }
    }
) }}
