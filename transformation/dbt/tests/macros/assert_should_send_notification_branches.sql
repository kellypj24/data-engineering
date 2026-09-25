-- Every branch of should_send_notification, over literal rows. Returns one row
-- per case where the macro disagrees with the expected decision.
--
--   expect_high_priority: the validation is in high_priority_validations
--   expect_default:       it is not
--   and with notifications disabled, nothing ever notifies.

WITH cases AS (
    SELECT *
    FROM (
        VALUES
        ('FAIL', 'CRITICAL', TRUE, TRUE),
        ('WARN', 'CRITICAL', TRUE, TRUE),
        ('FAIL', 'HIGH', TRUE, TRUE),
        ('WARN', 'HIGH', FALSE, FALSE),
        ('FAIL', 'MEDIUM', TRUE, FALSE),
        ('WARN', 'MEDIUM', FALSE, FALSE),
        ('FAIL', 'LOW', FALSE, FALSE),
        ('WARN', 'LOW', FALSE, FALSE),
        ('PASS', NULL, FALSE, FALSE)
    ) AS c (result, severity, expect_high_priority, expect_default)
),

evaluated AS (
    SELECT
        *,
        {{ should_send_notification('result', 'severity', 'example', high_priority=['example'], enabled=true) }}
            AS got_high_priority,
        {{ should_send_notification('result', 'severity', 'example', high_priority=[], enabled=true) }}
            AS got_default,
        {{ should_send_notification('result', 'severity', 'example', high_priority=['example'], enabled=false) }}
            AS got_disabled
    FROM cases
)

SELECT *
FROM evaluated
WHERE
    got_high_priority <> expect_high_priority
    OR got_default <> expect_default
    OR got_disabled
