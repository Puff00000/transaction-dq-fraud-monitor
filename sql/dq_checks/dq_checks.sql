-- dq_checks.sql
-- BigQuery equivalents of src/dq_checks/*.py. Each CTE mirrors one Python
-- check function 1:1 so the two can be read side by side.
--
-- NOTE: uses CREATE OR REPLACE TABLE ... AS SELECT (CTAS) rather than
-- INSERT INTO, because the free BigQuery Sandbox (no billing account)
-- blocks DML statements (INSERT/UPDATE/DELETE/MERGE) entirely -- CTAS is
-- DDL and is allowed.

CREATE OR REPLACE TABLE `fraud_monitor.dq_issues` AS
WITH total AS (
  SELECT COUNT(*) AS total_rows FROM `fraud_monitor.transactions_raw`
),

missing_customer_id AS (
  SELECT COUNT(*) AS affected
  FROM `fraud_monitor.transactions_raw`
  WHERE customer_id IS NULL OR customer_id = ''
),

missing_required_fields AS (
  SELECT COUNT(*) AS affected
  FROM `fraud_monitor.transactions_raw`
  WHERE transaction_id IS NULL OR customer_id IS NULL OR customer_id = ''
     OR amount IS NULL OR currency IS NULL OR currency = ''
     OR transaction_ts IS NULL OR status IS NULL OR status = ''
),

duplicate_ids AS (
  SELECT transaction_id, COUNT(*) OVER (PARTITION BY transaction_id) AS dup_count
  FROM `fraud_monitor.transactions_raw`
),
duplicate_transaction_id AS (
  SELECT COUNT(*) AS affected FROM duplicate_ids WHERE dup_count > 1
),

negative_or_zero_amount AS (
  SELECT COUNT(*) AS affected
  FROM `fraud_monitor.transactions_raw`
  WHERE amount <= 0
),

invalid_currency_code AS (
  SELECT COUNT(*) AS affected
  FROM `fraud_monitor.transactions_raw`
  WHERE UPPER(currency) NOT IN ('USD','EUR','GBP','INR','JPY','AUD')
     OR currency != UPPER(currency)
),

future_dated_transaction AS (
  SELECT COUNT(*) AS affected
  FROM `fraud_monitor.transactions_raw`
  WHERE transaction_ts > CURRENT_TIMESTAMP()
),

currency_country_mismatch AS (
  SELECT COUNT(*) AS affected
  FROM `fraud_monitor.transactions_raw` t
  LEFT JOIN UNNEST([
    STRUCT('US' AS country, 'USD' AS home_currency),
    ('GB','GBP'), ('DE','EUR'), ('IN','INR'),
    ('JP','JPY'), ('AU','AUD'), ('FR','EUR'), ('SG','USD')
  ]) AS home ON t.country = home.country
  WHERE home.home_currency IS NOT NULL AND t.currency != home.home_currency
)

SELECT 'completeness.missing_customer_id' AS check_name, 'critical' AS severity,
       affected AS affected_row_count, total.total_rows AS total_row_count,
       ROUND(100 * affected / total.total_rows, 3) AS affected_pct, 2.0 AS threshold_pct,
       (100 * affected / total.total_rows) <= 2.0 AS passed, CURRENT_TIMESTAMP() AS run_ts
FROM missing_customer_id, total
UNION ALL
SELECT 'completeness.missing_required_fields', 'critical', affected, total.total_rows,
       ROUND(100 * affected / total.total_rows, 3), 1.0,
       (100 * affected / total.total_rows) <= 1.0, CURRENT_TIMESTAMP()
FROM missing_required_fields, total
UNION ALL
SELECT 'uniqueness.duplicate_transaction_id', 'critical', affected, total.total_rows,
       ROUND(100 * affected / total.total_rows, 3), 0.1,
       (100 * affected / total.total_rows) <= 0.1, CURRENT_TIMESTAMP()
FROM duplicate_transaction_id, total
UNION ALL
SELECT 'validity.negative_or_zero_amount', 'critical', affected, total.total_rows,
       ROUND(100 * affected / total.total_rows, 3), 1.0,
       (100 * affected / total.total_rows) <= 1.0, CURRENT_TIMESTAMP()
FROM negative_or_zero_amount, total
UNION ALL
SELECT 'validity.invalid_currency_code', 'warning', affected, total.total_rows,
       ROUND(100 * affected / total.total_rows, 3), 0.5,
       (100 * affected / total.total_rows) <= 0.5, CURRENT_TIMESTAMP()
FROM invalid_currency_code, total
UNION ALL
SELECT 'consistency.future_dated_transaction', 'critical', affected, total.total_rows,
       ROUND(100 * affected / total.total_rows, 3), 0.3,
       (100 * affected / total.total_rows) <= 0.3, CURRENT_TIMESTAMP()
FROM future_dated_transaction, total
UNION ALL
SELECT 'consistency.currency_country_mismatch', 'warning', affected, total.total_rows,
       ROUND(100 * affected / total.total_rows, 3), 1.0,
       (100 * affected / total.total_rows) <= 1.0, CURRENT_TIMESTAMP()
FROM currency_country_mismatch, total;


-- transactions_clean: every row EXCEPT those with a CRITICAL issue.
-- Warning-only rows (e.g. currency/country mismatch) still flow through
-- to fraud scoring.
CREATE OR REPLACE TABLE `fraud_monitor.transactions_clean`
PARTITION BY DATE(transaction_ts)
CLUSTER BY customer_id
AS
SELECT t.transaction_id, t.customer_id, t.amount, t.currency, t.merchant,
       t.country, t.transaction_ts, t.status
FROM `fraud_monitor.transactions_raw` t
LEFT JOIN (
  SELECT transaction_id, COUNT(*) OVER (PARTITION BY transaction_id) AS dup_count
  FROM `fraud_monitor.transactions_raw`
) d USING (transaction_id)
WHERE t.customer_id IS NOT NULL AND t.customer_id != ''
  AND t.amount > 0
  AND t.transaction_ts <= CURRENT_TIMESTAMP()
  AND (d.dup_count IS NULL OR d.dup_count <= 1);

-- view results
SELECT * FROM `fraud_monitor.dq_issues` ORDER BY check_name;
