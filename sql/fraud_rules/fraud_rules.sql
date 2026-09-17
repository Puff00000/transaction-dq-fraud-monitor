-- fraud_rules.sql
-- BigQuery equivalents of src/fraud_rules/*.py. Runs against
-- transactions_clean (post-DQ-quarantine). Uses CREATE OR REPLACE TABLE
-- (CTAS) instead of INSERT INTO since the Sandbox blocks DML.

CREATE OR REPLACE TABLE `fraud_monitor.fraud_scores` AS

WITH
velocity_flags AS (
  SELECT
    a.transaction_id,
    'velocity' AS rule_name,
    25 AS weight,
    CONCAT(CAST(COUNT(*) AS STRING), ' transactions by ', a.customer_id,
           ' within 3 min (threshold: >5)') AS reason
  FROM `fraud_monitor.transactions_clean` a
  JOIN `fraud_monitor.transactions_clean` b
    ON a.customer_id = b.customer_id
    AND b.transaction_ts BETWEEN TIMESTAMP_SUB(a.transaction_ts, INTERVAL 3 MINUTE)
                              AND a.transaction_ts
  GROUP BY a.transaction_id, a.customer_id
  HAVING COUNT(*) > 5
),

amount_baseline AS (
  SELECT
    transaction_id, customer_id, amount, transaction_ts,
    COUNT(*) OVER w AS n_prior,
    AVG(amount) OVER w AS rolling_mean,
    STDDEV(amount) OVER w AS rolling_stddev
  FROM `fraud_monitor.transactions_clean`
  WINDOW w AS (
    PARTITION BY customer_id ORDER BY transaction_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )
),
amount_anomaly_flags AS (
  SELECT
    transaction_id, 'amount_anomaly' AS rule_name, 30 AS weight,
    CONCAT('amount ', CAST(ROUND(amount, 2) AS STRING), ' is ',
           CAST(ROUND((amount - rolling_mean) / rolling_stddev, 1) AS STRING),
           ' std devs above ', customer_id, '\'s rolling average of ',
           CAST(ROUND(rolling_mean, 2) AS STRING),
           ' (n=', CAST(n_prior AS STRING), ' prior txns)') AS reason
  FROM amount_baseline
  WHERE n_prior >= 4 AND rolling_stddev > 0
    AND (amount - rolling_mean) / rolling_stddev > 3
),

ordered_txns AS (
  SELECT transaction_id, customer_id, country, transaction_ts,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_ts) AS rn
  FROM `fraud_monitor.transactions_clean`
),
geo_pairs AS (
  SELECT
    prev.transaction_id AS prev_txn_id, curr.transaction_id AS curr_txn_id,
    prev.customer_id, prev.country AS prev_country, curr.country AS curr_country,
    TIMESTAMP_DIFF(curr.transaction_ts, prev.transaction_ts, MINUTE) AS gap_minutes
  FROM ordered_txns curr
  JOIN ordered_txns prev
    ON curr.customer_id = prev.customer_id AND curr.rn = prev.rn + 1
  WHERE prev.country != curr.country
    AND TIMESTAMP_DIFF(curr.transaction_ts, prev.transaction_ts, MINUTE) BETWEEN 0 AND 119
),
geo_impossible_flags AS (
  SELECT transaction_id, 'geo_impossible_travel' AS rule_name, 35 AS weight, reason
  FROM (
    SELECT prev_txn_id AS transaction_id,
      CONCAT(customer_id, ' transacted in ', prev_country, ' then ', curr_country,
             ' only ', CAST(gap_minutes AS STRING), ' min apart (threshold: <120 min)') AS reason
    FROM geo_pairs
    UNION ALL
    SELECT curr_txn_id AS transaction_id,
      CONCAT(customer_id, ' transacted in ', prev_country, ' then ', curr_country,
             ' only ', CAST(gap_minutes AS STRING), ' min apart (threshold: <120 min)') AS reason
    FROM geo_pairs
  )
),

failure_clusters AS (
  SELECT a.transaction_id, a.customer_id, a.transaction_ts, COUNT(*) AS cluster_size
  FROM `fraud_monitor.transactions_clean` a
  JOIN `fraud_monitor.transactions_clean` b
    ON a.customer_id = b.customer_id AND b.status = 'failed'
    AND b.transaction_ts BETWEEN TIMESTAMP_SUB(a.transaction_ts, INTERVAL 2 MINUTE) AND a.transaction_ts
  WHERE a.status = 'failed'
  GROUP BY a.transaction_id, a.customer_id, a.transaction_ts
  HAVING COUNT(*) >= 2
),
repeated_failure_flags AS (
  SELECT transaction_id, 'repeated_failure_cardtesting' AS rule_name, 30 AS weight,
    CONCAT(CAST(cluster_size AS STRING), ' failed transactions by ', customer_id,
           ' within 2 min windows (card-testing pattern, threshold: >=2)') AS reason
  FROM failure_clusters
  UNION ALL
  SELECT s.transaction_id, 'repeated_failure_cardtesting', 30,
    CONCAT('success followed a card-testing failure cluster by ', s.customer_id,
           ' within 2 min')
  FROM `fraud_monitor.transactions_clean` s
  JOIN failure_clusters f
    ON s.customer_id = f.customer_id AND s.status = 'success'
    AND s.transaction_ts BETWEEN f.transaction_ts AND TIMESTAMP_ADD(f.transaction_ts, INTERVAL 2 MINUTE)
),

odd_hour_flags AS (
  SELECT transaction_id, 'odd_hour' AS rule_name, 10 AS weight,
    CONCAT('transaction at ', FORMAT('%02d', EXTRACT(HOUR FROM transaction_ts)),
           ':00 local (odd-hour window: 1-4am) -- weak signal, contributing factor only') AS reason
  FROM `fraud_monitor.transactions_clean`
  WHERE EXTRACT(HOUR FROM transaction_ts) BETWEEN 1 AND 4
),

all_flags AS (
  SELECT * FROM velocity_flags
  UNION ALL SELECT * FROM amount_anomaly_flags
  UNION ALL SELECT * FROM geo_impossible_flags
  UNION ALL SELECT * FROM repeated_failure_flags
  UNION ALL SELECT * FROM odd_hour_flags
)

SELECT
  t.transaction_id, t.customer_id, t.amount,
  IFNULL(SUM(f.weight), 0) AS fraud_risk_score,
  CASE
    WHEN IFNULL(SUM(f.weight), 0) >= 50 THEN 'High'
    WHEN IFNULL(SUM(f.weight), 0) >= 25 THEN 'Medium'
    ELSE 'Low'
  END AS risk_bucket,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT f.rule_name IGNORE NULLS ORDER BY f.rule_name), ';') AS triggered_rules,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT f.reason IGNORE NULLS), ' | ') AS reasons,
  CURRENT_TIMESTAMP() AS run_ts
FROM `fraud_monitor.transactions_clean` t
LEFT JOIN all_flags f ON t.transaction_id = f.transaction_id
GROUP BY t.transaction_id, t.customer_id, t.amount;

-- view results
SELECT risk_bucket, COUNT(*) AS n
FROM `fraud_monitor.fraud_scores`
GROUP BY risk_bucket
ORDER BY n DESC;
