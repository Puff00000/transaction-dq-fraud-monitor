-- generate_synthetic_data.sql
-- Generates synthetic transaction data DIRECTLY inside BigQuery (no CSV
-- upload needed). Uses CREATE OR REPLACE TABLE ... AS SELECT (CTAS)
-- rather than INSERT INTO, because the free BigQuery Sandbox (no billing
-- account) blocks DML statements (INSERT/UPDATE/DELETE/MERGE) entirely --
-- CTAS is DDL and is allowed. Run this once, after schema.sql.

CREATE OR REPLACE TABLE `fraud_monitor.transactions_raw`
PARTITION BY DATE(transaction_ts)
CLUSTER BY customer_id
AS
WITH fx_lookup AS (
  SELECT * FROM UNNEST([
    STRUCT('US' AS country, 'USD' AS currency),
    ('GB','GBP'), ('DE','EUR'), ('IN','INR'),
    ('JP','JPY'), ('AU','AUD'), ('FR','EUR'), ('SG','USD')
  ])
),
merchants AS (
  SELECT * FROM UNNEST(['Amazon','Walmart','Target','BestBuy','Starbucks',
                         'Uber','Netflix','Apple','IKEA','Shell']) AS merchant
),
numbered AS (
  SELECT n FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS n
),
raw_gen AS (
  SELECT
    CONCAT('TXN', LPAD(CAST(n AS STRING), 6, '0')) AS transaction_id,
    CONCAT('CUST', LPAD(CAST(CAST(FLOOR(RAND() * 400) + 1 AS INT64) AS STRING), 4, '0')) AS customer_id,
    ROUND(EXP(RAND() * 4 + 2), 2) AS amount,
    (SELECT AS STRUCT * FROM fx_lookup ORDER BY RAND() LIMIT 1) AS loc,
    (SELECT merchant FROM merchants ORDER BY RAND() LIMIT 1) AS merchant,
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 30 * 24 * 60) AS INT64) MINUTE) AS transaction_ts,
    CASE WHEN RAND() < 0.05 THEN 'failed' ELSE 'success' END AS status,
    RAND() AS r1, RAND() AS r2, RAND() AS r3, RAND() AS r4, RAND() AS r5
  FROM numbered
),
base_txns AS (
  SELECT
    transaction_id,
    -- inject: ~1% missing customer_id
    CASE WHEN r1 < 0.01 THEN NULL ELSE customer_id END AS customer_id,
    -- inject: ~0.5% negative amounts
    CASE WHEN r2 < 0.005 THEN -amount ELSE amount END AS amount,
    -- inject: ~0.6% invalid currency code, ~1% currency/country mismatch
    CASE WHEN r3 < 0.006 THEN 'XYZ'
         WHEN r4 < 0.01 THEN 'EUR'
         ELSE loc.currency END AS currency,
    merchant,
    loc.country AS country,
    -- inject: ~0.3% future-dated transactions
    CASE WHEN r5 < 0.003
         THEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND() * 500) + 1 AS INT64) HOUR)
         ELSE transaction_ts END AS transaction_ts,
    status
  FROM raw_gen
)
-- main batch
SELECT *, CURRENT_TIMESTAMP() AS load_ts FROM base_txns
UNION ALL
-- inject: 15 duplicate transaction_ids (exact copies of existing rows --
-- same id, same everything, mirroring a real duplicate-ingestion bug)
SELECT *, CURRENT_TIMESTAMP() AS load_ts
FROM (SELECT * FROM base_txns ORDER BY RAND() LIMIT 15);

-- sanity check: row counts + a quick peek at the dirty rows
SELECT COUNT(*) AS total_rows,
       COUNTIF(customer_id IS NULL) AS missing_customer_id,
       COUNTIF(amount <= 0) AS bad_amounts,
       COUNTIF(transaction_ts > CURRENT_TIMESTAMP()) AS future_dated
FROM `fraud_monitor.transactions_raw`;
