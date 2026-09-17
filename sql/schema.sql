-- schema.sql
-- BigQuery DDL for the project. Run once to set up the dataset.
--
-- Design notes (also in docs/bigquery_notes.md):
-- - `transactions_raw` is partitioned by DATE(transaction_ts) and clustered
--   by customer_id. Every query in this project either filters by date
--   range or groups by customer, so this partition+cluster combo means
--   BigQuery skips scanning irrelevant partitions/blocks entirely --
--   directly reducing bytes scanned and therefore cost, since BigQuery's
--   on-demand pricing bills per byte scanned (the "columnar + serverless"
--   pricing model this project is meant to demonstrate understanding of).
-- - dq_issues and fraud_scores are small, append-only "results" tables --
--   not partitioned, since they're cheap to scan in full.

CREATE SCHEMA IF NOT EXISTS `fraud_monitor`;

CREATE TABLE IF NOT EXISTS `fraud_monitor.transactions_raw` (
  transaction_id  STRING,
  customer_id     STRING,
  amount          NUMERIC,
  currency        STRING,
  merchant        STRING,
  country         STRING,
  transaction_ts  TIMESTAMP,
  status          STRING,
  load_ts         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(transaction_ts)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `fraud_monitor.transactions_clean` (
  transaction_id  STRING,
  customer_id     STRING,
  amount          NUMERIC,
  currency        STRING,
  merchant        STRING,
  country         STRING,
  transaction_ts  TIMESTAMP,
  status          STRING
)
PARTITION BY DATE(transaction_ts)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `fraud_monitor.dq_issues` (
  check_name          STRING,
  severity            STRING,   -- 'critical' | 'warning'
  affected_row_count  INT64,
  total_row_count     INT64,
  affected_pct        NUMERIC,
  threshold_pct       NUMERIC,
  passed              BOOL,
  run_ts              TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `fraud_monitor.fraud_scores` (
  transaction_id    STRING,
  customer_id       STRING,
  amount            NUMERIC,
  fraud_risk_score  NUMERIC,
  risk_bucket       STRING,     -- 'Low' | 'Medium' | 'High'
  triggered_rules   STRING,     -- semicolon-joined rule names
  reasons           STRING,
  run_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
