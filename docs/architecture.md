# Architecture

## Pipeline diagram


## Why this shape

**Logic before infrastructure.** Every DQ check and fraud rule exists
first as a plain, standalone Python function (`src/dq_checks/`,
`src/fraud_rules/`) that can be unit tested with a five-line fixture and
no cloud dependency at all (`tests/`). Only once that logic is proven
does it get a BigQuery SQL twin (`sql/`) and an Airflow DAG to schedule
it. This ordering is deliberate: it means every design decision (a
threshold, a weight, a window size) can be explained and defended on its
own, independent of the orchestration layer around it.

**The DQ gate blocks fraud scoring on purpose.** A pipeline that always
marches forward regardless of what it finds is a liability in a
fraud/compliance context. If `dq_issues` shows a critical check breached
its threshold, fraud-scoring that batch anyway would produce a
`fraud_scores` table nobody should trust -- silently. The
`BranchPythonOperator` makes a bad batch visibly stop and alert instead
of quietly polluting the dashboard.

**Warning-severity issues don't block, critical ones do.** Within a
single batch, rows with only a warning-level issue (e.g. a
currency/country mismatch) still flow into fraud scoring -- a minor
inconsistency shouldn't hide a real fraud pattern. Rows with a
critical issue (missing key, duplicate ID, negative amount, future
timestamp) are quarantined into `transactions_clean` before scoring even
starts, because you can't reliably fraud-score a row you can't trust the
basic facts of.

**Dashboard reads curated tables, not raw ones.** `load_curated_tables`
materializes small, pre-aggregated tables (`dq_health_trend`,
`fraud_bucket_distribution`) specifically so the Streamlit app stays fast
and cheap to query -- it never scans `transactions_raw` directly.

## Scope notes (what this project deliberately does NOT do)

- No real-time streaming (batch/daily only) -- noted as future work.
- No real alerting integration (Slack/PagerDuty) -- `dq_fail_alert` is a
  stub that logs; wiring a real notifier is out of scope for the resume
  claim this project supports.
- No trained ML model -- rule-based scoring is a deliberate choice, not
  a limitation; see the README's "why rule-based over ML" section.
- Geo-impossible-travel uses a single flat time threshold, not a
  distance-aware flight-time matrix -- documented in the fraud rules
  README section as a simplification.
