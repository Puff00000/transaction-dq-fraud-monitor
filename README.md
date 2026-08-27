# Transaction Data Quality & Fraud Risk Monitoring System

An end-to-end pipeline that checks incoming transaction data for quality
issues (completeness, uniqueness, validity, consistency) and scores
transactions for fraud risk using rule-based, weighted, explainable
logic.

## Problem statement

A payments/issuer pipeline that ingests transactions and scores them for
fraud is only as trustworthy as the data feeding it. Two failure modes
this project is built to catch, separately:

1. **Data quality failures** — a batch with a spike in missing customer
   IDs, duplicate transaction IDs, negative amounts, or future-dated
   timestamps shouldn't be silently fraud-scored anyway; the resulting
   scores would be meaningless and no one would know.
2. **Fraud** — even on clean data, transactions that look individually
   fine can form a risky pattern together: a burst of transactions in
   seconds, an amount wildly outside a customer's own history, a card
   used in two countries within an implausible window, or a cluster of
   failed charges consistent with card-testing.

This project builds a small but real system for both, with an explicit
gate between them: fraud scoring only runs on data that has already
passed data quality checks.

## Architecture

\`\`\`
synthetic data → extract_load_raw → run_dq_checks → dq_gate (branch)
                                                        │
                                    PASS ───────────────┼─────── FAIL
                                     ▼                              ▼
                          run_fraud_scoring                 dq_fail_alert
                                     │                              │
                                     └──────────► load_curated_tables
                                                        │
                                                        ▼
                                              refresh_dashboard_view
                                                        │
                                                        ▼
                                          Streamlit dashboard (DQ trend,
                                          risk buckets, top flagged txns)
\`\`\`

Full diagram with task-by-task rationale: [`docs/architecture.md`](docs/architecture.md).
BigQuery-specific design notes (partitioning, clustering, columnar
storage, serverless pricing): [`docs/bigquery_notes.md`](docs/bigquery_notes.md).

**Build order** (logic first, infrastructure last — see "Build
philosophy" below): synthetic data generator → standalone DQ checks
module → standalone fraud rules/scoring module → BigQuery schema + SQL
→ Airflow DAG → Streamlit dashboard.

## Repo structure

\`\`\`
transaction-dq-fraud-monitor/
├── data/
│   ├── synthetic_generator.py   # generates transactions.csv with injected issues
│   ├── transactions.csv          # 4,913 rows, ~400 customers
│   ├── transactions_clean.csv    # post-DQ-quarantine (fed to fraud scoring)
│   ├── transactions_ground_truth.csv  # which rows have which injected issue (validation only)
│   ├── dq_issues.csv             # output of the DQ check run
│   └── fraud_scores.csv          # output of the fraud scoring run
├── src/
│   ├── dq_checks/                # completeness, uniqueness, validity, consistency + runner
│   └── fraud_rules/              # velocity, amount_anomaly, geo_impossible, repeated_failures,
│                                  #   odd_hour + scorer
├── sql/
│   ├── schema.sql                # BigQuery DDL (partitioned + clustered tables)
│   ├── dq_checks/dq_checks.sql   # SQL twin of src/dq_checks/*.py
│   └── fraud_rules/fraud_rules.sql  # SQL twin of src/fraud_rules/*.py
├── dags/transaction_dq_fraud_dag.py  # Airflow orchestration
├── dashboard/app.py               # Streamlit dashboard
├── tests/                         # pytest unit tests for both Python modules
├── docs/                          # architecture + BigQuery design notes
├── requirements.txt
└── README.md
\`\`\`

## Build philosophy

This was built logic-first, infrastructure-last, on purpose: as a
fresher who might get an interview call on short notice, the goal isn't
just working code — it's being able to explain *every* decision. A
threshold, a rule weight, a window size is much easier to defend when it
lives in a five-line, independently-testable Python function than when
it's buried inside a DAG or a 200-line SQL script. So: synthetic data
with known-injected issues first → DQ checks and fraud rules as
standalone, unit-tested modules → BigQuery SQL twins once the logic was
proven → Airflow DAG to schedule it (pure orchestration, no business
logic of its own) → dashboard last.

## Data quality checks

| Dimension | Check | Severity | Default threshold |
|---|---|---|---|
| Completeness | missing `customer_id` | critical | 2.0% |
| Completeness | any required field missing | critical | 1.0% |
| Uniqueness | duplicate `transaction_id` | critical | 0.1% |
| Validity | negative or zero `amount` | critical | 1.0% |
| Validity | invalid currency code | warning | 0.5% |
| Consistency | future-dated transaction | critical | 0.3% |
| Consistency | currency/country mismatch | warning | 1.0% |

Each check writes one row to `dq_issues` (`check_name`, `severity`,
`affected_row_count`, `run_ts`, plus `affected_pct`/`threshold_pct`/
`passed` for auditability). **Thresholds, not zero-tolerance** — a small
amount of missingness or an occasional mismatch is often structural
(e.g. guest checkouts, a merchant integration quirk) rather than a
pipeline bug; the pipeline fails loud only when a critical check crosses
its configured line, and every row it deems "critical" gets quarantined
out of `transactions_clean` before fraud scoring ever sees it.

### Sample DQ report output

Actual output from `python src/dq_checks/runner.py` against the included
synthetic dataset (4,913 rows):

\`\`\`
  [PASS] completeness.missing_customer_id       severity=critical affected=  48 (0.977%) threshold=2.0%
  [PASS] completeness.missing_required_fields   severity=critical affected=  49 (0.997%) threshold=1.0%
  [FAIL] uniqueness.duplicate_transaction_id    severity=critical affected=  30 (0.611%) threshold=0.1%
  [PASS] validity.invalid_currency_code         severity=warning  affected=   6 (0.122%) threshold=0.5%
  [PASS] validity.negative_or_zero_amount       severity=critical affected=  24 (0.488%) threshold=1.0%
  [PASS] consistency.currency_country_mismatch  severity=warning  affected=  16 (0.326%) threshold=1.0%
  [PASS] consistency.future_dated_transaction   severity=critical affected=   8 (0.163%) threshold=0.3%

Overall batch DQ status: FAIL
Rows quarantined (critical issues): 95
Rows with warning-only issues (still scored for fraud): 15
\`\`\`

Note this run genuinely **fails**: 15 injected duplicate `transaction_id`
pairs (30 affected rows) cross the deliberately strict 0.1% threshold —
this is by design (duplicates directly corrupt money numbers in a
payments context, see the docstring in `src/dq_checks/uniqueness.py`),
and it's a real demonstration of the DQ gate doing its job rather than a
bug: in the Airflow DAG this exact result is what routes execution to
`dq_fail_alert` instead of `run_fraud_scoring`.

## Fraud rules (rule-based, weighted scoring — not ML)

| Rule | Weight | Signal |
|---|---|---|
| Velocity | 25 | >5 transactions by one customer within 3 minutes |
| Amount anomaly | 30 | amount > 3 std devs above *that customer's own* rolling average (excl. current txn, min. 4 prior txns) |
| Geo-impossible travel | 35 | two different-country transactions <120 min apart |
| Repeated failed transactions | 30 | ≥2 failed transactions within 2 min (card-testing pattern); a success landing within 2 min of the cluster is flagged too |
| Odd-hour transaction | 10 | transaction between 1–4am — weak signal, contributing factor only |

`fraud_risk_score` = sum of weights of every rule a transaction
triggers (a transaction can trigger several — this compounding is
intentional: an amount-anomaly transaction during a velocity burst
scores higher than either alone). Bucketed: **0–24 Low, 25–49 Medium,
50+ High.**

**Why rule-based over ML:** explainability. In a real fraud/compliance
context, an analyst — or a regulator, or the cardholder disputing a
decline — needs to know *exactly* why a transaction was flagged.
"Velocity: 7 transactions in 3 minutes" is something you can act on,
audit, and defend. A black-box ML model's score alone can't answer "why"
without a separate explainability layer (SHAP, etc.), and even then
that's a post-hoc approximation, not the actual mechanism. Rule-based
scoring makes the reason *the same thing* as the mechanism — every row
in `fraud_scores` carries its own `reasons` string.

### Sample fraud flags output

Top of `python -m src.fraud_rules.scorer` against the cleaned dataset
(4,803 transactions scored):

\`\`\`
  Low     : 4579
  Medium  : 221
  High    : 3

Top 10 flagged transactions:
  TXN0e5c73fe27b5  score= 85  bucket=High    rules=[amount_anomaly;repeated_failure_cardtesting;velocity]
  TXNdb6b7b7abbc4  score= 65  bucket=High    rules=[odd_hour;repeated_failure_cardtesting;velocity]
  TXNbe744f961565  score= 60  bucket=High    rules=[amount_anomaly;repeated_failure_cardtesting]
  TXN413f6e9c1676  score= 45  bucket=Medium  rules=[geo_impossible_travel;odd_hour]
  ...
\`\`\`

### Validated against known-injected fraud patterns

`transactions_ground_truth.csv` labels which rows the synthetic
generator deliberately made fraudulent (used only for validation, never
fed into the pipeline). Checking the scorer's output against it:

| Injected pattern | Caught by its matching rule |
|---|---|
| Velocity burst | 66 / 126 (52%) |
| Amount anomaly | 18 / 25 (72%) |
| Geo-impossible travel | 40 / 40 (100%) |
| Repeated-failure card-testing | 74 / 74 (100%) |
| Odd-hour | 28 / 28 (100%) |

Across all injected fraud patterns combined: **81% precision, 62%
recall** at the Medium/High bucket level. The lower velocity recall is
the most interesting limitation to be upfront about: the generator's
injected bursts don't always land >5 transactions inside the exact
3-minute sliding window the rule checks, which is a real property of
threshold-based rules — a burst just under the line is invisible to it.
That's a legitimate, explainable trade-off of rule-based detection
(and exactly the kind of thing worth discussing in an interview), not a
bug being hidden.

## SQL techniques used

- **Window functions** — `amount_anomaly`'s rolling per-customer mean/
  stddev (`PARTITION BY customer_id ORDER BY transaction_ts ROWS BETWEEN
  UNBOUNDED PRECEDING AND 1 PRECEDING`, excluding the current row so a
  huge transaction can't inflate its own baseline); `uniqueness`'s
  duplicate count via `COUNT(*) OVER (PARTITION BY transaction_id)`.
- **Self-joins** — `velocity` and `repeated_failures` join
  `transactions_clean` to itself on `customer_id` with a timestamp
  window; `geo_impossible_travel` joins each row to its immediately
  preceding transaction per customer via `ROW_NUMBER()`.
- **CTEs** throughout both `sql/dq_checks/dq_checks.sql` and
  `sql/fraud_rules/fraud_rules.sql` for readability — each CTE mirrors
  one Python check/rule function 1:1 so the two can be read side by
  side.

## How to run locally

\`\`\`bash
pip install -r requirements.txt

# 1. Generate synthetic data (already included, re-run to regenerate)
python data/synthetic_generator.py

# 2. Run DQ checks -> writes data/dq_issues.csv + data/transactions_clean.csv
python src/dq_checks/runner.py

# 3. Run fraud scoring -> writes data/fraud_scores.csv
python -m src.fraud_rules.scorer

# 4. Run the test suite
pytest tests/ -v

# 5. Launch the dashboard (reads local CSVs automatically if GCP_PROJECT
#    isn't set)
streamlit run dashboard/app.py
\`\`\`

To run against real BigQuery instead of local CSVs: apply
`sql/schema.sql`, load `transactions.csv` into `transactions_raw`, run
`sql/dq_checks/dq_checks.sql` then `sql/fraud_rules/fraud_rules.sql`,
and set `GCP_PROJECT` before launching the dashboard. The Airflow DAG
(`dags/transaction_dq_fraud_dag.py`) automates exactly that sequence on
a daily schedule, with the DQ pass/fail branch in between.

## Design decisions

- **Thresholds over zero-tolerance** on most checks (duplicates being
  the deliberate exception) — see the DQ checks table above.
- **Per-customer baselines, not global ones**, for amount anomaly — a
  $2,000 transaction is unremarkable for one customer and wildly
  anomalous for another; a global average would either miss high-spend
  fraud or constantly false-positive on legitimately high-spend
  customers.
- **Warning-severity DQ issues don't block fraud scoring; critical ones
  do** — a currency/country mismatch shouldn't hide a real fraud
  pattern, but a row with a missing key or a negative amount can't be
  trusted enough to score at all.
- **Weighted, additive, rule-based scoring over ML** — explainability
  (see "Why rule-based over ML" above).

## Future work

- Real-time streaming ingestion via Pub/Sub + Dataflow, replacing the
  daily batch DAG.
- A trained ML-based anomaly detection layer *alongside* (not replacing)
  the rule-based system — e.g. an isolation forest flagging patterns the
  fixed rules miss, with its output still routed through the same
  explainable rule-scoring layer before anything gets actioned.
- Distance-aware geo-impossible-travel (haversine distance / minimum
  plausible travel speed between the two transaction locations) instead
  of the current flat 120-minute threshold.
- Real alerting integration (Slack/PagerDuty) in place of the current
  logging stub in the `dq_fail_alert` task.
