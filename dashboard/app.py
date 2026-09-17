"""
app.py
------
Streamlit dashboard for the DQ + fraud monitoring pipeline. Reads from
BigQuery when credentials/env vars are configured, and transparently falls
back to the local CSVs (data/dq_issues.csv, data/fraud_scores.csv) so the
dashboard is runnable end-to-end with zero cloud setup -- important for a
quick "run it locally in 2 minutes" experience during an interview.

Three views, matching the brief exactly:
  1. DQ health/pass-rate trend over time (line chart)
  2. Fraud risk bucket distribution (bar chart: Low/Medium/High)
  3. Table of top flagged transactions with which rule(s) triggered
"""

import os
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Transaction DQ & Fraud Risk Monitor", layout="wide")


# --------------------------------------------------------------------
# Data loading -- BigQuery if configured, else local CSVs
# --------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_data():
    project = os.environ.get("GCP_PROJECT")
    if project:
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=project)
            dq_issues = client.query(
                f"SELECT * FROM `{project}.fraud_monitor.dq_issues` ORDER BY run_ts"
            ).to_dataframe()
            fraud_scores = client.query(
                f"SELECT * FROM `{project}.fraud_monitor.fraud_scores` ORDER BY fraud_risk_score DESC"
            ).to_dataframe()
            return dq_issues, fraud_scores, "BigQuery"
        except Exception as e:
            st.warning(f"Could not reach BigQuery ({e}). Falling back to local CSVs.")

    base = os.path.join(os.path.dirname(__file__), "..", "data")
    dq_issues = pd.read_csv(os.path.join(base, "dq_issues.csv"))
    fraud_scores = pd.read_csv(os.path.join(base, "fraud_scores.csv"))
    return dq_issues, fraud_scores, "local CSV"


dq_issues, fraud_scores, source = load_data()

st.title("Transaction Data Quality & Fraud Risk Monitor")
st.caption(f"Data source: {source} · Amex CFR portfolio project")

# --------------------------------------------------------------------
# Top-line KPIs
# --------------------------------------------------------------------
critical = dq_issues[dq_issues["severity"] == "critical"]
overall_pass = bool(critical["passed"].all()) if not critical.empty else True
total_txns = len(fraud_scores)
high_risk = (fraud_scores["risk_bucket"] == "High").sum() if total_txns else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Latest DQ batch status", "PASS" if overall_pass else "FAIL")
k2.metric("Transactions scored", f"{total_txns:,}")
k3.metric("High-risk transactions", f"{high_risk:,}")
k4.metric(
    "Critical checks passing",
    f"{int(critical['passed'].sum())}/{len(critical)}" if not critical.empty else "—",
)

st.divider()

# --------------------------------------------------------------------
# 1. DQ health / pass-rate trend
# --------------------------------------------------------------------
st.subheader("DQ health — pass rate over time")

dq_issues["run_ts"] = pd.to_datetime(dq_issues["run_ts"])
dq_issues["run_date"] = dq_issues["run_ts"].dt.date
trend = dq_issues.groupby("run_date")["passed"].mean().reset_index()
trend.columns = ["run_date", "pass_rate"]

if len(trend) > 1:
    st.line_chart(trend.set_index("run_date")["pass_rate"])
else:
    st.info(
        "Only one DQ run recorded so far — the trend line will build up as "
        "the DAG runs on subsequent days. Showing this run's per-check results instead:"
    )
    st.dataframe(
        dq_issues[["check_name", "severity", "affected_pct", "threshold_pct", "passed"]],
        use_container_width=True,
        hide_index=True,
    )

# --------------------------------------------------------------------
# 2. Fraud risk bucket distribution
# --------------------------------------------------------------------
st.subheader("Fraud risk bucket distribution")

bucket_order = ["Low", "Medium", "High"]
bucket_counts = (
    fraud_scores["risk_bucket"]
    .value_counts()
    .reindex(bucket_order, fill_value=0)
)
st.bar_chart(bucket_counts)

# --------------------------------------------------------------------
# 3. Top flagged transactions
# --------------------------------------------------------------------
st.subheader("Top flagged transactions")

min_score = st.slider(
    "Minimum fraud risk score", 0, int(fraud_scores["fraud_risk_score"].max() or 0), 25
)
top_n = st.number_input("Rows to show", min_value=5, max_value=200, value=25, step=5)

filtered = (
    fraud_scores[fraud_scores["fraud_risk_score"] >= min_score]
    .sort_values("fraud_risk_score", ascending=False)
    .head(top_n)
)

st.dataframe(
    filtered[["transaction_id", "customer_id", "amount", "fraud_risk_score",
              "risk_bucket", "triggered_rules", "reasons"]],
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Rule-based, weighted scoring (not ML) — every score above is fully "
    "explainable from `reasons`. See README for why explainability was "
    "chosen over a black-box model for this use case."
)
