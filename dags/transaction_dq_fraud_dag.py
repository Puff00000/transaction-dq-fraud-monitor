"""
transaction_dq_fraud_dag.py
---------------------------
Orchestrates the whole pipeline. This is deliberately the LAST thing built
in this project (see README "Build philosophy") -- by the time this DAG
exists, every task it calls is already a standalone, independently-tested
Python function. The DAG's only job is sequencing, branching, and
scheduling; it contains no business logic of its own.

Shape (matches the brief exactly):
  extract_load_raw >> run_dq_checks >> [dq_pass_branch, dq_fail_alert]
                                     >> run_fraud_scoring >> load_curated_tables
                                     >> refresh_dashboard_view

Why BranchPythonOperator here specifically: a pipeline that always marches
forward regardless of what it finds is a liability in a fraud/compliance
context -- if the DQ checks find that, say, 8% of rows have missing
customer_id (way over threshold), scoring fraud on that batch anyway would
produce a fraud_scores table nobody should trust, silently. Branching means
a bad batch visibly stops and alerts a human instead of quietly polluting
the dashboard. This is "pipeline reliability thinking," not just
happy-path orchestration.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DAG_ID = "transaction_dq_fraud_monitor"
GCP_PROJECT = "{{ var.value.get('gcp_project', 'your-gcp-project') }}"
BQ_DATASET = "fraud_monitor"

default_args = {
    "owner": "samriddhi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    description="Transaction data quality checks + rule-based fraud risk scoring",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "data-quality", "cfr"],
) as dag:

    # ------------------------------------------------------------------
    # 1. EXTRACT_LOAD_RAW -- loads the day's transactions into
    #    transactions_raw. In this portfolio project the "source" is the
    #    synthetic generator's CSV; in production this would be a load
    #    job from a payments event stream / upstream table.
    # ------------------------------------------------------------------
    extract_load_raw = BigQueryInsertJobOperator(
        task_id="extract_load_raw",
        configuration={
            "load": {
                "sourceUris": ["gs://{{ var.value.get('gcs_bucket', 'your-bucket') }}"
                               "/transactions/{{ ds }}/transactions.csv"],
                "destinationTable": {
                    "projectId": GCP_PROJECT, "datasetId": BQ_DATASET,
                    "tableId": "transactions_raw",
                },
                "sourceFormat": "CSV", "skipLeadingRows": 1,
                "writeDisposition": "WRITE_APPEND",
                "timePartitioning": {"type": "DAY", "field": "transaction_ts"},
            }
        },
    )

    # ------------------------------------------------------------------
    # 2. RUN_DQ_CHECKS -- executes sql/dq_checks/dq_checks.sql, which
    #    inserts one row per check into dq_issues (see that file for the
    #    check-by-check SQL). This task itself just fires the query;
    #    the branch task below reads its result to decide pass/fail.
    # ------------------------------------------------------------------
    run_dq_checks = BigQueryInsertJobOperator(
        task_id="run_dq_checks",
        configuration={
            "query": {
                "query": "{% include 'sql/dq_checks/dq_checks.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # ------------------------------------------------------------------
    # 3. BRANCH -- reads the just-written dq_issues rows for today's
    #    run_ts and checks whether any CRITICAL check breached its
    #    threshold (mirrors src/dq_checks/runner.py's batch_passed()
    #    logic exactly, just re-implemented against BigQuery instead of
    #    an in-memory list of DQResult objects).
    # ------------------------------------------------------------------
    def _check_dq_gate(**context):
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT)
        query = f"""
            SELECT LOGICAL_AND(passed) AS all_critical_passed
            FROM `{GCP_PROJECT}.{BQ_DATASET}.dq_issues`
            WHERE severity = 'critical'
              AND DATE(run_ts) = '{{{{ ds }}}}'
        """
        result = list(client.query(query).result())
        all_passed = bool(result[0]["all_critical_passed"]) if result else False
        return "run_fraud_scoring" if all_passed else "dq_fail_alert"

    dq_gate = BranchPythonOperator(
        task_id="dq_gate",
        python_callable=_check_dq_gate,
    )

    # ------------------------------------------------------------------
    # 4a. RUN_FRAUD_SCORING (dq_pass_branch) -- only reached if every
    #     critical DQ check passed its threshold. Executes
    #     sql/fraud_rules/fraud_rules.sql against transactions_clean.
    # ------------------------------------------------------------------
    run_fraud_scoring = BigQueryInsertJobOperator(
        task_id="run_fraud_scoring",
        configuration={
            "query": {
                "query": "{% include 'sql/fraud_rules/fraud_rules.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # ------------------------------------------------------------------
    # 4b. DQ_FAIL_ALERT -- the other branch. In production this would
    #     page/Slack the data-quality on-call; kept as a stub here since
    #     alerting integration is out of scope for this project's resume
    #     claim (documented explicitly in README's scope notes).
    # ------------------------------------------------------------------
    def _alert(**context):
        print(f"DQ GATE FAILED for run {context['ds']} -- fraud scoring skipped. "
              f"A critical check breached its threshold; see dq_issues table.")

    dq_fail_alert = PythonOperator(
        task_id="dq_fail_alert",
        python_callable=_alert,
    )

    # ------------------------------------------------------------------
    # 5. LOAD_CURATED_TABLES -- only runs after fraud scoring succeeds;
    #    materializes the small "curated" views the dashboard reads
    #    (kept as separate tables/views rather than querying raw tables
    #    live, so the dashboard stays fast and cheap to query).
    # ------------------------------------------------------------------
    load_curated_tables = BigQueryInsertJobOperator(
        task_id="load_curated_tables",
        trigger_rule="none_failed_min_one_success",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.dq_health_trend` AS
                    SELECT DATE(run_ts) AS run_date,
                           SAFE_DIVIDE(COUNTIF(passed), COUNT(*)) AS pass_rate
                    FROM `{GCP_PROJECT}.{BQ_DATASET}.dq_issues`
                    GROUP BY run_date;

                    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.fraud_bucket_distribution` AS
                    SELECT DATE(run_ts) AS run_date, risk_bucket, COUNT(*) AS n
                    FROM `{GCP_PROJECT}.{BQ_DATASET}.fraud_scores`
                    GROUP BY run_date, risk_bucket;
                """,
                "useLegacySql": False,
            }
        },
    )

    # ------------------------------------------------------------------
    # 6. REFRESH_DASHBOARD_VIEW -- final no-op marker task the Streamlit
    #    app's "last refreshed" indicator can key off of (or, in a fuller
    #    setup, a Cloud Run redeploy hook / cache-invalidation call).
    # ------------------------------------------------------------------
    refresh_dashboard_view = EmptyOperator(task_id="refresh_dashboard_view")

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------
    extract_load_raw >> run_dq_checks >> dq_gate
    dq_gate >> run_fraud_scoring >> load_curated_tables
    dq_gate >> dq_fail_alert >> load_curated_tables
    load_curated_tables >> refresh_dashboard_view
