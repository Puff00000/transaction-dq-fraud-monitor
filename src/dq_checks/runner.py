"""
runner.py
---------
Orchestrates all four DQ dimensions (completeness, uniqueness, validity,
consistency), writes a `dq_issues` table (one row per check, matching the
schema: check_name, severity, affected_row_count, run_ts), and decides
whether the batch as a whole PASSES or FAILS — this pass/fail decision is
exactly what the Airflow DAG's BranchPythonOperator will read to decide
whether to continue to fraud scoring or divert to an alert path.

Also produces `transactions_clean.csv`: rows that were NOT flagged by any
CRITICAL check. Rows flagged only by a WARNING-severity check still flow
into fraud scoring (e.g. a currency/country mismatch shouldn't block
fraud detection), but rows with a CRITICAL issue (missing key, duplicate,
negative amount, future timestamp) are quarantined — you can't reliably
fraud-score a row you can't trust the basic facts of.
"""

import csv
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dq_checks import completeness, uniqueness, validity, consistency  # noqa: E402

CHECK_MODULES = [completeness, uniqueness, validity, consistency]


def load_rows(path="data/transactions.csv"):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def run_all_checks(rows):
    """Runs every check function in every DQ module once, and returns:
    - all_results: list[DQResult], one per check (this becomes dq_issues)
    - critical_ids: transaction_ids flagged by any CRITICAL-severity check
    - warning_ids: transaction_ids flagged by any WARNING-severity check
    """
    all_results = []
    critical_ids, warning_ids = set(), set()

    for module in CHECK_MODULES:
        for check_fn_name in _check_fns(module):
            fn = getattr(module, check_fn_name)
            result, affected = fn(rows)
            all_results.append(result)
            ids = {r["transaction_id"] for r in affected if r.get("transaction_id")}
            if result.severity == "critical":
                critical_ids.update(ids)
            else:
                warning_ids.update(ids)

    return all_results, critical_ids, warning_ids


def _check_fns(module):
    return [name for name in dir(module) if name.startswith("check_")]


def write_dq_issues(results, path="data/dq_issues.csv"):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "check_name", "severity", "affected_row_count", "total_row_count",
            "affected_pct", "threshold_pct", "passed", "run_ts",
        ])
        writer.writeheader()
        for r in results:
            writer.writerow(r.as_row())


def write_clean_dataset(rows, critical_ids, path="data/transactions_clean.csv"):
    clean_rows = [r for r in rows if r.get("transaction_id") not in critical_ids]
    if not clean_rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(clean_rows[0].keys()))
        writer.writeheader()
        writer.writerows(clean_rows)


def batch_passed(results) -> bool:
    """The whole batch fails if ANY critical check breaches its threshold.
    Warning-severity breaches are logged but don't block the pipeline —
    this mirrors how real DQ frameworks distinguish "stop the pipeline"
    issues from "flag for review" issues."""
    return all(r.passed for r in results if r.severity == "critical")


def main():
    rows = load_rows()
    results, critical_ids, warning_ids = run_all_checks(rows)
    write_dq_issues(results)
    write_clean_dataset(rows, critical_ids)

    print(f"Ran {len(results)} DQ checks across "
          f"{len({m.__name__ for m in CHECK_MODULES})} dimensions on {len(rows)} rows.\n")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.check_name:45s} severity={r.severity:8s} "
              f"affected={r.affected_row_count:4d} ({r.affected_pct}%) "
              f"threshold={r.threshold_pct}%")

    overall = batch_passed(results)
    print(f"\nOverall batch DQ status: {'PASS' if overall else 'FAIL'}")
    print(f"Rows quarantined (critical issues): {len(critical_ids)}")
    print(f"Rows with warning-only issues (still scored for fraud): {len(warning_ids - critical_ids)}")
    print(f"Clean rows written to data/transactions_clean.csv: {len(rows) - len(critical_ids)}")

    return overall


if __name__ == "__main__":
    passed = main()
    sys.exit(0 if passed else 1)
