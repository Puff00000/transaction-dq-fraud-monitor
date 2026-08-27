"""
completeness.py
----------------
Why this matters: a transaction we can't attribute to a customer (missing
customer_id) or that's missing a required field is unusable downstream —
it can't be scored for fraud, can't be tied to a customer baseline, and if
it leaks into reporting it silently understates volume or risk. This is
usually the FIRST thing a real data quality framework checks, because every
other check depends on having a valid key to group by.

Threshold, not zero-tolerance: a small amount of missingness is often
structural (e.g. guest checkouts) rather than a pipeline bug. We fail loud
only when it crosses a configured threshold (default 2%), and still log
the exact rows below that so they can be triaged.
"""

from .base import DQResult

REQUIRED_FIELDS = ["transaction_id", "customer_id", "amount",
                    "currency", "transaction_ts", "status"]


def check_missing_customer_id(rows, threshold_pct=2.0):
    affected = [r for r in rows if not r.get("customer_id")]
    return DQResult(
        check_name="completeness.missing_customer_id",
        severity="critical",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    ), affected


def check_missing_required_fields(rows, threshold_pct=1.0):
    affected = [r for r in rows if any(not r.get(f) for f in REQUIRED_FIELDS)]
    return DQResult(
        check_name="completeness.missing_required_fields",
        severity="critical",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    ), affected


def run_all(rows):
    results = []
    flagged_ids = set()
    for fn in (check_missing_customer_id, check_missing_required_fields):
        result, affected = fn(rows)
        results.append(result)
        flagged_ids.update(r["transaction_id"] for r in affected if r.get("transaction_id"))
    return results, flagged_ids
