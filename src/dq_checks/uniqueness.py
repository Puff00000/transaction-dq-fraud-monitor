"""
uniqueness.py
-------------
Why this matters: a duplicate transaction_id means a transaction could get
counted twice in revenue/volume reporting, or — worse — scored twice for
fraud, which would double-count a customer's velocity and could either
create false positives or (if dedup happens inconsistently across systems)
let a real duplicate-charge bug hide in plain sight. Payments pipelines
treat this as a critical check because duplicates directly corrupt money
numbers, not just data quality metrics.

Zero-tolerance is arguably correct for this one in production, but we keep
a small threshold (default 0.1%) so a single one-off replay doesn't page
someone at 2am — it still shows up as a failed check either way.
"""

from collections import Counter
from .base import DQResult


def check_duplicate_transaction_ids(rows, threshold_pct=0.1):
    ids = [r["transaction_id"] for r in rows if r.get("transaction_id")]
    counts = Counter(ids)
    dup_ids = {tid for tid, c in counts.items() if c > 1}
    # affected_row_count = every row that shares a duplicated id (not just the extras)
    affected = [r for r in rows if r.get("transaction_id") in dup_ids]
    result = DQResult(
        check_name="uniqueness.duplicate_transaction_id",
        severity="critical",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    )
    return result, affected


def run_all(rows):
    results = []
    flagged_ids = set()
    result, affected = check_duplicate_transaction_ids(rows)
    results.append(result)
    flagged_ids.update(r["transaction_id"] for r in affected)
    return results, flagged_ids
