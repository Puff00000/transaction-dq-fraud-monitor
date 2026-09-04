"""
velocity.py
-----------
Why this matters: a burst of many transactions from the same customer in a
short window is one of the oldest and most reliable fraud signals — it's
what a stolen card looks like in the hands of someone racing to spend it
before it gets blocked. Legitimate customers rarely transact more than a
couple of times in a few minutes.

Rule: flag a customer's transactions if they have MORE THAN N transactions
within an X-minute rolling window. Implemented with a simple sort +
sliding pointer per customer (O(n log n) overall) — the BigQuery version
of this same idea uses a self-join / window function, see
sql/fraud_rules/velocity.sql.
"""

from collections import defaultdict
from datetime import datetime
from .base import RuleFlag, RULE_WEIGHTS

N_THRESHOLD = 5          # more than 5 transactions...
WINDOW_MINUTES = 3        # ...within a 3-minute window


def _parse_ts(val):
    return datetime.fromisoformat(val)


def detect(rows):
    by_customer = defaultdict(list)
    for r in rows:
        if r.get("customer_id"):
            by_customer[r["customer_id"]].append(r)

    flags = []
    for cust_id, txns in by_customer.items():
        txns = sorted(txns, key=lambda r: _parse_ts(r["transaction_ts"]))
        left = 0
        for right in range(len(txns)):
            window_start = _parse_ts(txns[right]["transaction_ts"])
            while (window_start - _parse_ts(txns[left]["transaction_ts"])).total_seconds() \
                    > WINDOW_MINUTES * 60:
                left += 1
            window_size = right - left + 1
            if window_size > N_THRESHOLD:
                flags.append(RuleFlag(
                    transaction_id=txns[right]["transaction_id"],
                    rule_name="velocity",
                    weight=RULE_WEIGHTS["velocity"],
                    reason=f"{window_size} transactions by {cust_id} within "
                           f"{WINDOW_MINUTES} min (threshold: >{N_THRESHOLD})",
                ))
    return flags
