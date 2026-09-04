"""
amount_anomaly.py
------------------
Why this matters: a $2,000 transaction is not inherently suspicious — for
some customers that's a normal grocery run for the month, for others it's
wildly out of character. Flagging against a GLOBAL average would either
miss high-spend customers' fraud or constantly false-positive on customers
who spend more than average. This is why the rule is built per-customer:
each customer is compared only against THEIR OWN historical baseline.

Rule: flag a transaction if its amount is more than 3 standard deviations
above that customer's OWN historical average, computed excluding the
current transaction (so a single huge transaction can't inflate its own
baseline and hide from the check). Requires at least 4 prior transactions
to establish a meaningful baseline; customers with too little history are
skipped (documented as a limitation in the README).
"""

import statistics
from collections import defaultdict
from datetime import datetime
from .base import RuleFlag, RULE_WEIGHTS

STD_DEV_THRESHOLD = 3
MIN_HISTORY_FOR_BASELINE = 4


def detect(rows):
    by_customer = defaultdict(list)
    for r in rows:
        if r.get("customer_id"):
            by_customer[r["customer_id"]].append(r)

    flags = []
    for cust_id, txns in by_customer.items():
        txns = sorted(txns, key=lambda r: datetime.fromisoformat(r["transaction_ts"]))
        amounts = [float(r["amount"]) for r in txns]

        for i, txn in enumerate(txns):
            history = amounts[:i]  # rolling, excludes current txn -- no leakage
            if len(history) < MIN_HISTORY_FOR_BASELINE:
                continue
            mean = statistics.mean(history)
            stdev = statistics.stdev(history) if len(history) > 1 else 0
            if stdev == 0:
                continue
            z = (amounts[i] - mean) / stdev
            if z > STD_DEV_THRESHOLD:
                flags.append(RuleFlag(
                    transaction_id=txn["transaction_id"],
                    rule_name="amount_anomaly",
                    weight=RULE_WEIGHTS["amount_anomaly"],
                    reason=f"amount {amounts[i]:.2f} is {z:.1f} std devs above "
                           f"{cust_id}'s rolling average of {mean:.2f} "
                           f"(n={len(history)} prior txns)",
                ))
    return flags
