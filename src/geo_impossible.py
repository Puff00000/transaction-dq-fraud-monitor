"""
geo_impossible.py
------------------
Why this matters: this is the classic "impossible travel" check used
across fraud and account-security systems (payments, login-risk, etc.) —
if a customer transacts in Country A and then, implausibly soon after,
in Country B, either the card is being used in two places at once (cloned
card / stolen card details) or credentials have been compromised. It's a
strong signal specifically BECAUSE it doesn't rely on amount or velocity
at all — it catches fraud that would otherwise look completely normal.

Rule: for each customer, compare every pair of consecutive transactions.
If the country differs AND the time gap is below a plausibility threshold
for that country pair, flag BOTH transactions. Real systems use flight-time
matrices between country pairs; we use a simplified single threshold
(120 minutes) with a note in the README that a distance-aware version
(haversine distance / min plausible travel speed) is future work.
"""

from collections import defaultdict
from datetime import datetime
from .base import RuleFlag, RULE_WEIGHTS

IMPLAUSIBLE_GAP_MINUTES = 120


def detect(rows):
    by_customer = defaultdict(list)
    for r in rows:
        if r.get("customer_id"):
            by_customer[r["customer_id"]].append(r)

    flags = []
    for cust_id, txns in by_customer.items():
        txns = sorted(txns, key=lambda r: datetime.fromisoformat(r["transaction_ts"]))
        for i in range(1, len(txns)):
            prev, curr = txns[i - 1], txns[i]
            if prev["country"] == curr["country"]:
                continue
            gap_minutes = (
                datetime.fromisoformat(curr["transaction_ts"])
                - datetime.fromisoformat(prev["transaction_ts"])
            ).total_seconds() / 60
            if 0 <= gap_minutes < IMPLAUSIBLE_GAP_MINUTES:
                reason = (f"{cust_id} transacted in {prev['country']} then "
                          f"{curr['country']} only {gap_minutes:.0f} min apart "
                          f"(threshold: <{IMPLAUSIBLE_GAP_MINUTES} min)")
                flags.append(RuleFlag(prev["transaction_id"], "geo_impossible_travel",
                                       RULE_WEIGHTS["geo_impossible_travel"], reason))
                flags.append(RuleFlag(curr["transaction_id"], "geo_impossible_travel",
                                       RULE_WEIGHTS["geo_impossible_travel"], reason))
    return flags
