"""
repeated_failures.py
---------------------
Why this matters: this is a "card-testing" fraud pattern — someone with a
list of stolen card numbers runs small/rapid charges against each one to
find which cards are still active and unblocked, before using the working
ones for real fraud elsewhere. The signature is a cluster of FAILED
transactions in quick succession, often followed by a SUCCESS once a
working combination is found. This pattern is specifically valuable in a
payments/issuer context (like Amex's CFR team) because it's often the
*earliest* observable signal of a card being compromised — before any
large fraudulent charge has actually gone through.

Rule: flag a customer if they have >= N failed transactions within an
X-minute window (regardless of whether a success follows), since even the
failed attempts represent risk exposure. If a success closely follows the
failed cluster, that success is flagged too (as the fraud likely "landed").
"""

from collections import defaultdict
from datetime import datetime
from .base import RuleFlag, RULE_WEIGHTS

MIN_FAILURES = 2
WINDOW_MINUTES = 2
SUCCESS_FOLLOWUP_MINUTES = 2


def detect(rows):
    by_customer = defaultdict(list)
    for r in rows:
        if r.get("customer_id"):
            by_customer[r["customer_id"]].append(r)

    flags = []
    for cust_id, txns in by_customer.items():
        txns = sorted(txns, key=lambda r: datetime.fromisoformat(r["transaction_ts"]))

        i = 0
        while i < len(txns):
            if txns[i]["status"] != "failed":
                i += 1
                continue
            cluster = [txns[i]]
            j = i + 1
            while j < len(txns):
                gap = (datetime.fromisoformat(txns[j]["transaction_ts"])
                       - datetime.fromisoformat(cluster[-1]["transaction_ts"])).total_seconds() / 60
                if txns[j]["status"] == "failed" and gap <= WINDOW_MINUTES:
                    cluster.append(txns[j])
                    j += 1
                else:
                    break

            if len(cluster) >= MIN_FAILURES:
                reason = (f"{len(cluster)} failed transactions by {cust_id} within "
                          f"{WINDOW_MINUTES} min windows (card-testing pattern, "
                          f"threshold: >={MIN_FAILURES})")
                for txn in cluster:
                    flags.append(RuleFlag(txn["transaction_id"], "repeated_failure_cardtesting",
                                           RULE_WEIGHTS["repeated_failure_cardtesting"], reason))
                # check for a success shortly after the cluster -> likely the fraud landing
                if j < len(txns) and txns[j]["status"] == "success":
                    gap = (datetime.fromisoformat(txns[j]["transaction_ts"])
                           - datetime.fromisoformat(cluster[-1]["transaction_ts"])).total_seconds() / 60
                    if gap <= SUCCESS_FOLLOWUP_MINUTES:
                        flags.append(RuleFlag(
                            txns[j]["transaction_id"], "repeated_failure_cardtesting",
                            RULE_WEIGHTS["repeated_failure_cardtesting"],
                            f"success followed a {len(cluster)}-failure card-testing "
                            f"cluster within {gap:.0f} min"))
                i = j
            else:
                i += 1
    return flags
