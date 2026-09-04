"""
scorer.py
---------
Combines the output of every rule module into one fraud_risk_score per
transaction, and buckets it into Low/Medium/High. This is deliberately
RULE-BASED, weighted scoring rather than a trained ML model.

Why rule-based over ML (goes in the README verbatim too):
Explainability. In a real fraud/compliance context, an analyst (or a
regulator, or the cardholder disputing a decline) needs to know EXACTLY
why a transaction was flagged — "velocity: 7 transactions in 3 minutes" is
something you can act on, audit, and defend. A black-box ML model's score
alone can't answer "why" without a separate explainability layer (SHAP,
etc.), and even then it's a post-hoc approximation, not the actual reason.
Rule-based scoring makes the reason the SAME thing as the mechanism.

fraud_risk_score = sum of weights of every rule triggered for a
transaction (a transaction can trigger more than one rule, e.g. an
amount-anomaly transaction during a velocity burst scores higher than
either alone — this compounding is intentional).
"""

import csv
from collections import defaultdict

from . import velocity, amount_anomaly, geo_impossible, repeated_failures, odd_hour
from .base import bucket_for_score

RULE_MODULES = [velocity, amount_anomaly, geo_impossible, repeated_failures, odd_hour]


def score_transactions(rows):
    """Returns (scores: dict[transaction_id -> dict], all_flags: list[RuleFlag])"""
    all_flags = []
    for module in RULE_MODULES:
        all_flags.extend(module.detect(rows))

    flags_by_txn = defaultdict(list)
    for flag in all_flags:
        flags_by_txn[flag.transaction_id].append(flag)

    scores = {}
    for r in rows:
        txn_id = r["transaction_id"]
        txn_flags = flags_by_txn.get(txn_id, [])
        score = sum(f.weight for f in txn_flags)
        scores[txn_id] = {
            "transaction_id": txn_id,
            "customer_id": r.get("customer_id"),
            "amount": r.get("amount"),
            "fraud_risk_score": score,
            "risk_bucket": bucket_for_score(score),
            "triggered_rules": ";".join(sorted({f.rule_name for f in txn_flags})),
            "reasons": " | ".join(f.reason for f in txn_flags),
        }
    return scores, all_flags


def write_fraud_scores(scores, path="data/fraud_scores.csv"):
    if not scores:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "transaction_id", "customer_id", "amount", "fraud_risk_score",
            "risk_bucket", "triggered_rules", "reasons",
        ])
        writer.writeheader()
        writer.writerows(scores.values())


def main():
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from dq_checks.runner import load_rows

    # Fraud scoring runs on the CLEAN dataset (post-DQ-quarantine) so that
    # e.g. a row with a missing customer_id never even reaches scoring.
    try:
        rows = load_rows("data/transactions_clean.csv")
    except FileNotFoundError:
        print("data/transactions_clean.csv not found — run dq_checks/runner.py first.")
        rows = load_rows("data/transactions.csv")

    scores, all_flags = score_transactions(rows)
    write_fraud_scores(scores)

    bucket_counts = defaultdict(int)
    for s in scores.values():
        bucket_counts[s["risk_bucket"]] += 1

    print(f"Scored {len(scores)} transactions with {len(all_flags)} total rule triggers.\n")
    for bucket in ("Low", "Medium", "High"):
        print(f"  {bucket:8s}: {bucket_counts[bucket]}")

    top_flagged = sorted(scores.values(), key=lambda s: -s["fraud_risk_score"])[:10]
    print("\nTop 10 flagged transactions:")
    for s in top_flagged:
        print(f"  {s['transaction_id']}  score={s['fraud_risk_score']:>3}  "
              f"bucket={s['risk_bucket']:6s}  rules=[{s['triggered_rules']}]")

    print(f"\nWritten to data/fraud_scores.csv")


if __name__ == "__main__":
    main()
