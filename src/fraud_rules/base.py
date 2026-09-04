"""
base.py
-------
Shared types for the fraud rules module. Each rule returns a set of
transaction_ids it flagged (plus a per-transaction reason string), and a
configured WEIGHT. The scorer sums (weight * triggered) per transaction
into fraud_risk_score, then buckets into Low/Medium/High.

Weights are a product/policy decision, not a data-science one — they're
kept here, in one place, specifically so they're easy to point to and
justify in an interview ("here's why card-testing outweighs odd-hour").
"""

from dataclasses import dataclass, field


@dataclass
class RuleFlag:
    transaction_id: str
    rule_name: str
    weight: float
    reason: str


RULE_WEIGHTS = {
    "velocity": 25,
    "amount_anomaly": 30,
    "geo_impossible_travel": 35,
    "repeated_failure_cardtesting": 30,
    "odd_hour": 10,  # weak signal by design — contributing factor only
}

RISK_BUCKETS = [
    (0, 24, "Low"),
    (25, 49, "Medium"),
    (50, 10_000, "High"),
]


def bucket_for_score(score: float) -> str:
    for low, high, label in RISK_BUCKETS:
        if low <= score <= high:
            return label
    return "High"
