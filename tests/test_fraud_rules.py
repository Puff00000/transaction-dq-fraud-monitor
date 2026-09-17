"""
test_fraud_rules.py
--------------------
Same philosophy as test_dq_checks.py: small, hand-crafted fixtures with a
known pattern baked in, asserting each rule fires (or deliberately does
NOT fire) exactly as designed.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fraud_rules import velocity, amount_anomaly, geo_impossible, repeated_failures, odd_hour
from src.fraud_rules.scorer import score_transactions
from src.fraud_rules.base import bucket_for_score


def make_row(**overrides):
    row = {
        "transaction_id": "TXN1", "customer_id": "CUST1", "amount": "50.00",
        "currency": "USD", "merchant": "Amazon", "country": "US",
        "transaction_ts": "2025-03-01T10:00:00", "status": "success",
    }
    row.update(overrides)
    return row


# -------------------------------------------------------------------- velocity
def test_velocity_flags_burst():
    rows = [
        make_row(transaction_id=f"TXN{i}", transaction_ts=f"2025-03-01T10:00:{i*10:02d}")
        for i in range(6)  # 6 transactions within 60 seconds -> >5 in 3-min window
    ]
    flags = velocity.detect(rows)
    assert len(flags) >= 1
    assert flags[0].rule_name == "velocity"


def test_velocity_does_not_flag_spread_out_transactions():
    rows = [
        make_row(transaction_id="TXN1", transaction_ts="2025-03-01T10:00:00"),
        make_row(transaction_id="TXN2", transaction_ts="2025-03-01T11:00:00"),
        make_row(transaction_id="TXN3", transaction_ts="2025-03-01T12:00:00"),
    ]
    assert velocity.detect(rows) == []


# ---------------------------------------------------------------- amount anomaly
def test_amount_anomaly_flags_outlier_against_own_baseline():
    # small variance in the baseline (not all identical) so stdev != 0 --
    # a zero-stdev baseline
