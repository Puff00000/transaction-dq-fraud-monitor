"""
test_dq_checks.py
------------------
Each test constructs a tiny, hand-crafted set of rows with a KNOWN issue
baked in, and asserts the check catches exactly that issue -- no more, no
less. Deliberately not using the 5,000-row synthetic dataset here: small,
explicit fixtures make it obvious to a reader (or an interviewer) exactly
what each check is being proven to do.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.dq_checks import completeness, uniqueness, validity, consistency


def make_row(**overrides):
    row = {
        "transaction_id": "TXN1", "customer_id": "CUST1", "amount": "100.00",
        "currency": "USD", "merchant": "Amazon", "country": "US",
        "transaction_ts": "2025-03-01T10:00:00+00:00", "status": "success",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------- completeness
def test_missing_customer_id_flagged():
    rows = [make_row(transaction_id="TXN1", customer_id=""),
            make_row(transaction_id="TXN2")]
    result, affected = completeness.check_missing_customer_id(rows)
    assert result.affected_row_count == 1
    assert affected[0]["transaction_id"] == "TXN1"


def test_missing_customer_id_none_flagged_when_clean():
    rows = [make_row(transaction_id="TXN1"), make_row(transaction_id="TXN2")]
    result, affected = completeness.check_missing_customer_id(rows)
    assert result.affected_row_count == 0
    assert result.passed


def test_missing_required_field_amount():
    rows = [make_row(transaction_id="TXN1", amount="")]
    result, affected = completeness.check_missing_required_fields(rows)
    assert result.affected_row_count == 1


# ----------------------------------------------------------------- uniqueness
def test_duplicate_transaction_id_flags_all_copies():
    rows = [make_row(transaction_id="TXN1"), make_row(transaction_id="TXN1"),
            make_row(transaction_id="TXN2")]
    result, affected = uniqueness.check_duplicate_transaction_ids(rows)
    assert result.affected_row_count == 2  # both copies of TXN1, not just the "extra" one
    assert result.severity == "critical"


def test_no_duplicates_passes():
    rows = [make_row(transaction_id="TXN1"), make_row(transaction_id="TXN2")]
    result, affected = uniqueness.check_duplicate_transaction_ids(rows)
    assert result.affected_row_count == 0
    assert result.passed


# ------------------------------------------------------------------- validity
def test_negative_amount_flagged():
    rows = [make_row(transaction_id="TXN1", amount="-50.00"),
            make_row(transaction_id="TXN2", amount="0"),
            make_row(transaction_id="TXN3", amount="20.00")]
    result, affected = validity.check_negative_or_zero_amount(rows)
    assert result.affected_row_count == 2
    ids = {r["transaction_id"] for r in affected}
    assert ids == {"TXN1", "TXN2"}


def test_invalid_currency_code_flagged():
    rows = [make_row(transaction_id="TXN1", currency="XYZ"),
            make_row(transaction_id="TXN2", currency="USD")]
    result, affected = validity.check_invalid_currency(rows)
    assert result.affected_row_count == 1
    assert affected[0]["transaction_id"] == "TXN1"


# ---------------------------------------------------------------- consistency
def test_future_dated_transaction_flagged():
    rows = [make_row(transaction_id="TXN1", transaction_ts="2099-01-01T00:00:00+00:00"),
            make_row(transaction_id="TXN2")]
    result, affected = consistency.check_future_dated(rows)
    assert result.affected_row_count == 1
    assert affected[0]["transaction_id"] == "TXN1"


def test_currency_country_mismatch_flagged():
    rows = [make_row(transaction_id="TXN1", country="US", currency="EUR"),
            make_row(transaction_id="TXN2", country="US", currency="USD")]
    result, affected = consistency.check_currency_country_mismatch(rows)
    assert result.affected_row_count == 1
    assert affected[0]["transaction_id"] == "TXN1"


# ------------------------------------------------------------------- runner
def test_batch_passed_false_when_critical_check_breaches_threshold():
    from src.dq_checks.base import DQResult

    results = [
        DQResult("dummy.critical", "critical", affected_row_count=50,
                  total_row_count=100, threshold_pct=1.0),   # 50% >> 1% -> fails
        DQResult("dummy.warning", "warning", affected_row_count=50,
                  total_row_count=100, threshold_pct=1.0),   # warning breach doesn't block
    ]
    from src.dq_checks.runner import batch_passed
    assert batch_passed(results) is False


def test_batch_passed_true_when_only_warnings_breach():
    from src.dq_checks.base import DQResult
    from src.dq_checks.runner import batch_passed

    results = [
        DQResult("dummy.critical", "critical", affected_row_count=0,
                  total_row_count=100, threshold_pct=1.0),
        DQResult("dummy.warning", "warning", affected_row_count=50,
                  total_row_count=100, threshold_pct=1.0),
    ]
    assert batch_passed(results) is True
