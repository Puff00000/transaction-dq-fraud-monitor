"""
validity.py
-----------
Why this matters: values that exist but violate a basic domain rule
(negative amount, currency code that isn't a real ISO 4217 code) usually
mean either an upstream integration bug (e.g. a sign flip on refunds vs.
charges) or a parsing/encoding error. Left unchecked, negative amounts can
silently net against real revenue in aggregates, and invalid currency
codes break any downstream FX conversion.
"""

from .base import DQResult

VALID_CURRENCIES = {"USD", "EUR", "GBP", "INR", "JPY", "AUD"}


def check_negative_or_zero_amount(rows, threshold_pct=1.0):
    affected = [r for r in rows if _to_float(r.get("amount")) is not None
                and _to_float(r["amount"]) <= 0]
    return DQResult(
        check_name="validity.negative_or_zero_amount",
        severity="critical",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    ), affected


def check_invalid_currency(rows, threshold_pct=0.5):
    affected = [r for r in rows if r.get("currency", "").upper() not in VALID_CURRENCIES
                or r.get("currency", "") != r.get("currency", "").upper()]
    return DQResult(
        check_name="validity.invalid_currency_code",
        severity="warning",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    ), affected


def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def run_all(rows):
    results = []
    flagged_ids = set()
    for fn in (check_negative_or_zero_amount, check_invalid_currency):
        result, affected = fn(rows)
        results.append(result)
        flagged_ids.update(r["transaction_id"] for r in affected if r.get("transaction_id"))
    return results, flagged_ids
