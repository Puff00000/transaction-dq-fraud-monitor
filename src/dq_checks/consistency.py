"""
consistency.py
---------------
Why this matters: consistency checks catch values that are each
individually "valid" but don't make sense TOGETHER. A future-dated
transaction is a real timestamp, but a transaction can't happen before the
system processed it — usually a timezone bug or a clock skew on an
upstream service. A currency that doesn't match the country a transaction
was made in isn't necessarily wrong (foreign cards exist) but a high rate
of mismatches usually signals a mapping bug rather than genuine
cross-border spend, so we monitor the rate rather than flagging every row.
"""

from datetime import datetime, timezone
from .base import DQResult

COUNTRY_HOME_CURRENCY = {
    "US": "USD", "GB": "GBP", "DE": "EUR", "IN": "INR",
    "JP": "JPY", "AU": "AUD", "FR": "EUR", "SG": "USD",
}


def check_future_dated(rows, threshold_pct=0.3):
    now = datetime.now(timezone.utc)
    affected = []
    for r in rows:
        ts = _parse_ts(r.get("transaction_ts"))
        if ts and ts > now:
            affected.append(r)
    return DQResult(
        check_name="consistency.future_dated_transaction",
        severity="critical",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    ), affected


def check_currency_country_mismatch(rows, threshold_pct=1.0):
    affected = [
        r for r in rows
        if r.get("country") in COUNTRY_HOME_CURRENCY
        and r.get("currency") != COUNTRY_HOME_CURRENCY[r["country"]]
    ]
    return DQResult(
        check_name="consistency.currency_country_mismatch",
        severity="warning",
        affected_row_count=len(affected),
        total_row_count=len(rows),
        threshold_pct=threshold_pct,
    ), affected


def _parse_ts(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val)
    except ValueError:
        return None


def run_all(rows):
    results = []
    flagged_ids = set()
    for fn in (check_future_dated, check_currency_country_mismatch):
        result, affected = fn(rows)
        results.append(result)
        flagged_ids.update(r["transaction_id"] for r in affected if r.get("transaction_id"))
    return results, flagged_ids
