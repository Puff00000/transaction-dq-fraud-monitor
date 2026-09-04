"""
odd_hour.py
-----------
Why this matters, and why it's WEAK on its own: fraud does skew toward
late-night/early-morning hours (when a cardholder is asleep and least
likely to notice or react), but plenty of legitimate transactions happen
overnight too (night-shift workers, different time zones, insomniac online
shopping). Treating this as a standalone fraud flag would generate a lot
of noise. Its real value is as a CONTRIBUTING factor — it nudges the
weighted score up modestly, but is deliberately given the lowest weight of
any rule, and should never be the sole reason a transaction reaches a High
bucket on its own.

Rule: flag transactions between 1am-4am LOCAL to the transaction (we use
the transaction_ts as given, i.e. assume it's already in a customer-local
or otherwise consistent reference frame — a real system would need proper
timezone handling per country, noted as future work in the README).
"""

from datetime import datetime
from .base import RuleFlag, RULE_WEIGHTS

ODD_HOURS = {1, 2, 3, 4}


def detect(rows):
    flags = []
    for r in rows:
        ts = datetime.fromisoformat(r["transaction_ts"])
        if ts.hour in ODD_HOURS:
            flags.append(RuleFlag(
                transaction_id=r["transaction_id"],
                rule_name="odd_hour",
                weight=RULE_WEIGHTS["odd_hour"],
                reason=f"transaction at {ts.hour:02d}:00 local (odd-hour window: "
                       f"{min(ODD_HOURS)}-{max(ODD_HOURS)}am) — weak signal, "
                       f"contributing factor only",
            ))
    return flags
