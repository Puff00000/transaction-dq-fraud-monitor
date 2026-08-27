"""
base.py
-------
Shared types for the data-quality module. Every check returns a DQResult so
the runner can write a uniform `dq_issues` table regardless of which check
produced the row. Keeping this dead simple (a dataclass, not a framework)
is deliberate — it's easy to explain in an interview and easy to port to
BigQuery SQL later.
"""

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class DQResult:
    check_name: str
    severity: str            # "critical" or "warning"
    affected_row_count: int
    total_row_count: int
    threshold_pct: float     # the % threshold that was configured for this check
    run_ts: str = None

    def __post_init__(self):
        if self.run_ts is None:
            self.run_ts = datetime.now(timezone.utc).isoformat()

    @property
    def affected_pct(self) -> float:
        if self.total_row_count == 0:
            return 0.0
        return round(100 * self.affected_row_count / self.total_row_count, 3)

    @property
    def passed(self) -> bool:
        return self.affected_pct <= self.threshold_pct

    def as_row(self):
        return {
            "check_name": self.check_name,
            "severity": self.severity,
            "affected_row_count": self.affected_row_count,
            "total_row_count": self.total_row_count,
            "affected_pct": self.affected_pct,
            "threshold_pct": self.threshold_pct,
            "passed": self.passed,
            "run_ts": self.run_ts,
        }
