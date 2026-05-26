"""The data quality engine — run checks, collect results, decide pass/fail.

**The check contract**: every :class:`QualityCheck` carries a SQL statement
that returns exactly one integer — the count of *failing* rows. ``0`` means the
check passed. This is intentionally the same convention dbt tests use, so the
mental model carries over between the two repos.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

from hr_pipeline.warehouse import Warehouse

logger = logging.getLogger(__name__)


class Severity(StrEnum):
    """How a failed check is treated by the orchestrator."""

    ERROR = "error"  # a failure fails the task (and the DAG)
    WARN = "warn"  # a failure is logged but the task still succeeds


@dataclass(frozen=True, slots=True)
class QualityCheck:
    """A single, named data quality assertion."""

    name: str
    description: str
    sql: str
    severity: Severity = Severity.ERROR


@dataclass(frozen=True, slots=True)
class CheckResult:
    """The outcome of running one :class:`QualityCheck`."""

    name: str
    description: str
    severity: Severity
    failing_rows: int
    passed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "severity": self.severity.value,
            "failing_rows": self.failing_rows,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class QualityReport:
    """Aggregated outcome of a whole suite."""

    results: list[CheckResult]

    @property
    def passed(self) -> bool:
        """True when every check passed."""
        return all(result.passed for result in self.results)

    @property
    def passed_count(self) -> int:
        """Number of checks that passed."""
        return sum(1 for r in self.results if r.passed)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        """Failed ERROR-severity checks — these must fail the DAG."""
        return [r for r in self.results if not r.passed and r.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        """Failed WARN-severity checks — surfaced but non-blocking."""
        return [r for r in self.results if not r.passed and r.severity is Severity.WARN]

    def has_blocking_failures(self) -> bool:
        return bool(self.blocking_failures)

    def summary(self) -> str:
        """A human-readable one-paragraph summary for logs and alerts."""
        lines = [
            f"Data quality: {self.passed_count}/{len(self.results)} checks passed.",
        ]
        for result in self.results:
            if not result.passed:
                lines.append(
                    f"  - [{result.severity.value.upper()}] {result.name}: "
                    f"{result.failing_rows} failing row(s) — {result.description}"
                )
        return "\n".join(lines)

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "total": len(self.results),
            "blocking_failures": len(self.blocking_failures),
            "warnings": len(self.warnings),
            "results": [r.as_dict() for r in self.results],
        }


def run_check(warehouse: Warehouse, check: QualityCheck) -> CheckResult:
    """Execute one check against the warehouse."""
    raw = warehouse.fetch_scalar(check.sql)
    failing_rows = int(raw or 0)
    return CheckResult(
        name=check.name,
        description=check.description,
        severity=check.severity,
        failing_rows=failing_rows,
        passed=failing_rows == 0,
    )


def run_suite(warehouse: Warehouse, checks: list[QualityCheck]) -> QualityReport:
    """Execute every check and return an aggregated :class:`QualityReport`."""
    results: list[CheckResult] = []
    for check in checks:
        result = run_check(warehouse, check)
        if result.passed:
            logger.info("PASS  %s", check.name)
        elif check.severity is Severity.ERROR:
            logger.error("FAIL  %s — %s failing row(s)", check.name, result.failing_rows)
        else:
            logger.warning("WARN  %s — %s failing row(s)", check.name, result.failing_rows)
        results.append(result)
    return QualityReport(results=results)
