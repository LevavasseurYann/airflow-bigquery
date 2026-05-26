"""Data quality — independent, post-build verification of the marts.

dbt already tests models *during* the build. This package adds a **separate**
quality gate the orchestrator runs *after* the build: a platform-level safety
net that can alert independently of dbt. See ``docs/data-quality.md``.
"""

from hr_pipeline.quality.checks import (
    CheckResult,
    QualityCheck,
    QualityReport,
    Severity,
    run_check,
    run_suite,
)
from hr_pipeline.quality.suite import build_marts_quality_suite

__all__ = [
    "CheckResult",
    "QualityCheck",
    "QualityReport",
    "Severity",
    "build_marts_quality_suite",
    "run_check",
    "run_suite",
]
