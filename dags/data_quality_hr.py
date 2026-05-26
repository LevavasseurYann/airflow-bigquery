"""DAG ``data_quality_hr`` — an independent quality gate on the HR marts.

dbt already tests models *during* the build. This DAG is deliberately
**separate**: a platform-level safety net that re-verifies the published marts
*after* the fact and records every run. It is the kind of independent control a
data platform team owns on top of what each dbt project does for itself.

Airflow 3 features on display:

* **asset-driven scheduling** — runs as soon as ``transform_hr_dbt`` publishes
  ``HR_MARTS``;
* a **custom operator** — :class:`DataQualityCheckOperator` — one task per
  check, generated from the suite at parse time;
* an aggregating reporter task that runs on ``trigger_rule="all_done"`` so it
  reports even when (especially when) a check has failed.
"""

from __future__ import annotations

import logging
import os

from airflow.sdk import dag, get_current_context, task

from common import DEFAULT_ARGS, DEFAULT_START_DATE
from hr_pipeline.assets import HR_MARTS
from hr_pipeline.config import Settings
from hr_pipeline.operators import DataQualityCheckOperator
from hr_pipeline.quality import build_marts_quality_suite
from hr_pipeline.quality.checks import CheckResult, QualityReport, Severity
from hr_pipeline.warehouse import get_warehouse

logger = logging.getLogger(__name__)

# The suite is constructed once at parse time (its SQL is static — no
# time-sensitive values are baked in). The freshness check uses
# CURRENT_TIMESTAMP inside its SQL, so it is always evaluated relative to when
# the check task actually executes.
# os.getenv() is safe at parse time; Settings.from_env() is not needed here
# because marts_schema has no other parse-time dependencies.
_QUALITY_SUITE = build_marts_quality_suite(
    marts_schema=os.getenv("BQ_DATASET_MARTS", "marts"),
)

_DOC = """
### `data_quality_hr`

Runs the HR marts quality suite — uniqueness, referential integrity, business
rules (no future hires), freshness — one Airflow task per check.

* **Schedule** — asset-driven on `hr_marts`.
* **Severity** — `ERROR` checks fail the DAG; `WARN` checks only annotate it.
* **Audit trail** — every run is appended to `quality.check_runs` in the
  warehouse.
"""


def _result_from_xcom(check_name: str, payload: dict | None, suite_lookup: dict) -> CheckResult:
    """Rebuild a CheckResult from XCom, or synthesise a failure if absent."""
    if payload is None:
        # The check task raised before pushing — treat as a failure of record.
        check = suite_lookup[check_name]
        return CheckResult(
            name=check.name,
            description=check.description,
            severity=check.severity,
            failing_rows=-1,
            passed=False,
        )
    return CheckResult(
        name=str(payload["name"]),
        description=str(payload["description"]),
        severity=Severity(payload["severity"]),
        failing_rows=int(payload["failing_rows"]),
        passed=bool(payload["passed"]),
    )


@dag(
    dag_id="data_quality_hr",
    description="Independent post-build data quality gate on the HR marts.",
    schedule=[HR_MARTS],
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=_DOC,
    tags=["hr", "data-quality", "layer:marts"],
)
def data_quality_hr() -> None:
    @task(trigger_rule="all_done")
    def publish_quality_report() -> dict[str, object]:
        """Aggregate every check into one report and persist it to the warehouse."""
        import pandas as pd  # deferred — DAG files are parsed frequently

        context = get_current_context()
        task_instance = context["ti"]
        suite_lookup = {check.name: check for check in _QUALITY_SUITE}

        results = [
            _result_from_xcom(
                check.name,
                task_instance.xcom_pull(task_ids=f"check__{check.name}", key="check_result"),
                suite_lookup,
            )
            for check in _QUALITY_SUITE
        ]
        report = QualityReport(results=results)
        logger.info("\n%s", report.summary())

        # Append one audit row per run — a queryable history of platform health.
        settings = Settings.from_env()
        run_row = pd.DataFrame(
            [
                {
                    "run_id": str(context["run_id"]),
                    "generated_at": pd.Timestamp.now(tz="UTC"),
                    "total_checks": len(report.results),
                    "passed": report.passed_count,
                    "blocking_failures": len(report.blocking_failures),
                    "warnings": len(report.warnings),
                }
            ]
        )
        with get_warehouse(settings) as warehouse:
            warehouse.load_dataframe("quality", "check_runs", run_row, mode="append")

        if report.has_blocking_failures():
            logger.error(
                "%s blocking data quality failure(s) — see the failed check tasks.",
                len(report.blocking_failures),
            )
        return report.as_dict()

    report = publish_quality_report()

    # One task per check, generated from the suite. Each runs before the report.
    for check in _QUALITY_SUITE:
        check_task = DataQualityCheckOperator(
            task_id=f"check__{check.name}",
            quality_check=check,
        )
        check_task >> report


data_quality_hr()
