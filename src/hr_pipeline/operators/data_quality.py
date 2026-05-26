"""A custom operator that runs a single hr_pipeline data quality check.

Writing a focused custom operator (instead of a generic ``@task``) is the
idiomatic way to package reusable, parametrised behaviour in Airflow: the check
becomes a first-class node in the graph with its own colour, docs and XCom.
"""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from hr_pipeline.quality.checks import QualityCheck, Severity


class DataQualityCheckOperator(BaseOperator):
    """Run one :class:`~hr_pipeline.quality.checks.QualityCheck`.

    Behaviour on failure depends on the check severity:

    * :attr:`Severity.ERROR` — raises :class:`AirflowException`, failing the task
      (and, by default, the DAG run).
    * :attr:`Severity.WARN` — logs a warning; the task still succeeds.

    The check result is returned and therefore pushed to XCom, so a downstream
    task can build a consolidated quality report.
    """

    ui_color = "#4a90d9"
    ui_fgcolor = "#ffffff"

    def __init__(self, *, quality_check: QualityCheck, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.quality_check = quality_check

    def execute(self, context: Any) -> dict[str, Any]:
        # Imported inside execute() so DAG parsing stays fast and import-light.
        from hr_pipeline.config import Settings
        from hr_pipeline.quality.checks import run_check
        from hr_pipeline.warehouse import get_warehouse

        settings = Settings.from_env()
        self.log.info("Running data quality check '%s'", self.quality_check.name)

        # Read-only connection — quality checks never write, so several can run
        # in parallel without contending for DuckDB's single-writer lock.
        with get_warehouse(settings, read_only=True) as warehouse:
            result = run_check(warehouse, self.quality_check)

        # Push the result BEFORE any raise, so the downstream report task can
        # account for a failed check too.
        result_dict = result.as_dict()
        context["ti"].xcom_push(key="check_result", value=result_dict)

        if result.passed:
            self.log.info("PASS — %s", self.quality_check.description)
            return result_dict

        message = (
            f"Data quality check '{result.name}' failed: "
            f"{result.failing_rows} failing row(s). {result.description}"
        )
        if self.quality_check.severity is Severity.ERROR:
            raise AirflowException(message)

        self.log.warning("%s — severity=WARN, not blocking the DAG.", message)
        return result_dict
