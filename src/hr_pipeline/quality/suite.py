"""The HR marts quality suite.

These checks run *after* dbt has rebuilt the marts. They are deliberately
written in portable SQL so the exact same suite runs against DuckDB locally and
BigQuery in production. Each one mirrors a guarantee the marts are supposed to
hold — together they form the platform's data quality SLO.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hr_pipeline.quality.checks import QualityCheck, Severity

FCT_ACTIVE = "fct_employees_active"
DIM_DEPARTMENTS = "dim_departments"
FCT_HEADCOUNT = "fct_employee_headcount_monthly"


def build_marts_quality_suite(
    *,
    marts_schema: str = "marts",
    freshness_threshold: datetime | None = None,
    freshness_max_age_hours: int = 24,
) -> list[QualityCheck]:
    """Return the ordered list of quality checks for the HR marts.

    Args:
        marts_schema: schema (DuckDB) / dataset (BigQuery) holding the marts.
        freshness_threshold: marts loaded before this instant are considered
            stale. Defaults to ``now - freshness_max_age_hours``.
        freshness_max_age_hours: age used to derive the default threshold.
    """
    threshold = freshness_threshold or datetime.now(UTC) - timedelta(hours=freshness_max_age_hours)
    stale_literal = threshold.strftime("%Y-%m-%d %H:%M:%S")

    fct = f"{marts_schema}.{FCT_ACTIVE}"
    dim = f"{marts_schema}.{DIM_DEPARTMENTS}"
    headcount = f"{marts_schema}.{FCT_HEADCOUNT}"

    return [
        QualityCheck(
            name="fct_active__employee_id_not_null",
            description="Every active-employee fact row must have an employee_id.",
            sql=f"SELECT count(*) FROM {fct} WHERE employee_id IS NULL",
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="fct_active__employee_id_unique",
            description="employee_id is the grain of the fact table — no duplicates.",
            sql=(
                f"SELECT count(*) FROM ("
                f"  SELECT employee_id FROM {fct}"
                f"  GROUP BY employee_id HAVING count(*) > 1"
                f") duplicated_keys"
            ),
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="fct_active__salary_non_negative",
            description="A salary can never be negative.",
            sql=f"SELECT count(*) FROM {fct} WHERE salary < 0",
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="fct_active__no_future_hire_dates",
            description="Business rule: nobody can be hired in the future.",
            sql=f"SELECT count(*) FROM {fct} WHERE hire_date > CURRENT_DATE",
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="fct_active__years_of_service_non_negative",
            description="Derived tenure must be a non-negative number of years.",
            sql=f"SELECT count(*) FROM {fct} WHERE years_of_service < 0",
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="fct_active__department_referential_integrity",
            description="Every department in the fact table must exist in dim_departments.",
            sql=(
                f"SELECT count(*) FROM {fct} f"
                f"  LEFT JOIN {dim} d ON f.department = d.department"
                f"  WHERE d.department IS NULL"
            ),
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="dim_departments__department_unique",
            description="The department dimension has one row per department.",
            sql=(
                f"SELECT count(*) FROM ("
                f"  SELECT department FROM {dim}"
                f"  GROUP BY department HAVING count(*) > 1"
                f") duplicated_departments"
            ),
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="headcount__non_negative",
            description="Monthly headcount can never be negative.",
            sql=f"SELECT count(*) FROM {headcount} WHERE headcount < 0",
            severity=Severity.ERROR,
        ),
        QualityCheck(
            name="fct_active__not_empty",
            description="The active-employee fact table should not be empty after a build.",
            sql=f"SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM {fct}",
            severity=Severity.WARN,
        ),
        QualityCheck(
            name="fct_active__freshness",
            description=(
                f"The marts must have been rebuilt within the last {freshness_max_age_hours}h."
            ),
            sql=(
                f"SELECT count(*) FROM ("
                f"  SELECT max(dbt_loaded_at) AS last_loaded_at FROM {fct}"
                f") latest"
                f"  WHERE last_loaded_at IS NULL"
                # Cast normalises DuckDB's timestamptz vs BigQuery's timestamp.
                f"  OR CAST(last_loaded_at AS TIMESTAMP) < TIMESTAMP '{stale_literal}'"
            ),
            severity=Severity.WARN,
        ),
    ]
