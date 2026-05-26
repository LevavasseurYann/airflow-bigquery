"""Unit tests for the data quality engine and the HR marts suite.

These tests seed minimal marts into a DuckDB fixture and run the *real* quality
suite against them — proving both the engine and the SQL of every check.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from hr_pipeline.quality import build_marts_quality_suite, run_suite
from hr_pipeline.quality.checks import QualityCheck, Severity, run_check


def _seed_marts(warehouse, *, negative_salary: bool = False) -> None:
    """Build minimal but valid marts in the warehouse fixture."""
    fct = pd.DataFrame(
        {
            "employee_id": ["E1", "E2", "E3"],
            "employee_sk": ["s1", "s2", "s3"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
            "hire_date": pd.to_datetime(["2020-01-01", "2021-06-15", "2019-03-30"]),
            "department": ["engineering", "sales", "hr"],
            "salary": [90000.0, 70000.0, -1.0 if negative_salary else 60000.0],
            "salary_vs_target": ["below_target", "below_target", "below_target"],
            "years_of_service": [5, 4, 6],
            "dbt_loaded_at": [pd.Timestamp.now()] * 3,
        }
    )
    warehouse.load_dataframe("marts", "fct_employees_active", fct)

    dim = pd.DataFrame(
        {
            "department_sk": ["d1", "d2", "d3"],
            "department": ["engineering", "sales", "hr"],
        }
    )
    warehouse.load_dataframe("marts", "dim_departments", dim)

    headcount = pd.DataFrame(
        {
            "hired_month": [dt.date(2020, 1, 1)],
            "department": ["engineering"],
            "headcount": [1],
        }
    )
    warehouse.load_dataframe("marts", "fct_employee_headcount_monthly", headcount)


def test_build_suite_returns_runnable_checks():
    suite = build_marts_quality_suite()
    assert len(suite) >= 8
    assert all(isinstance(check, QualityCheck) and check.sql for check in suite)


def test_suite_passes_on_clean_marts(duckdb_warehouse):
    _seed_marts(duckdb_warehouse)
    report = run_suite(duckdb_warehouse, build_marts_quality_suite(marts_schema="marts"))
    assert report.passed, report.summary()
    assert report.has_blocking_failures() is False


def test_negative_salary_is_a_blocking_failure(duckdb_warehouse):
    _seed_marts(duckdb_warehouse, negative_salary=True)
    report = run_suite(duckdb_warehouse, build_marts_quality_suite(marts_schema="marts"))
    assert report.passed is False
    assert report.has_blocking_failures() is True
    assert "fct_active__salary_non_negative" in {r.name for r in report.blocking_failures}


def test_run_check_counts_failing_rows(duckdb_warehouse):
    _seed_marts(duckdb_warehouse, negative_salary=True)
    check = QualityCheck(
        name="probe",
        description="rows with a negative salary",
        sql="SELECT count(*) FROM marts.fct_employees_active WHERE salary < 0",
        severity=Severity.ERROR,
    )
    result = run_check(duckdb_warehouse, check)
    assert result.failing_rows == 1
    assert result.passed is False
