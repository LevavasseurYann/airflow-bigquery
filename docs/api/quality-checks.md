# quality.checks

The data quality engine. Every `QualityCheck` carries a SQL statement that
returns exactly **one integer — the count of failing rows**. Zero means pass.
This is the same convention dbt tests use, so the mental model is consistent
across both quality layers.

```python
check = QualityCheck(
    name="no_future_hires",
    description="Nobody can be hired in the future.",
    sql="SELECT count(*) FROM marts.fct_employees_active WHERE hire_date > CURRENT_DATE",
    severity=Severity.ERROR,
)
result = run_check(warehouse, check)
# result.passed → True / False
# result.failing_rows → int
```

---

::: hr_pipeline.quality.checks.Severity

---

::: hr_pipeline.quality.checks.QualityCheck

---

::: hr_pipeline.quality.checks.CheckResult

---

::: hr_pipeline.quality.checks.QualityReport

---

::: hr_pipeline.quality.checks.run_check

---

::: hr_pipeline.quality.checks.run_suite
