# quality.suite

The HR marts quality catalogue — the ordered list of assertions the platform
makes about the published marts after every dbt build.

!!! info "Runtime evaluation"
    The freshness check uses `CURRENT_TIMESTAMP - INTERVAL 'N' HOUR` in SQL,
    so the threshold is always relative to **when the check task runs**, not
    when the suite was constructed.  See
    [ADR-0007](../adr/0007-dag-parse-time-vs-run-time-configuration.md).

## Checks included

| Name | Dimension | Severity |
|------|-----------|----------|
| `fct_active__employee_id_not_null` | Completeness | ERROR |
| `fct_active__employee_id_unique` | Uniqueness | ERROR |
| `fct_active__salary_non_negative` | Validity | ERROR |
| `fct_active__no_future_hire_dates` | Business rule | ERROR |
| `fct_active__years_of_service_non_negative` | Validity | ERROR |
| `fct_active__department_referential_integrity` | Referential integrity | ERROR |
| `dim_departments__department_unique` | Uniqueness | ERROR |
| `headcount__non_negative` | Validity | ERROR |
| `fct_active__not_empty` | Sanity | WARN |
| `fct_active__freshness` | Freshness | WARN |

---

::: hr_pipeline.quality.suite.build_marts_quality_suite
