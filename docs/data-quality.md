# Data quality

The project verifies data at **two independent layers**. That redundancy is
intentional — it is defence in depth, not duplication.

## Layer 1 — dbt tests (build time)

Inside `transform_hr_dbt`, dbt tests run *as part of the build*:

- **generic tests** — `not_null`, `unique`, `accepted_values`, `relationships`;
- a **custom generic test** — `non_negative` (`include/dbt/macros/`);
- a **singular test** — `assert_no_future_hires` (`include/dbt/tests/`);
- **source freshness** — on the `_ingested_at` column of `raw_hr.employees`.

If a dbt test fails, that model's task fails. Quality is enforced *as the marts
are built*.

## Layer 2 — the independent quality suite (post-build)

`data_quality_hr` runs *after* the marts are published. It is owned by the
"platform", not by the dbt project — a separate control that would catch a
problem even if the dbt tests were misconfigured, and that produces its own
audit trail.

### The check contract

Every check (`hr_pipeline/quality/suite.py`) carries a SQL statement that
returns **one integer: the count of failing rows**. Zero means pass. This is
the exact convention dbt tests use — one mental model across the whole platform.

```python
QualityCheck(
    name="fct_active__no_future_hire_dates",
    description="Business rule: nobody can be hired in the future.",
    sql="SELECT count(*) FROM marts.fct_employees_active WHERE hire_date > CURRENT_DATE",
    severity=Severity.ERROR,
)
```

### Severity

| Severity | A failure… |
|----------|------------|
| `ERROR` | fails the task, and so the DAG run. |
| `WARN` | is logged and surfaced, but the task still succeeds. |

This separates "the data is wrong, stop" from "worth a look, keep going".

### What the suite checks

| Dimension | Example check |
|-----------|---------------|
| Completeness | `employee_id` is never null |
| Uniqueness | `employee_id` is the unique grain of the fact table |
| Validity | salary ≥ 0; `years_of_service` ≥ 0 |
| Business rule | no hire date in the future |
| Referential integrity | every fact `department` exists in `dim_departments` |
| Freshness | the marts were rebuilt within the last 24 h (`WARN`) |
| Sanity | the fact table is not empty (`WARN`) |

### The audit trail

`publish_quality_report` appends one row per run to `quality.check_runs`
(run id, timestamp, totals, blocking failures, warnings). Platform health
becomes a **queryable history**, not a scroll through logs.

## Why it is portable

Every check is written in SQL common to DuckDB and BigQuery, so the identical
suite runs locally and in production. The freshness threshold is computed in
Python and injected as a literal, side-stepping the one place the two
warehouses differ on timestamp arithmetic.
