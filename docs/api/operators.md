# operators

Custom Airflow operators packaged in `hr_pipeline.operators`.

!!! note "Airflow dependency"
    This subpackage imports from `airflow.sdk` and is only imported from DAG
    files.  It is **not** available when running `hr_pipeline` outside of an
    Airflow environment.

---

::: hr_pipeline.operators.data_quality.DataQualityCheckOperator
