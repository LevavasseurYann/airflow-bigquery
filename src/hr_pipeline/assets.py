"""Airflow Asset definitions — the backbone of data-aware scheduling.

Airflow 3 can schedule a DAG on **Assets** instead of (or alongside) a clock:
when an upstream task updates an asset, every DAG subscribed to that asset is
triggered automatically. This replaces brittle ``ExternalTaskSensor`` / fixed
-offset coupling with explicit, observable data lineage.

In this project:

* the ingestion DAG **produces** :data:`RAW_HR_EMPLOYEES`;
* the dbt transformation DAG is **scheduled on** it, and **produces**
  :data:`HR_MARTS`;
* the data quality DAG is **scheduled on** :data:`HR_MARTS`.

Defining the assets in one shared module guarantees producer and consumer
reference the *exact same* object — a frequent source of silent scheduling bugs.
"""

from __future__ import annotations

from airflow.sdk import Asset

#: Raw landing table ``raw_hr.employees`` — produced by ``ingest_hr_sources``.
RAW_HR_EMPLOYEES = Asset(
    name="raw_hr_employees",
    uri="warehouse://raw_hr/employees",
    group="raw",
)

#: Curated HR marts — produced by ``transform_hr_dbt`` once dbt has rebuilt them.
HR_MARTS = Asset(
    name="hr_marts",
    uri="warehouse://marts/hr",
    group="marts",
)
