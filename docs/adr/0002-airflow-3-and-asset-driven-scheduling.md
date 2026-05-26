# ADR-0002 — Airflow 3 and asset-driven scheduling

**Status:** Accepted · **Date:** 2026-05

## Context

The pipeline has three data stages that must run in order: ingest → transform →
verify. The classic ways to chain them across DAGs — a shared cron offset, or an
`ExternalTaskSensor` — are brittle: an offset guesses how long the previous
stage takes, and a sensor burns a worker slot waiting.

## Decision

Target **Apache Airflow 3** and chain the data DAGs with **Assets**
(data-aware scheduling):

- `ingest_hr_sources` produces the `raw_hr_employees` asset;
- `transform_hr_dbt` is `schedule=[raw_hr_employees]` and produces `hr_marts`;
- `data_quality_hr` is `schedule=[hr_marts]`.

DAGs are authored with the Task SDK (`airflow.sdk`) — the current API.

## Consequences

- The chain self-sequences: each stage starts the instant its input is ready,
  with no clock, no offset and no sensor.
- The dependency is **data lineage**, visible in the Airflow Assets view.
- Time-based scheduling is still used where it is the honest fit —
  `platform_maintenance` runs weekly because housekeeping has no data input.
- Requires Airflow ≥ 3.0; the project does not run on Airflow 2.

## Alternatives considered

- *Cron offsets* — fragile; breaks the moment a stage runs long. Rejected.
- *`ExternalTaskSensor`* — wastes a worker slot and still couples on timing.
  Rejected.
- *One mega-DAG* — loses independent schedules, retries and ownership per
  stage. Rejected.
