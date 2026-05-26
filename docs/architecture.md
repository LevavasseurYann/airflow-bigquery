# Architecture

## Overview

`airflow-bigquery` is the **orchestration layer** of a small but complete data
platform. It moves HR data through four stages — ingest, transform, verify,
maintain — and is deliberately built so the *same code* runs on a laptop
(DuckDB) and in the cloud (BigQuery).

## Runtime topology

The local cluster is the official Apache Airflow 3 multi-service deployment,
plus a custom image:

```
                    ┌──────────────────┐
   browser ───────► │ airflow-apiserver │  UI + REST (8080)
                    └──────────────────┘
   ┌─────────────┐  ┌──────────────────┐  ┌─────────────────────┐
   │  scheduler  │  │   dag-processor  │  │     triggerer       │
   └──────┬──────┘  └────────┬─────────┘  └─────────────────────┘
          │                  │  parses dags/
          ▼                  ▼
   ┌─────────────┐    ┌──────────────┐
   │   worker    │    │  postgres    │  metadata DB
   │  (Celery)   │    │  redis       │  Celery broker
   └──────┬──────┘    └──────────────┘
          │ executes tasks
          ▼
   ┌──────────────────────────────────────────────┐
   │  warehouse:  DuckDB file  |  BigQuery (prod)  │
   └──────────────────────────────────────────────┘
```

Every Airflow service shares **one custom image** (`Dockerfile`) so dbt, Cosmos
and the `hr_pipeline` package are available identically everywhere.

## The two halves of the codebase

| Layer | Location | Responsibility |
|-------|----------|----------------|
| **Orchestration** | `dags/` | *When* and *in what order* things run. Thin. |
| **Logic** | `src/hr_pipeline/` + `include/dbt/` | *What* actually happens. Tested. |

This separation is the project's backbone. A DAG file wires tasks together; it
never contains business logic. The logic is an installable, typed,
unit-testable package — see [ADR-0001](adr/0001-record-architecture-decisions.md).

## Data flow

```
regional CSVs ──► raw_hr.employees ──► staging ──► intermediate ──► marts ──► quality.check_runs
   (sources)         (ingestion)        (dbt views)  (ephemeral)   (dbt tables)   (audit)
```

| Layer | Schema | Built by | Materialisation |
|-------|--------|----------|-----------------|
| Raw | `raw_hr` | `ingest_hr_sources` | table (replaced each run) |
| Staging | `staging` | dbt | view |
| Intermediate | — | dbt | ephemeral |
| Marts | `marts` | dbt | table (one incremental) |
| Snapshot | `snapshots` | dbt | SCD2 table |
| Quality audit | `quality` | `data_quality_hr` | table (appended) |

## Scheduling model

The project uses **both** of Airflow's scheduling paradigms, on purpose:

- **Asset-driven** (the data DAGs). `ingest_hr_sources` produces the
  `raw_hr_employees` asset; `transform_hr_dbt` is scheduled *on* it and produces
  `hr_marts`; `data_quality_hr` is scheduled on *that*. The chain self-sequences
  with no clock and no sensors — see
  [ADR-0002](adr/0002-airflow-3-and-asset-driven-scheduling.md).
- **Time-driven** (`platform_maintenance`, weekly) — housekeeping has no data
  dependency, so a clock is the right trigger.

## Concurrency model

DuckDB allows **many concurrent readers but a single writer**. The project
models this explicitly rather than hoping for the best:

- **Writers are serialised.** Every dbt task runs in the `dbt` pool (one slot).
- **Readers run in parallel.** The data quality checks open the warehouse
  `read_only=True`, so all ~10 checks run concurrently.
- **Stages do not overlap.** The asset chain guarantees ingestion, dbt and
  quality never run at the same time.

In production (BigQuery) none of this applies — the pool can simply be widened.

## Configuration & environments

`hr_pipeline.config.Settings` is the single source of truth, resolved from
environment variables. `HR_ENV` switches the whole platform between the local
DuckDB profile and the production BigQuery profile — see
[ADR-0004](adr/0004-duckdb-local-bigquery-production.md) and
[docs/production-gcp.md](production-gcp.md).
