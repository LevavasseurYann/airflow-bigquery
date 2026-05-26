# ADR-0003 — Cosmos for dbt orchestration

**Status:** Accepted · **Date:** 2026-05

## Context

`transform_hr_dbt` has to run a dbt project. The simplest option is a single
`BashOperator` running `dbt build` — but then the whole transformation is one
opaque task: one log, one retry, no per-model lineage. When one model fails you
rerun everything.

## Decision

Orchestrate dbt with **astronomer-cosmos**. Cosmos parses the dbt project and
renders **every model, seed, snapshot and test as its own Airflow task**, with
dbt's internal DAG preserved as Airflow dependencies.

Execution mode is `LOCAL`: dbt is installed in the Airflow image (via the
`astronomer-cosmos[dbt-duckdb]` extra) and Cosmos runs it in-place.

## Consequences

- Per-model logs, retries, durations and lineage in the Airflow UI.
- A failed model is retried on its own; downstream models wait.
- dbt tests appear as tasks — quality is visible in the orchestration graph.
- Cost: an extra dependency, and dbt now shares the Airflow image's Python
  environment.

## Alternatives considered

- *`BashOperator` running `dbt build`* — trivial, but opaque and all-or-nothing.
  Rejected.
- *Cosmos `VIRTUALENV` execution mode* — fully isolates dbt's dependencies from
  Airflow's. The cleaner choice if a dependency conflict ever appears; noted as
  the upgrade path. `LOCAL` is kept here for simplicity.
- *Hand-written `BashOperator` per model* — reinvents Cosmos, by hand. Rejected.
