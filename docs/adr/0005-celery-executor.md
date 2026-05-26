# ADR-0005 — CeleryExecutor on the official compose topology

**Status:** Accepted · **Date:** 2026-05

## Context

The project needs a local runtime. The options span from `airflow standalone`
(one process) to the full multi-service cluster. A portfolio project should
demonstrate the *real* deployment shape, while still starting with one command.

## Decision

Run the **official Apache Airflow 3 `docker-compose` topology with
CeleryExecutor**: separate `api-server`, `scheduler`, `dag-processor`,
`worker`, `triggerer`, plus Postgres and Redis. A custom `Dockerfile` extends
the official image with dbt, Cosmos and the `hr_pipeline` package.

## Consequences

- The local setup mirrors a production-shaped, distributed Airflow deployment —
  tasks run in dedicated workers, the DAG processor is isolated (Airflow 3).
- Honest about resources: the stack needs ~6 GB of memory, documented in the
  README and `docs/local-development.md`.
- Building a custom image (rather than `_PIP_ADDITIONAL_REQUIREMENTS`) makes the
  environment reproducible — dependencies are not reinstalled on every boot.

## Alternatives considered

- *`airflow standalone`* — one command, but a single process hides the real
  architecture and the Airflow 3 component split. Rejected for a showcase repo.
- *`LocalExecutor`* — fewer services and lighter, a reasonable trade. Celery was
  chosen to demonstrate the distributed topology; switching back is a small,
  documented change.
- *Astro CLI* — excellent DX, but adds an Astronomer-specific layer; the plain
  upstream `docker-compose` keeps the project vendor-neutral.
