# ADR-0004 — DuckDB locally, BigQuery in production

**Status:** Accepted · **Date:** 2026-05

## Context

The pipeline targets **Google BigQuery**. But a portfolio repository must be
*clonable and runnable by anyone* — a recruiter, a reviewer — without a GCP
account, billing or credentials. Binding everything to BigQuery would make the
project un-runnable for exactly the audience it is built for.

## Decision

Support two warehouse backends behind one interface:

- **Local (default)** — **DuckDB**, an embedded file-based warehouse: zero
  install, zero credentials.
- **Production** — **BigQuery**.

The choice is driven entirely by the `HR_ENV` environment variable, resolved in
`hr_pipeline.config.Settings`. The `Warehouse` abstraction
(`hr_pipeline/warehouse.py`) has a DuckDB and a BigQuery implementation; the dbt
project uses dbt **cross-database macros** so the same SQL compiles on both.

## Consequences

- `git clone` → `docker compose up` works with no cloud account. This is the
  single most important property for a public portfolio repo.
- The same DAGs, models and quality checks run in both environments — proof the
  abstraction is real, not cosmetic.
- DuckDB's single-writer model has to be respected locally (the `dbt` pool,
  read-only connections) — see [ADR-0005](0005-celery-executor.md) and
  `docs/architecture.md`.
- The two warehouses differ slightly in SQL; cross-database macros and one
  Python-computed timestamp literal absorb the difference.

## Alternatives considered

- *BigQuery only* — most realistic, but un-runnable without credentials.
  Rejected for a portfolio project.
- *Postgres locally* — needs a running server; DuckDB needs nothing and is a
  genuine analytical (columnar) engine, closer to BigQuery in spirit.
- *A separate "demo" pipeline* — two codebases drift apart. Rejected.
