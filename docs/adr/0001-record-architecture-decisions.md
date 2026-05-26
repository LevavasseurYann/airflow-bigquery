# ADR-0001 — Record architecture decisions

**Status:** Accepted · **Date:** 2026-05

## Context

Even a small project accumulates decisions whose *reasons* are quickly lost.
A reviewer (or a future me) then can't tell a deliberate trade-off from an
accident. The first decision worth recording is the project's structural
principle.

## Decision

1. **Use ADRs.** Every decision that shapes the architecture is captured as a
   short, immutable, numbered file in `docs/adr/`. Superseded ADRs are kept and
   marked, never deleted.
2. **Separate orchestration from logic.** `dags/` only wires tasks together.
   All business logic lives in the installable `hr_pipeline` package or in the
   dbt project — both unit-testable *without* Airflow.

## Consequences

- The `hr_pipeline` package can be tested with plain `pytest` — fast CI, no
  scheduler. DAG files stay short and readable.
- A small amount of indirection: a reader follows a DAG into the package to see
  what a task really does. Worth it.
- New significant decisions must be written down — a light, deliberate tax.

## Alternatives considered

- *Logic inline in DAG files* — simpler to skim, but untestable without Airflow
  and it makes DAGs long and noisy. Rejected.
