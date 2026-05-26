# ADR-0006 — SQL identifier validation in the Warehouse abstraction

**Status:** Accepted · **Date:** 2026-05

## Context

`hr_pipeline.warehouse` exposes methods such as `load_dataframe(schema, table,
…)` and `row_count(schema, table)`. These methods build SQL statements by
interpolating `schema` and `table` directly into f-strings with double-quote
quoting:

```python
f'SELECT count(*) FROM "{schema}"."{table}"'
```

DuckDB and BigQuery do not support parameterised identifiers (the `?`
placeholder works for *values*, not for table or schema names), so
parameterisation is not an option.

Double-quoting is sufficient for *valid* identifiers, but is exploitable if the
name itself contains a double quote: a caller passing `schema='a"b'` would
produce broken or malicious SQL. In this project today, `schema` and `table`
always come from `Settings` defaults and hard-coded constants. That is *now*
safe. However:

- The `Warehouse` class is the project's core abstraction, intended to be
  reused. Future callers could pass names derived from external sources (a UI
  field, an API response, a config file) without realising the risk.
- Code review is an imperfect guard: the callsite and the vulnerable site are
  in different files; a reviewer of the caller has no reason to check the
  implementation.

## Decision

Add a module-level `_safe_id(name: str) -> str` helper that validates a name
against the pattern `^[A-Za-z_][A-Za-z0-9_]*$` and raises `ValueError` on
anything else. Every method in `Warehouse` that interpolates an identifier into
SQL calls `_safe_id` before doing so.

This is a **preventive control**, not a reactive one: the validation runs before
any SQL is sent to the engine, and the error message names the bad value and the
rule, so debugging is immediate.

## Consequences

- SQL injection through schema/table names is impossible regardless of who calls
  the warehouse interface or where the names come from.
- Names with hyphens or dots (`raw-hr`, `my.schema`) now fail at the warehouse
  boundary rather than producing a confusing database error. Callers must use
  underscores (e.g. `raw_hr`), which the project already does consistently.
- `BigQueryWarehouse.table_exists` and `ensure_namespace` also call `_safe_id`
  even though they pass the name to the BigQuery client API (not to raw SQL).
  This is intentional: defensive validation at the boundary is cheaper than
  reasoning about what the client library does internally with the name.

## Alternatives considered

- *Document the constraint and trust callers* — leaves the risk open
  indefinitely; any future caller that breaks the convention silently introduces
  a vulnerability. Rejected.
- *Allowlist of known-good names* — fragile (the allowlist diverges from
  reality); still requires a validation mechanism. Rejected in favour of a
  structural rule.
- *Switch to a query builder that handles identifiers* — introduces a
  dependency for a problem solvable with twelve lines. Rejected.
