# ADR-0007 — DAG parse-time vs. run-time configuration

**Status:** Accepted · **Date:** 2026-05

## Context

Airflow's `dag-processor` service imports every DAG file on a tight loop to
detect changes. Any code that runs at **module level** (i.e. outside a task
function) runs during *parsing*, not during *task execution*. This matters for
two reasons:

1. **Errors at parse time become import errors**, visible as red alerts in the
   Airflow UI for the whole DAG, not as task failures that are clearly scoped to
   a run.
2. **State captured at parse time is reused across runs.** A timestamp computed
   at import (`datetime.now()`) stays the same value until the DAG module is
   re-imported — which may be hours later.

This second point caused a concrete bug in `data_quality_hr`: the freshness
check SQL was built with a threshold literal computed at parse time:

```python
# At module import — frozen until the next parse cycle
threshold = datetime.now(UTC) - timedelta(hours=24)
stale_literal = threshold.strftime("%Y-%m-%d %H:%M:%S")
```

Because the DAG processor keeps the module cached, `stale_literal` was always
relative to *when the DAG was first loaded*, not to when the check task
actually ran. The freshness check silently always passed.

## Decision

Apply the following rules for every DAG file:

### Run at parse time (module level) when — and only when — it is unavoidable

The only legitimate use is configuring static Airflow structures that Airflow
builds from the module-level namespace at parse time. In this project: Cosmos
configuration in `transform_hr_dbt` requires `_settings` at parse time because
`DbtTaskGroup` is instantiated there.

```python
# transform_hr_dbt.py — Cosmos must be configured at parse time
_settings = Settings.from_env()
_profile_config = ProfileConfig(…, target_name=_settings.dbt_target, …)
dbt_transform = DbtTaskGroup(…)   # uses _profile_config
```

### Prefer module-level `os.getenv()` over `Settings.from_env()` at parse time

`os.getenv()` never raises. `Settings.from_env()` raises `ValueError` if the
environment is misconfigured (wrong `HR_ENV`, missing `GCP_PROJECT`). A
`ValueError` at parse time becomes a DAG import error — a broad-brush failure
that hides the real cause. When only one or two env vars are needed at parse
time, read them directly:

```python
# data_quality_hr.py — only the schema name is needed at parse time
_QUALITY_SUITE = build_marts_quality_suite(
    marts_schema=os.getenv("BQ_DATASET_MARTS", "marts"),
)
```

### Never compute time-sensitive state at parse time

Any value that must reflect the *current moment* when a task runs (timestamps,
freshness windows, "now") must be computed **inside the task function**, or
expressed in SQL using `CURRENT_TIMESTAMP`. Module-level computation freezes
the value at first parse, not at first execution.

```python
# Wrong — frozen at parse time:
_STALE_AFTER = datetime.now(UTC) - timedelta(hours=24)

# Right — evaluated each time the task runs:
@task
def my_check():
    threshold = datetime.now(UTC) - timedelta(hours=24)
    …

# Best for SQL-only checks — evaluated at query time:
sql = "… WHERE ts < CURRENT_TIMESTAMP - INTERVAL '24' HOUR"
```

### Never import heavy libraries at DAG file top level

`import pandas` costs ~200 ms and ~50 MB. The `dag-processor` parses files
frequently. Heavy imports belong inside task bodies or `execute()` methods.

## Consequences

- DAG import errors are reserved for genuine structural problems
  (syntax error, bad Cosmos config), not for transient environment issues.
- Time-sensitive checks always reflect "now" at execution time.
- `transform_hr_dbt` intentionally violates the "avoid parse-time
  `Settings.from_env()`" rule — it is the documented exception, not the
  norm, because Cosmos leaves no alternative.
- The `dbt` pool, task defaults and asset definitions remain at parse time
  because they are static Airflow metadata — this is their correct location.

## Alternatives considered

- *Call `Settings.from_env()` everywhere at parse time* — brittle; any
  misconfigured environment collapses the whole DAG bag into import errors.
  Rejected.
- *Move all configuration into Airflow Variables* — indirects the problem;
  adds a runtime dependency on the metadata DB at parse time. Rejected.
- *Build time-sensitive values from a deferred task* — adds a task per DAG run
  just to compute a threshold. The SQL `CURRENT_TIMESTAMP` approach eliminates
  the need entirely. Rejected.
