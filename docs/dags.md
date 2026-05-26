# DAG reference

Four DAGs. Three form the asset-linked data chain; one is time-scheduled
housekeeping.

---

## `ingest_hr_sources`

**Schedule:** daily · **Produces:** asset `raw_hr_employees`

Extracts the regional HR CSVs into the raw warehouse table.

| Task | Type | Role |
|------|------|------|
| `discover_sources` | `@task` | Lists every `employees_*.csv` extract. |
| `extract_region` | `@task` (mapped) | Validates one file, lands it as Parquet. One parallel instance **per file**. |
| `load_raw` | `@task`, `outlets=[…]` | Consolidates the Parquet files into `raw_hr.employees`; publishes the asset. |

**Airflow features:** TaskFlow API, dynamic task mapping (`.expand`), asset
producer. **Idempotent** — the raw table is fully replaced; cross-region
duplicates collapse last-write-wins on `updated_at`.

---

## `transform_hr_dbt`

**Schedule:** asset `raw_hr_employees` · **Produces:** asset `hr_marts`

Runs the embedded dbt project through Cosmos.

| Task | Type | Role |
|------|------|------|
| `verify_raw_source` | `@task` | Fails fast if `raw_hr.employees` is missing or empty. |
| `dbt_transform` | `DbtTaskGroup` | Cosmos renders **every** model, seed, snapshot and test as its own task. |
| `publish_marts` | `@task`, `outlets=[…]` | Confirms the marts are non-empty; publishes the asset. |

**Airflow features:** asset-driven scheduling, Cosmos integration, the `dbt`
pool (DuckDB single-writer). Every dbt task has independent logs, retries and
lineage — far better than one opaque `dbt build` task.

---

## `data_quality_hr`

**Schedule:** asset `hr_marts` · **Produces:** `quality.check_runs` rows

An independent quality gate on the published marts.

| Task | Type | Role |
|------|------|------|
| `check__*` | `DataQualityCheckOperator` | One task per check in the suite. `ERROR` checks fail the DAG; `WARN` checks only annotate it. |
| `publish_quality_report` | `@task`, `trigger_rule=all_done` | Aggregates all results, logs a summary, appends an audit row. |

**Airflow features:** asset-driven scheduling, a custom operator, `trigger_rule`
(`all_done` — the report runs even when a check failed). Detail in
[data-quality.md](data-quality.md).

---

## `platform_maintenance`

**Schedule:** weekly

Operational housekeeping — the part tutorials skip.

| Task | Role |
|------|------|
| `check_warehouse_connectivity` | Smoke test: the warehouse must answer `SELECT 1`. |
| `report_warehouse_inventory` | Logs the row count of every known table. |
| `prune_staging_files` | Deletes Parquet scratch files older than 7 days. |

**Airflow features:** time-based scheduling (a deliberate contrast with the
asset-driven data DAGs), fan-out dependencies.

---

## Shared conventions

All DAGs inherit from `dags/common.py`:

- `DEFAULT_ARGS` — owner, **2 retries with exponential backoff**, a structured
  `on_failure_callback` (the single hook point for real alerting).
- `catchup=False`, `max_active_runs=1` — no backfilling, no overlapping runs.
- Tags, a description and `doc_md` — required by the cluster policy.
