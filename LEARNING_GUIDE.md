# Learning Guide — a hands-on tour of the project

This guide walks through every component, what it does, and *why it is built
that way*. Read it top to bottom once; then keep it open while you explore the
code. It assumes you have run the [Quickstart](README.md#quickstart).

---

## 1. The mental model

The project orchestrates the **modern data stack** on one HR dataset:

```
   ingest          transform           verify
  (Python)   →      (dbt)        →   (data quality)
  raw_hr.*        staging/marts       quality.check_runs
```

Three ideas hold it together:

1. **Orchestration ≠ logic.** DAGs in `dags/` only *wire tasks together*. The
   actual work lives in the `hr_pipeline` Python package and in the dbt project.
   A DAG file should read like a table of contents.
2. **Data-aware scheduling.** Stages are linked by **Assets**, not by guessing
   how long the previous stage takes.
3. **One codebase, two environments.** Everything is resolved from environment
   variables, so the laptop run (DuckDB) and the production run (BigQuery) use
   identical code.

---

## 2. The local environment

`docker compose` starts the official Airflow 3 topology:

| Service | Role |
|---------|------|
| `postgres` | Airflow metadata database |
| `redis` | Celery broker |
| `airflow-apiserver` | web UI + REST API (port 8080) |
| `airflow-scheduler` | decides what runs when |
| `airflow-dag-processor` | parses DAG files (isolated, Airflow 3) |
| `airflow-worker` | executes tasks (Celery) |
| `airflow-triggerer` | runs deferred/async tasks |

The pipeline's **warehouse** is a single DuckDB file in `include/data/`.
No server, no account — that is the whole point of the local profile.

---

## 3. The code, section by section

### 3.1 Configuration — `src/hr_pipeline/config.py`
A frozen `Settings` dataclass built by `Settings.from_env()`. It is the only
place that reads the environment. Everything downstream receives a `Settings`
object. Start here — it explains every path and switch in the project.

### 3.2 Warehouse — `src/hr_pipeline/warehouse.py`
One `Warehouse` interface, two implementations (`DuckDBWarehouse`,
`BigQueryWarehouse`). `get_warehouse(settings)` returns the right one. This is
the abstraction that makes "local ↔ production" a configuration change.
Note the `read_only` flag — it exists because DuckDB allows many readers but
only one writer.

### 3.3 Ingestion — `src/hr_pipeline/ingestion.py`
Pure functions implementing **extract → land → load**: discover the CSVs,
validate and land each as Parquet, then consolidate into `raw_hr.employees`.
No Airflow here — it is plain, testable Python.

### 3.4 DAG 1 — `dags/ingest_hr_sources.py`
Wraps the ingestion functions in tasks. Watch for **dynamic task mapping**:
`extract_region.expand(source=...)` creates one parallel task per source file.
The final task carries `outlets=[RAW_HR_EMPLOYEES]` — it **produces an asset**.

### 3.5 DAG 2 — `dags/transform_hr_dbt.py`
`schedule=[RAW_HR_EMPLOYEES]` — the DAG is **triggered by the asset**, with no
clock. **Cosmos** turns the dbt project (`include/dbt`) into a task group: one
Airflow task per model / seed / snapshot / test. Every dbt task runs in the
`dbt` pool (size 1) — see §4.4.

### 3.6 The dbt project — `include/dbt/`
A self-contained, **zero-dependency**, portable dbt project. Layered
staging → intermediate → marts, an incremental fact, an SCD2 snapshot, a seed,
a custom generic test and a singular test. Every model uses dbt's
cross-database macros so the same SQL runs on DuckDB and BigQuery.

### 3.7 DAG 3 — `dags/data_quality_hr.py`
`schedule=[HR_MARTS]`. Runs the quality suite — one task per check, built with
the **custom `DataQualityCheckOperator`**. A final reporter task aggregates the
results and writes an audit row to `quality.check_runs`.

### 3.8 DAG 4 — `dags/platform_maintenance.py`
A **time-scheduled** DAG (weekly) — a deliberate contrast with the asset-driven
data DAGs. Connectivity probe, inventory report, scratch cleanup.

### 3.9 Cluster policies — `config/airflow_local_settings.py`
`dag_policy` rejects any DAG that is not tagged, described and owned;
`task_policy` applies a default execution timeout. Org-wide rules, enforced
centrally at parse time.

---

## 4. Airflow 3 concepts demonstrated

### 4.1 Assets & data-aware scheduling
An **Asset** is a named piece of data. A task `outlets` an asset to mark it
updated; a DAG `schedule=[asset]` runs when it is. Defined once in
`hr_pipeline/assets.py` so producer and consumer share the same object.

### 4.2 TaskFlow API
`@dag` / `@task` from `airflow.sdk`. Return values flow between tasks as XComs
automatically — no manual `xcom_push`/`pull` for the common case.

### 4.3 Dynamic task mapping
`.expand()` fans a task out at run time — the number of mapped instances is not
known when the DAG is written. Here: one extract task per discovered file.

### 4.4 Pools
A pool caps how many tasks run at once. The `dbt` pool has **one slot**: it
serialises the dbt tasks because DuckDB has a single writer. On BigQuery you
would simply widen the pool — no code change.

### 4.5 Cluster policies
Hooks Airflow calls for every DAG/task. The place a platform team enforces
standards without trusting every author to remember them.

---

## 5. Suggested hands-on run order

1. `docker compose build && docker compose up airflow-init && docker compose up -d`
2. UI → unpause **all four** DAGs.
3. Trigger `ingest_hr_sources`. Watch the mapped `extract_region` tasks.
4. `transform_hr_dbt` starts on its own — open it and see the dbt task graph.
5. `data_quality_hr` starts after it — inspect a `check__*` task's log.
6. Open **Assets** in the UI — see the lineage graph between the DAGs.
7. `docker compose exec airflow-worker dbt docs generate --project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt`

---

## 6. Experiments to try

- **Break a check.** Edit a row in `include/data/raw/employees_eu.csv` to a
  negative salary, re-run the chain, and watch `data_quality_hr` fail exactly
  one task.
- **Add a region.** Drop `employees_latam.csv` into `include/data/raw/`;
  re-run ingestion — a new mapped task appears, no code change.
- **Break the policy.** Remove the `tags=[...]` from a DAG and reload — the
  cluster policy rejects it with a clear error.
- **Incremental dbt.** Re-run `transform_hr_dbt` twice; the second run of
  `fct_employees_active` processes only changed rows.
- **Go to BigQuery.** Follow [docs/production-gcp.md](docs/production-gcp.md)
  and flip `HR_ENV=production`.

---

## 7. Going deeper

- [docs/architecture.md](docs/architecture.md) — the full architecture.
- [docs/adr/](docs/adr/) — why each decision was made.
- [Apache Airflow docs](https://airflow.apache.org/docs/) ·
  [Cosmos docs](https://astronomer.github.io/astronomer-cosmos/) ·
  [dbt docs](https://docs.getdbt.com/)
