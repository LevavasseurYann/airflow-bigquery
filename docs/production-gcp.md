# Running against Google BigQuery

The local profile (DuckDB) and the production profile (BigQuery) run the **same
code**. Going to production is a configuration change, documented here.

> This is the production *design*. The repository runs fully locally by default
> so anyone can clone and explore it — see
> [ADR-0004](adr/0004-duckdb-local-bigquery-production.md).

## What changes — and what does not

| | Local | Production |
|--|-------|------------|
| `HR_ENV` | `local` | `production` |
| Warehouse | DuckDB file | BigQuery |
| `hr_pipeline` warehouse class | `DuckDBWarehouse` | `BigQueryWarehouse` |
| dbt target | `dev` (duckdb) | `prod` (bigquery) |
| Credentials | none | GCP service account |
| **DAGs, dbt models, SQL, quality suite** | **identical** | **identical** |

Only `hr_pipeline.config.Settings` and the dbt profile change behaviour — every
DAG and every model is untouched.

## 1. GCP prerequisites

- A GCP project with the **BigQuery API** enabled.
- A **service account** with `roles/bigquery.dataEditor` and
  `roles/bigquery.jobUser`.
- Its **JSON key**, mounted into the containers (never committed — `.gitignore`
  already blocks `sa-*.json` / `*-key.json`).

## 2. Environment

In `.env`:

```dotenv
HR_ENV=production
GCP_PROJECT=your-gcp-project-id
BQ_LOCATION=EU
BQ_DATASET_RAW=raw_hr
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/sa-key.json
```

Mount the key by adding to the `volumes` of `x-airflow-common` in
`docker-compose.yaml`:

```yaml
  - ./secrets:/opt/airflow/secrets:ro
```

For a real deployment, prefer a secrets manager (Google Secret Manager, or the
Airflow secrets backend) over a mounted file, and Workload Identity over a key.

## 3. dbt on BigQuery

`include/dbt/profiles.yml` already defines the `prod` target. With
`HR_ENV=production`, `transform_hr_dbt` runs Cosmos against it automatically.
The models need no changes — they use dbt cross-database macros, so the same
SQL compiles to BigQuery (and there the incremental fact uses the native
`merge` strategy, with `hire_date` partitioning and `department` clustering).

## 4. Concurrency

The DuckDB single-writer constraint does not exist on BigQuery. Widen the `dbt`
pool so dbt models run in parallel:

```bash
airflow pools set dbt 8 "dbt model concurrency on BigQuery"
```

## 5. Beyond this repo

A genuine production deployment would also add: a managed Airflow runtime
(Cloud Composer or Astronomer) instead of `docker compose`, CI/CD that builds
and promotes the image, a real Fernet key and secrets backend, and monitoring
wired into `on_failure_callback` (`dags/common.py`). Those are out of scope
here — this repository is the *pipeline*, designed so that lift is small.
