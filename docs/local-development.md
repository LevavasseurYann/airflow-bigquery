# Local development

How to run, explore and debug the project on your machine. The local profile
uses DuckDB — **no cloud account and no credentials are required**.

## Prerequisites

- Docker Desktop / Docker Engine with **~6 GB** of memory available (the Celery
  topology is several services). Check Docker → Settings → Resources.
- Docker Compose v2 (`docker compose`, not `docker-compose`).
- Optional: `make`, and Python 3.12 for running the tests outside Docker.

## First run

```bash
cp .env.example .env          # PowerShell: Copy-Item .env.example .env
docker compose build          # build the custom image (a few minutes once)
docker compose up airflow-init   # bootstrap DB, admin user, the `dbt` pool
docker compose up -d          # start the cluster
```

Open <http://localhost:8080> — login `airflow` / `airflow`. Unpause the four
DAGs, then trigger `ingest_hr_sources`; the asset chain runs the rest.

## Everyday commands

| Action | Command | `make` |
|--------|---------|--------|
| Start | `docker compose up -d` | `make up` |
| Stop | `docker compose down` | `make down` |
| Logs | `docker compose logs -f` | `make logs` |
| Shell in a worker | `docker compose exec airflow-worker bash` | `make shell` |
| Run dbt directly | `docker compose exec airflow-worker dbt build --project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt` | `make dbt-build` |
| Full clean slate | `docker compose down --volumes` | `make reset` |

## Running the tests (no Docker needed)

```bash
pip install -e ".[dev]"
pytest -m "not dags"   # fast unit tests
pytest -m dags         # DAG-bag validation (needs Airflow + Cosmos installed)
ruff check src dags tests scripts
```

## Inspecting the warehouse

The DuckDB file is `include/data/warehouse.duckdb`. With the DuckDB CLI:

```bash
duckdb include/data/warehouse.duckdb
D SELECT * FROM marts.fct_employees_active LIMIT 5;
D SELECT * FROM quality.check_runs ORDER BY generated_at DESC;
```

You can also seed it without Airflow at all:

```bash
HR_RAW_DATA_DIR=./include/data/raw DUCKDB_PATH=./include/data/warehouse.duckdb \
  python scripts/seed_local_warehouse.py
```

## Troubleshooting

| Symptom | Cause & fix |
|---------|-------------|
| Services restart / get OOM-killed | Not enough memory for Docker — raise it to ≥ 6 GB. |
| Port 8080 already in use | Another service holds it — change the `airflow-apiserver` port mapping. |
| `airflow-init` exits non-zero | Read its logs; usually low memory/disk. Re-run after fixing. |
| DuckDB "Conflicting lock" | Two writers at once. The `dbt` pool should prevent it — confirm the pool exists (`airflow-init` creates it) and that dbt tasks use it. |
| DAG shows an import error | Open it in the UI for the traceback. Run `pytest -m dags` locally for the same check. |
| dbt task fails: relation not found | `raw_hr.employees` is missing — run `ingest_hr_sources` first. |
| Changes to `src/` not picked up | `src/` is bind-mounted and installed editable; restart the affected service. A `requirements.txt` change needs `docker compose build`. |

## Resetting

```bash
docker compose down --volumes --remove-orphans   # drops the metadata DB
rm -f include/data/*.duckdb                      # drops the warehouse
```
