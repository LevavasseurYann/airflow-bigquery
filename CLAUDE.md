# airflow-bigquery — Contexte agent

> Fichier lu automatiquement par Claude Code. Donne au modèle le contexte
> nécessaire pour travailler efficacement dans ce repo sans redemander les bases.

---

## Stack et versions

| Outil | Version | Rôle |
|---|---|---|
| Apache Airflow | 3.1 | Orchestrateur (CeleryExecutor) |
| astronomer-cosmos | latest | Intégration dbt dans Airflow |
| dbt-core | 1.9+ | Transformations embarquées dans `include/dbt/` |
| DuckDB | 1.1+ | Warehouse local (défaut, sans cloud) |
| Google BigQuery | — | Warehouse production (`HR_ENV=production`) |
| Python | 3.12 | Runtime |
| Docker Compose | v2 | Runtime local multi-services |
| pytest | 8+ | Tests unitaires + DAG-integrity |
| ruff | 0.6+ | Lint + format |
| mypy | 1.11+ | Type-checking du package `hr_pipeline` |

---

## Architecture en 30 secondes

```
CSV sources (EU/US/APAC)
  → ingest_hr_sources (DAG 1, daily)          → raw_hr.employees
  → transform_hr_dbt (DAG 2, asset-driven)    → marts/* via Cosmos+dbt
  → data_quality_hr (DAG 3, asset-driven)     → quality.check_runs
  → platform_maintenance (DAG 4, weekly)      → housekeeping
```

**Règle fondamentale :** logique métier dans `src/hr_pipeline/`, **jamais** dans `dags/`.
Les DAGs orchestrent — ils n'implémentent pas.

---

## Structure critique

```
dags/
  common.py              # DEFAULT_ARGS, on_failure_callback, PROJECT_OWNER
  ingest_hr_sources.py   # DAG 1
  transform_hr_dbt.py    # DAG 2 (Cosmos DbtDag)
  data_quality_hr.py     # DAG 3
  platform_maintenance.py # DAG 4

src/hr_pipeline/
  config.py       # Settings.from_env() — point d'entrée configuration
  warehouse.py    # abstraction DuckDB / BigQuery
  ingestion.py    # extract → land (Parquet) → load
  assets.py       # définitions Airflow Assets (asset-driven scheduling)
  quality/
    checks.py     # fonctions de vérification
    suite.py      # suite HR complète
  operators/
    data_quality.py  # DataQualityCheckOperator (custom)

include/
  dbt/         # projet dbt embarqué (staging → intermediate → marts)
  data/
    raw/       # CSV sources (employees_eu/us/apac.csv)
    _staging/  # zone de landing Parquet (généré par ingestion.py, gitignored)

scripts/
  seed_local_warehouse.py  # seed DuckDB sans Airflow (CI + exploration locale)
                           # Usage: HR_RAW_DATA_DIR=./include/data/raw DUCKDB_PATH=./include/data/warehouse.duckdb python scripts/seed_local_warehouse.py

config/
  airflow_local_settings.py  # cluster policies (parse-time guardrails)
  airflow.cfg                # config Airflow locale (géré par le container — ne pas éditer manuellement)

plugins/    # répertoire vide, réservé pour custom Airflow plugins
tests/      # test_dag_integrity.py, test_ingestion.py, test_warehouse.py,
            # test_quality.py, test_config.py
docs/adr/   # 7 Architecture Decision Records — lire avant de modifier une décision structurelle
            # (0002: Asset-driven scheduling, 0003: Cosmos, 0004: DuckDB/BQ, 0005: Celery,
            #  0006: SQL validation, 0007: parse-time vs run-time config)
```

---

## Commandes

```bash
# Démarrage cluster local
Copy-Item .env.example .env   # une seule fois
docker compose build
docker compose up airflow-init  # bootstrap DB + admin + pool
docker compose up -d
# UI : http://localhost:8080  (airflow / airflow)

# Tests (sans Docker)
pytest -m "not dags"   # rapide, pas d'Airflow requis
pytest -m dags         # DAG-integrity (Airflow + Cosmos requis)
pytest                 # tout

# Qualité code (sans Docker)
ruff check src dags tests
ruff format src dags tests
mypy

# Seed DuckDB local sans Airflow (CI / exploration dbt hors container)
python scripts/seed_local_warehouse.py

# dbt (dans le container)
docker compose exec airflow-worker dbt debug \
  --project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt
docker compose exec airflow-worker dbt build \
  --project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt
```

**Makefile (Git Bash / WSL / Linux/macOS)** — `make help` pour la liste complète :

| Cible | Action |
|---|---|
| `make env` | Crée `.env` depuis `.env.example` (no-op si existe déjà) |
| `make build` | Build l'image Airflow custom |
| `make init` | Bootstrap DB + admin + pools (`env` inclus) |
| `make up` | Démarre le cluster en arrière-plan |
| `make down` | Arrête et supprime les containers |
| `make restart` | `down` + `up` |
| `make stop` | Arrête sans supprimer les containers |
| `make logs` | Tail les logs de tous les services |
| `make ps` | Statut des services |
| `make shell` | Shell bash dans le worker |
| `make lint` | `ruff check` |
| `make format` | `ruff format` + `ruff check --fix` |
| `make typecheck` | `mypy` |
| `make test` | Suite complète pytest |
| `make test-dags` | Tests DAG-integrity seulement |
| `make dbt-debug` | `dbt debug` dans le container |
| `make dbt-build` | `dbt build` dans le container |
| `make dbt-docs` | `dbt docs generate` dans le container |
| `make clean` | Supprime caches, `include/dbt/target`, fichiers DuckDB |
| `make reset` | `down --volumes` + `clean` (clean slate complet) |

---

## Configuration (variables d'environnement)

| Variable | Défaut | Description |
|---|---|---|
| `HR_ENV` | `local` | `local` = DuckDB, `production` = BigQuery |
| `DUCKDB_PATH` | `/opt/airflow/include/data/warehouse.duckdb` | Fichier DuckDB |
| `HR_RAW_DATA_DIR` | `/opt/airflow/include/data/raw` | Dossier CSVs source |
| `DBT_PROJECT_DIR` | `/opt/airflow/include/dbt` | Projet dbt |
| `GCP_PROJECT` | _(vide)_ | Requis seulement si `HR_ENV=production` |

Toujours lire la config via `Settings.from_env()` — jamais `os.getenv()` direct dans les DAGs.

---

## Conventions de code

1. **DAGs = orchestration uniquement** — pas de logique SQL/pandas dans `dags/`
2. **Imports** : `from hr_pipeline.config import Settings` (package installé, pas path hack)
3. **DEFAULT_ARGS** : toujours importé depuis `common.py`, jamais redéfini
4. **Assets Airflow 3** : définis dans `src/hr_pipeline/assets.py`, référencés dans les DAGs
5. **Type hints** obligatoires dans `src/hr_pipeline/` (mypy strict)
6. **Tests** : colocalisés dans `tests/`, nommés `test_<module>.py`
7. **Ligne max** : 100 caractères (ruff)
8. **Langue** : code et docstrings en anglais

---

## Limites à respecter

- **Ne jamais committer `.env`** — secrets locaux uniquement
- **DuckDB par défaut** — éviter tout appel BigQuery/GCP sans nécessité (coûts)
- **Pas de `os.getenv()` direct** dans les DAGs — passer par `Settings`
- **Ne pas modifier `config/airflow_local_settings.py`** sans comprendre les cluster policies
- **Ne pas modifier `config/airflow.cfg`** manuellement — géré par le container
- **Tests doivent passer** avant tout commit — `pytest -m "not dags"` au minimum
- **Diff minimal** — ne pas refactorer ce qui n'est pas dans le scope de la tâche
- **Pre-commit hooks actifs** (`.pre-commit-config.yaml`) : ruff lint/format + détection de clés privées — ils bloquent le commit si les checks échouent

## CI/CD

`.github/workflows/ci.yml` — 4 jobs indépendants sur push/PR vers `main` :

| Job | Ce qu'il prouve |
|---|---|
| `lint` | `ruff check` + `ruff format --check` |
| `unit-tests` | `pytest -m "not dags"` — rapide, hermétique |
| `dbt-build` | dbt seed + run + test sur DuckDB (seed via `scripts/seed_local_warehouse.py`) |
| `dag-validation` | Parse tous les DAGs via DagBag (Airflow + Cosmos requis) |

`.github/workflows/docs.yml` — déploie le site MkDocs sur GitHub Pages.  
`.github/dependabot.yml` — mises à jour automatiques des dépendances GitHub Actions.

---

## Patterns à suivre

### Nouveau DAG
```python
from common import DEFAULT_ARGS, DEFAULT_START_DATE
from hr_pipeline.assets import MY_ASSET

with DAG(
    dag_id="my_dag",
    default_args=DEFAULT_ARGS,
    start_date=DEFAULT_START_DATE,
    schedule=[MY_ASSET],  # asset-driven, pas de cron
    catchup=False,
    tags=["hr", "my-layer"],
) as dag:
    ...
```

### Nouvelle tâche avec Settings
```python
@task
def my_task() -> None:
    settings = Settings.from_env()
    # utiliser settings.duckdb_path, settings.raw_schema, etc.
```

### Nouveau check qualité
Ajouter dans `src/hr_pipeline/quality/checks.py`, référencer dans `suite.py`.
Le `DataQualityCheckOperator` prend une liste de checks — ne pas dupliquer la logique dans le DAG.
