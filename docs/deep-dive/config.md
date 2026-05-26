# Deep dive — `hr_pipeline.config`

> Fichier source : `src/hr_pipeline/config.py`

## Pourquoi ce module existe

Le pipeline tourne dans deux contextes radicalement différents :

| Contexte | Warehouse | Credentials |
|---|---|---|
| `HR_ENV=local` (défaut) | DuckDB — fichier local, zéro serveur | aucun |
| `HR_ENV=production` | Google BigQuery | Application Default Credentials (ADC) |

**Le code DAG ne doit rien savoir de cette différence.** `config.py` encapsule cette résolution en un seul endroit : on lit l'environnement une fois, on construit un objet `Settings` immuable, et tout le reste du code reçoit cet objet par paramètre.

## `Environment` (StrEnum)

```python
class Environment(StrEnum):
    LOCAL = "local"
    PRODUCTION = "production"
```

`StrEnum` (Python 3.11+) : les membres sont à la fois des `str` et des constantes d'enum. `Environment("local")` fonctionne directement depuis la valeur lue dans l'env.

## `Settings` — `frozen=True, slots=True`

```python
@dataclass(frozen=True, slots=True)
class Settings:
    ...
```

- **`frozen=True`** : l'instance est immuable une fois construite. Pas de mutation à distance.
- **`slots=True`** : plus rapide et plus économe en mémoire (supprime le `__dict__` par instance).

### Champs

| Champ | Type | Rôle |
|---|---|---|
| `environment` | `Environment` | Cible de déploiement |
| `duckdb_path` | `Path` | Chemin vers le fichier `.duckdb` |
| `raw_data_dir` | `Path` | Dossier des CSV sources |
| `dbt_project_dir` | `Path` | Racine du projet dbt embarqué |
| `dbt_profiles_dir` | `Path` | Dossier `profiles.yml` dbt |
| `gcp_project` | `str \| None` | Projet GCP (production uniquement) |
| `bq_location` | `str` | Région BigQuery (défaut : `EU`) |
| `raw_schema` | `str` | Schéma/dataset de landing (défaut : `raw_hr`) |
| `marts_schema` | `str` | Schéma/dataset des marts (défaut : `marts`) |
| `freshness_warn_hours` | `int` | Seuil d'alerte fraîcheur données (12h) |
| `freshness_error_hours` | `int` | Seuil d'erreur fraîcheur données (24h) |

### Propriétés calculées

**`is_production`** → `bool` — raccourci lisible.

**`dbt_target`** → `str` — retourne `"prod"` ou `"dev"` pour Cosmos/dbt.

**`staging_dir`** → `Path`
```python
return self.raw_data_dir.parent / "_staging"
```
Dossier scratch pour les Parquet intermédiaires. Placé *à côté* de `raw_data_dir` pour éviter que dbt ne le ramasse. Les tâches d'extraction parallèles écrivent ici sans contention DuckDB.

### `validate()`
```python
def validate(self) -> None:
    if self.is_production and not self.gcp_project:
        raise ValueError("HR_ENV=production requires GCP_PROJECT to be set.")
```
Fail-fast appelé automatiquement dans `from_env()`.

### `from_env()` — le seul constructeur

| Variable | Défaut local |
|---|---|
| `HR_ENV` | `local` |
| `DUCKDB_PATH` | `/opt/airflow/include/data/warehouse.duckdb` |
| `HR_RAW_DATA_DIR` | `/opt/airflow/include/data/raw` |
| `DBT_PROJECT_DIR` | `/opt/airflow/include/dbt` |
| `GCP_PROJECT` | _(vide — None)_ |
| `BQ_LOCATION` | `EU` |
| `BQ_DATASET_RAW` | `raw_hr` |

!!! note "Le `or None` sur `GCP_PROJECT`"
    `os.getenv("GCP_PROJECT") or None` — si la variable est définie mais vide (`""`),
    `"" or None` retourne `None`, ce qui déclenche correctement `validate()`.

## Pattern d'utilisation

```python
@task
def my_task() -> None:
    settings = Settings.from_env()  # runtime, pas parse time
    with get_warehouse(settings) as wh:
        ...
```

!!! warning "Règle parse-time"
    `Settings.from_env()` ne doit **jamais** être appelé au niveau module d'un DAG
    (hors `transform_hr_dbt.py` — exception documentée dans ADR-0007).
    Le DAG processor d'Airflow parse les fichiers DAG constamment.
    En production, `validate()` lèverait une exception si `GCP_PROJECT` manque
    dans l'environnement du scheduler → DAG en erreur d'import.

## Références ADR

- [`ADR-0004`](../adr/0004-duckdb-local-bigquery-production.md) — décision local vs production
- [`ADR-0007`](../adr/0007-dag-parse-time-vs-run-time-configuration.md) — règle parse-time
