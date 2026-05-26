# Deep Dive — `src/hr_pipeline/warehouse.py`

> Annotation complète de la couche d'abstraction warehouse. Chaque pattern a une raison d'être.

---

## Pourquoi ce fichier existe

Le pipeline doit écrire des DataFrames pandas dans DuckDB en local et dans BigQuery en production. Sans abstraction, chaque module d'ingestion ou de qualité contiendrait une branche `if settings.is_production:` suivie de code totalement différent. Le résultat : doublement du code, tests difficiles à isoler, et un couplage fort vers deux SDKs incompatibles.

`warehouse.py` applique le **pattern Adapter** : une interface abstraite `Warehouse` définit le contrat, deux implémentations concrètes (`DuckDBWarehouse`, `BigQueryWarehouse`) l'honorent. Le code appelant n'interagit qu'avec `Warehouse` — il ignore totalement quel backend est derrière.

!!! note "Décision de design"
    Ce fichier est la seule frontière où DuckDB et BigQuery se touchent. En dehors de ce module,
    le mot "DuckDB" ou "BigQuery" n'apparaît jamais dans la logique métier. Tout test qui mocke
    `get_warehouse()` peut tester la logique métier sans aucune dépendance cloud.

---

## Vue d'ensemble

`warehouse.py` expose quatre éléments publics :

- `LoadMode` : type littéral `"replace" | "append"` pour la politique d'écriture.
- `Warehouse` : classe abstraite (ABC) définissant l'interface complète.
- `DuckDBWarehouse` / `BigQueryWarehouse` : implémentations concrètes.
- `get_warehouse(settings)` : factory function, point d'entrée unique pour obtenir un warehouse.

Le module inclut aussi deux utilitaires internes : `_safe_id()` (protection SQL injection) et `WarehouseSettings` (Protocol pour éviter les imports circulaires).

---

## Walkthrough complet

### `LoadMode` — type littéral

```python
LoadMode = Literal["replace", "append"]
```

Un `Literal` type est préféré à un `Enum` ici car la valeur est directement mappée sur des strings SQL (`WRITE_TRUNCATE`, `INSERT INTO`). L'appelant peut écrire `mode="replace"` sans importer un enum — moins de friction pour une valeur purement locale à ce module.

---

### `_safe_id()` — garde contre l'injection SQL

```python
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _safe_id(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Unsafe SQL identifier {name!r}: only letters, digits and underscores are allowed."
        )
    return name
```

!!! warning "Pourquoi les identifiants SQL ne peuvent pas être paramétrés"
    Les paramètres SQL (`?`, `%s`, `$1`) protègent les **valeurs** dans les clauses `WHERE`, `INSERT`, etc.
    Mais les **noms de tables et de schémas** doivent être interpolés directement dans la string SQL —
    il n'existe pas de mécanisme standard de paramétrage pour les identifiants.

    Sans `_safe_id()`, un nom de schéma comme `raw_hr; DROP TABLE employees--` serait interpolé
    tel quel dans `CREATE SCHEMA IF NOT EXISTS "raw_hr; DROP TABLE employees--"`.
    La regex interdit tout caractère hors `[A-Za-z0-9_]` avec l'obligation de commencer par une lettre ou `_`.

La regex est compilée une fois au niveau module (`_IDENTIFIER_RE`) et réutilisée à chaque appel — pas de recompilation.

---

### `WarehouseSettings` — Protocol anti-circular-import

```python
class WarehouseSettings(Protocol):
    is_production: bool
    gcp_project: str | None
    bq_location: str
    duckdb_path: Path
```

`warehouse.py` a besoin de savoir si on est en production et où se trouve le fichier DuckDB. La solution naïve serait `from hr_pipeline.config import Settings`. Mais si `config.py` importait à son tour `warehouse.py`, on aurait un import circulaire.

**`Protocol`** résout ça : `warehouse.py` déclare uniquement les attributs dont il a besoin, sans importer `Settings`. N'importe quel objet qui possède ces quatre attributs satisfait le Protocol à la vérification mypy — duck typing structurel. `Settings` de `config.py` le satisfait sans l'implémenter explicitement.

!!! tip "Structural subtyping vs nominal subtyping"
    En Python, `Protocol` implémente le **structural subtyping** (duck typing vérifié statiquement).
    Contrairement à l'héritage (`class Settings(WarehouseSettings)`), aucune déclaration n'est
    nécessaire côté `Settings`. mypy valide la compatibilité à l'usage.

---

### `Warehouse` — ABC

```python
class Warehouse(ABC):
    @abstractmethod
    def ensure_namespace(self, schema: str) -> None: ...
    @abstractmethod
    def load_dataframe(self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace") -> int: ...
    @abstractmethod
    def fetch_scalar(self, sql: str) -> Any: ...
    @abstractmethod
    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]: ...
    @abstractmethod
    def table_exists(self, schema: str, table: str) -> bool: ...
    @abstractmethod
    def close(self) -> None: ...

    def row_count(self, schema: str, table: str) -> int:
        return int(self.fetch_scalar(
            f'SELECT count(*) FROM "{_safe_id(schema)}"."{_safe_id(table)}"'
        ))

    def __enter__(self) -> Warehouse:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
```

`load_dataframe` retourne `int` (nombre de lignes chargées) plutôt que `None`. Ce choix permet au code appelant de loguer ou valider le row count sans requête supplémentaire.

**`row_count`** est une méthode concrète sur l'ABC : elle s'implémente via `fetch_scalar`, qui est abstraite. Toutes les implémentations concrètes héritent gratuitement de `row_count` sans le réimplémenter. C'est le **Template Method pattern** — le comportement commun est défini sur la classe de base en termes de méthodes abstraites.

**Context manager (`__enter__`/`__exit__`)** : la gestion des connexions est intégrée dans l'ABC. Le pattern d'utilisation recommandé est toujours :

```python
with get_warehouse(settings) as wh:
    wh.load_dataframe(...)
# connexion fermée automatiquement, même en cas d'exception
```

Sans le context manager, une exception dans `load_dataframe` laisserait la connexion DuckDB ouverte — or DuckDB n'accepte qu'un seul writer à la fois. En production BigQuery, les connexions non fermées consomment des quotas.

---

### `DuckDBWarehouse`

#### Construction

```python
def __init__(self, path: Path, *, read_only: bool = False) -> None:
    import duckdb
    path.parent.mkdir(parents=True, exist_ok=True)
    self._path = path
    self._conn = duckdb.connect(str(path), read_only=read_only)
```

**Import lazy** (`import duckdb` à l'intérieur du `__init__`) : DuckDB n'est pas importé au niveau module. En production, la dépendance `duckdb` n'est pas installée dans l'image Docker BigQuery — l'import lazy garantit qu'on n'obtient pas `ModuleNotFoundError` au démarrage du scheduler.

`path.parent.mkdir(parents=True, exist_ok=True)` : crée le répertoire parent si nécessaire. Sans ça, la première exécution sur un volume Docker vide échouerait avec `FileNotFoundError` avant même d'ouvrir DuckDB.

**`read_only=True`** : DuckDB supporte plusieurs connexions en lecture simultanées, mais une seule en écriture. Les checks qualité (`data_quality_hr`) utilisent `read_only=True` pour pouvoir s'exécuter en parallèle avec d'autres tâches sans bloquer ni être bloqués.

#### `load_dataframe` — le pattern register/unregister

```python
def load_dataframe(self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace") -> int:
    self.ensure_namespace(schema)
    qualified = f'"{_safe_id(schema)}"."{_safe_id(table)}"'
    self._conn.register("_hr_frame", frame)
    try:
        if mode == "replace":
            self._conn.execute(f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _hr_frame")
        else:
            self._conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _hr_frame WHERE 1=0")
            self._conn.execute(f"INSERT INTO {qualified} SELECT * FROM _hr_frame")
    finally:
        self._conn.unregister("_hr_frame")
    return len(frame)
```

!!! tip "Pourquoi `register()` plutôt que `to_sql()` ?"
    DuckDB a une API `conn.register("name", dataframe)` qui expose le DataFrame pandas comme une
    vue virtuelle dans le moteur SQL **sans copie de données**. DuckDB lit directement le buffer
    numpy/arrow sous-jacent. `to_sql()` forcerait une sérialisation complète du DataFrame.

    Sur un DataFrame de 500k lignes, la différence de performance est mesurable (×5 à ×10).

Le bloc `try/finally` autour du `unregister` est critique : si `execute()` lève une exception, `_hr_frame` est toujours désenregistré. Sans ça, un retry de la tâche Airflow trouverait `_hr_frame` déjà enregistré et obtiendrait une erreur "view already exists".

**Mode `append`** : `CREATE TABLE IF NOT EXISTS ... WHERE 1=0` crée la table avec le bon schéma si elle n'existe pas (zéro ligne), puis `INSERT INTO` ajoute les données. C'est plus robuste que `CREATE TABLE IF NOT EXISTS` suivi d'un `INSERT` classique, car le schéma est inféré du DataFrame et non déclaré manuellement.

---

### `BigQueryWarehouse`

#### Construction et import lazy

```python
def __init__(self, project: str, location: str) -> None:
    from google.cloud import bigquery
    self._bigquery = bigquery
    self._project = project
    self._client = bigquery.Client(project=project, location=location)
```

`self._bigquery = bigquery` : le module bigquery est stocké comme attribut d'instance. Cela permet d'accéder à `self._bigquery.Dataset(...)`, `self._bigquery.LoadJobConfig(...)` etc. dans les autres méthodes **sans réimporter** à chaque appel. L'import lazy (dans `__init__`) garantit qu'en local, `google-cloud-bigquery` n'est pas nécessaire.

#### `ensure_namespace`

```python
def ensure_namespace(self, schema: str) -> None:
    _safe_id(schema)
    dataset = self._bigquery.Dataset(f"{self._project}.{schema}")
    dataset.location = self._client.location
    self._client.create_dataset(dataset, exists_ok=True)
```

`exists_ok=True` est l'équivalent BigQuery de `CREATE SCHEMA IF NOT EXISTS`. Sans ce flag, le code lèverait `Conflict` à chaque exécution après la première. `dataset.location = self._client.location` est obligatoire : BigQuery refuse de créer un dataset sans région explicitement définie sur l'objet Dataset (même si le client a une location par défaut).

#### `load_dataframe`

```python
def load_dataframe(self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace") -> int:
    self.ensure_namespace(schema)
    _safe_id(table)
    table_id = f"{self._project}.{schema}.{table}"
    disposition = "WRITE_TRUNCATE" if mode == "replace" else "WRITE_APPEND"
    job_config = self._bigquery.LoadJobConfig(write_disposition=disposition)
    self._client.load_table_from_dataframe(frame, table_id, job_config=job_config).result()
    return len(frame)
```

**`.result()`** : `load_table_from_dataframe` est asynchrone — il retourne un `LoadJob`. L'appel `.result()` bloque jusqu'à la fin du job et lève l'exception BigQuery en cas d'échec. Sans `.result()`, le code retournerait immédiatement avec un job encore en cours, et le pipeline Airflow croirait avoir réussi alors que le chargement n'est pas terminé.

**`WRITE_TRUNCATE`** vs **`WRITE_APPEND`** : BigQuery ne supporte pas `CREATE OR REPLACE TABLE ... AS SELECT` à la volée comme DuckDB. `WRITE_TRUNCATE` vide la table avant d'écrire — sémantiquement équivalent à `replace`.

#### `fetch_all`

```python
def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
    return [tuple(row.values()) for row in self._client.query(sql).result()]
```

`row.values()` retourne les valeurs de la `Row` BigQuery dans l'ordre des colonnes. La conversion en `tuple` normalise la réponse au même type que DuckDB (qui retourne des `tuple` nativement), garantissant que le code appelant n'a pas à gérer des types différents selon le backend.

---

### `get_warehouse()` — factory function

```python
def get_warehouse(settings: WarehouseSettings, *, read_only: bool = False) -> Warehouse:
    if settings.is_production:
        return BigQueryWarehouse(project=settings.gcp_project, location=settings.bq_location)
    return DuckDBWarehouse(path=settings.duckdb_path, read_only=read_only)
```

C'est le seul endroit du codebase où la sélection du backend est faite. Toute la logique métier appelle `get_warehouse(settings)` et travaille avec `Warehouse` — le backend est invisible.

`read_only` est ignoré pour BigQuery (BigQuery est toujours "read_only" depuis le point de vue des verrous) mais passé à DuckDB. C'est une asymétrie acceptable car le comportement observable est identique : les deux backends permettent des lectures concurrentes.

---

## Connexions

```
get_warehouse(settings)
    ↑ appelé par
    ├── src/hr_pipeline/ingestion.py   → write (mode="replace")
    ├── src/hr_pipeline/quality/checks.py  → read_only=True, fetch_scalar/fetch_all
    └── dags/platform_maintenance.py   → cleanup (fetch_all pour lister les tables)

DuckDBWarehouse
    ↓ utilise
    └── duckdb (import lazy dans __init__)

BigQueryWarehouse
    ↓ utilise
    └── google.cloud.bigquery (import lazy dans __init__)

Warehouse (ABC)
    ↓ implémente implicitement WarehouseSettings Protocol via Settings
    └── src/hr_pipeline/config.Settings
```

---

## Comparaison DuckDB vs BigQuery — comportements clés

| Opération | DuckDB | BigQuery |
|---|---|---|
| `ensure_namespace` | `CREATE SCHEMA IF NOT EXISTS` (SQL) | `create_dataset(exists_ok=True)` (API) |
| `load_dataframe` replace | `CREATE OR REPLACE TABLE ... AS SELECT` | `LoadJob(WRITE_TRUNCATE)` |
| `load_dataframe` append | `INSERT INTO` | `LoadJob(WRITE_APPEND)` |
| `fetch_scalar` | `.fetchone()[0]` | iterate sur `query().result()`, `return row[0]` |
| `table_exists` | `information_schema.tables` (SQL) | `get_table()` + `NotFound` exception |
| Concurrence | 1 writer, N readers (read_only flag) | N writers, N readers (pas de verrous) |
| Import | `import duckdb` (lazy) | `from google.cloud import bigquery` (lazy) |
| `close()` | `self._conn.close()` | `self._client.close()` |

---

## Pièges & gotchas

!!! warning "DuckDB : un seul writer à la fois"
    Si deux tâches Airflow tentent d'écrire dans DuckDB simultanément (même fichier `.duckdb`),
    la deuxième obtient `duckdb.IOException: Could not set lock on database file`.
    Solution : les tâches d'écriture doivent être séquentielles dans le DAG, ou utiliser
    des fichiers DuckDB séparés par worker.

!!! warning "BigQuery : `load_table_from_dataframe` requiert `pyarrow`"
    Le SDK `google-cloud-bigquery` utilise PyArrow pour sérialiser le DataFrame.
    Si `pyarrow` n'est pas installé, l'erreur arrive à l'exécution, pas à l'import.
    Vérifier que `pyarrow` est dans les dépendances de l'image Docker de production.

!!! warning "`_safe_id()` ne protège pas contre les collisions de noms"
    La regex valide la forme d'un identifiant, pas son existence ou sa sécurité métier.
    Un schéma nommé `information_schema` passerait la validation mais causerait des problèmes.
    C'est une protection anti-injection, pas un validateur de noms métier.

!!! tip "Tester sans BigQuery installé"
    ```python
    # Dans un test unitaire
    from unittest.mock import MagicMock, patch

    def test_load_calls_warehouse(monkeypatch):
        mock_wh = MagicMock(spec=Warehouse)
        monkeypatch.setattr("hr_pipeline.ingestion.get_warehouse", lambda s, **kw: mock_wh)
        # ... tester la logique d'ingestion sans DuckDB ni BigQuery
    ```

!!! tip "Inspecter le DuckDB local"
    ```bash
    docker compose exec airflow-worker python -c "
    import duckdb
    conn = duckdb.connect('/opt/airflow/include/data/warehouse.duckdb', read_only=True)
    print(conn.execute('SHOW ALL TABLES').fetchdf())
    "
    ```
