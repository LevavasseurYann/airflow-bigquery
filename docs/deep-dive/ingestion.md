# Module deep-dive : `src/hr_pipeline/ingestion.py`

> Ce module implémente la couche d'ingestion du pipeline HR.
> Il est entièrement indépendant d'Airflow — c'est un choix délibéré.

---

## 1. Pourquoi ce fichier existe

Le pipeline charge des extraits CSV régionaux (EU, US, APAC) dans une table
`raw_hr.employees` que le projet dbt lira ensuite. Il aurait été possible
d'écrire cette logique directement dans le DAG Airflow. Ce ne serait pas une
erreur fonctionnelle — mais ce serait une erreur architecturale.

**Le problème du code dans les DAGs.** Le scheduler Airflow parse les fichiers
du dossier `dags/` en permanence — toutes les quelques secondes. Si la logique
métier y réside, elle ne peut pas être testée sans instancier un contexte
Airflow, ce qui rend les tests lents, fragiles et difficiles à lancer en CI.
En séparant le code dans un package Python ordinaire (`hr_pipeline`), on peut
écrire des tests `pytest` qui s'exécutent en millisecondes, sans aucune
dépendance à Docker ou à Airflow.

**Règle fondamentale du projet :** les DAGs orchestrent. `hr_pipeline` implémente.

Ce module réalise trois étapes qui suivent le **pattern extract → land → load** :

1. `discover_source_files` — trouve les CSV régionaux sur le disque.
2. `extract_to_parquet` — valide et atterrit un seul fichier en Parquet.
3. `load_raw_employees` — consolide tous les Parquet en une table unique.

---

## 2. Vue d'ensemble

```
CSV (EU)  ┐
CSV (US)  ├── discover_source_files()
CSV (APAC)┘
              │
              ▼ [SourceFile, SourceFile, SourceFile]
              │
    ┌─────────┼─────────┐  (en parallèle, via dynamic task mapping)
    ▼         ▼         ▼
extract()  extract()  extract()   → Parquet sur le filesystem local
    └─────────┼─────────┘
              │
              ▼ [RegionExtract, RegionExtract, RegionExtract]
              │
     load_raw_employees()
              │
              ▼
    raw_hr.employees (DuckDB ou BigQuery)
```

!!! note "Zéro import Airflow dans ce fichier"
    `ingestion.py` importe uniquement `pandas`, `pathlib` et `hr_pipeline.warehouse`.
    Cette contrainte est vérifiable en un coup d'œil : si un import `airflow`
    apparaît ici, c'est un bug architectural.

---

## 3. Walkthrough complet

### 3.1 La constante `REQUIRED_COLUMNS`

```python
REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {"employee_id", "email", "hire_date", "updated_at", "is_active", "department", "salary"}
)
```

**Pourquoi `frozenset` et non `set` ou `list` ?**

Deux raisons liées mais distinctes.

La première est sémantique : un ensemble de colonnes obligatoires est une
constante. `frozenset` rend cette immuabilité explicite et vérifiable au
typage — un `set` ordinaire pourrait être muté accidentellement par du code
appelant.

La seconde est opérationnelle : la validation de schéma s'écrit
`REQUIRED_COLUMNS - set(frame.columns)`, une soustraction d'ensembles. Cette
opération renvoie directement les colonnes manquantes, en O(n). Avec une
`list`, il faudrait une compréhension de liste. Avec un `frozenset`, c'est une
ligne idiomatique Python.

**Pourquoi ces colonnes spécifiquement ?**

Ce sont les colonnes sur lesquelles le projet dbt a un contrat via la
définition `source` dans `include/dbt/models/staging/sources.yml`. Si un HRIS
régional modifie son schéma d'export (renommage, suppression), la validation
échoue à l'ingestion — pas silencieusement dans un modèle dbt des heures plus
tard.

---

### 3.2 `IngestionError`

```python
class IngestionError(RuntimeError):
    """Raised when a source file fails structural validation."""
```

**Pourquoi une exception personnalisée ?**

`RuntimeError` est la classe mère parce que les erreurs d'ingestion sont des
erreurs d'exécution (pas des `ValueError` de mauvais argument ni des
`TypeError`). Mais sous-classer permet à un appelant de distinguer
`IngestionError` de toute autre `RuntimeError` par un `except IngestionError`.
Dans les tests, on peut asserter `pytest.raises(IngestionError)` sans risque
d'attraper une erreur non liée.

---

### 3.3 Dataclass `SourceFile`

```python
@dataclass(frozen=True, slots=True)
class SourceFile:
    path: Path
    region: str

    @classmethod
    def from_path(cls, path: Path) -> SourceFile:
        stem = path.stem
        region = stem.split("_", 1)[1] if "_" in stem else "unknown"
        return cls(path=path, region=region)
```

**`frozen=True`** : une fois créé, l'objet ne peut plus être modifié.
C'est la règle générale pour les value objects — un fichier source ne change
pas de chemin en cours de traitement.

**`slots=True`** : Python alloue un `__dict__` par défaut pour chaque instance.
`slots=True` remplace ce dictionnaire par des slots déclarés statiquement, ce
qui réduit l'empreinte mémoire et accélère l'accès aux attributs. Pour une
dataclass construite des centaines de fois par run, c'est une bonne habitude.

**`from_path`** : la convention de nommage `employees_<region>.csv` encode la
région dans le nom du fichier. `split("_", 1)[1]` coupe au premier underscore
uniquement (`maxsplit=1`) — ce qui garantit qu'un nom comme
`employees_north_america.csv` produit `"north_america"` et non `"america"`.
Le fallback `"unknown"` évite un crash si un fichier mal nommé passe le glob.

---

### 3.4 Dataclass `RegionExtract` et le pattern `as_dict` / `from_dict`

```python
@dataclass(frozen=True, slots=True)
class RegionExtract:
    region: str
    parquet_path: str
    row_count: int

    def as_dict(self) -> dict[str, object]:
        return {"region": self.region, "parquet_path": self.parquet_path, "row_count": self.row_count}

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RegionExtract:
        return cls(
            region=str(payload["region"]),
            parquet_path=str(payload["parquet_path"]),
            row_count=int(payload["row_count"]),
        )
```

!!! warning "La contrainte XCom d'Airflow"
    Airflow sérialise les valeurs de retour des `@task` en JSON pour les stocker
    dans XCom (la base de métadonnées Airflow). Les dataclasses Python ne sont
    **pas** JSON-sérialisables nativement. Si une tâche retourne une instance de
    `RegionExtract` directement, Airflow lève une `TypeError` à la sérialisation.

**Le pattern `as_dict` / `from_dict`** résout ce problème proprement. La tâche
`extract_region` appelle `extract.as_dict()` avant de retourner, produisant un
dict JSON-safe. La tâche `load_raw` reçoit ce dict via XCom et appelle
`RegionExtract.from_dict(item)` pour reconstruire l'objet typé.

Ce pattern présente un avantage secondaire : le cast explicite dans `from_dict`
(`int(payload["row_count"])`) protège contre la désérialisation JSON qui retourne
parfois des types inattendus (un entier sauvegardé comme string par un
sérialiseur exotique).

**Pourquoi `parquet_path` est `str` et non `Path` ?**

`Path` n'est pas JSON-sérialisable. Stocker le chemin en string dans le
dataclass évite de devoir le convertir à chaque sérialisation. La conversion
vers `Path` n'a lieu que là où c'est nécessaire — dans `load_raw_employees`.

---

### 3.5 `discover_source_files`

```python
def discover_source_files(raw_dir: Path) -> list[SourceFile]:
    files = sorted(Path(raw_dir).glob("employees_*.csv"))
    if not files:
        raise IngestionError(f"No source files matching 'employees_*.csv' found in {raw_dir}.")
    logger.info("Discovered %s source file(s): %s", len(files), [f.name for f in files])
    return [SourceFile.from_path(path) for path in files]
```

**`sorted()`** : `glob` ne garantit pas un ordre stable entre systèmes
d'exploitation. Trier assure que la liste retournée — et donc l'ordre des
tâches mappées dans le DAG — est déterministe. C'est important pour la
lisibilité des logs et pour la reproductibilité des runs.

**L'échec early** (`raise IngestionError`) : si aucun CSV n'est trouvé,
continuer est inutile. Échouer tôt avec un message clair (`"No source files
… found in /path"`) est infiniment plus utile qu'un `KeyError` ou un
`EmptyDataFrame` plusieurs étapes plus loin.

**`Path(raw_dir).glob(...)`** : on re-wrape explicitement en `Path` même si
`raw_dir` est déjà un `Path`. Cela absorbe les cas où un appelant passerait
une string (ce qui arrive en tests ou quand la valeur vient d'une variable
d'environnement non encore convertie).

---

### 3.6 `_validate`

```python
def _validate(frame: pd.DataFrame, source_file: SourceFile) -> None:
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise IngestionError(
            f"{source_file.path.name} is missing required column(s): {sorted(missing)}"
        )
    if frame.empty:
        raise IngestionError(f"{source_file.path.name} contains no rows.")
    null_ids = int(frame["employee_id"].isna().sum())
    if null_ids:
        raise IngestionError(
            f"{source_file.path.name} has {null_ids} row(s) with a null employee_id."
        )
```

**Préfixe `_`** : convention Python pour une fonction privée au module. Elle
n'est pas importée par le DAG et n'apparaît pas dans l'API publique.

**Ordre des checks** :

1. Colonnes manquantes — si le schéma est cassé, les checks suivants feraient
   des `KeyError` peu informatifs. On vérifie le contrat de schéma en premier.
2. DataFrame vide — un CSV vide est un problème de source, pas un schéma cassé.
3. `employee_id` nulls — la clé naturelle ne peut pas être nulle : la
   déduplication et les jointures dbt en dépendent.

**`sorted(missing)`** dans le message d'erreur : `frozenset - set` retourne un
`set`, dont l'ordre n'est pas garanti. Trier rend les messages déterministes,
ce qui facilite les assertions dans les tests (`assert "column_x" in str(exc)`).

**`int(frame["employee_id"].isna().sum())`** : `.sum()` retourne un `numpy.int64`.
Le cast `int()` produit un Python natif lisible dans les f-strings et les logs.

---

### 3.7 `extract_to_parquet` — le cœur du pattern "land"

```python
def extract_to_parquet(source_file: SourceFile, staging_dir: Path) -> RegionExtract:
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    frame = pd.read_csv(source_file.path, dtype=str)
    _validate(frame, source_file)

    frame["_source_region"] = source_file.region
    frame["_source_file"] = source_file.path.name
    frame["_ingested_at"] = pd.Timestamp.now(tz="UTC")

    destination = staging_dir / f"{source_file.path.stem}.parquet"
    frame.to_parquet(destination, index=False)
    ...
```

**`dtype=str` — la règle du layer "raw"**

C'est la décision de design la plus importante de ce module. Lire le CSV avec
`dtype=str` empêche pandas d'inférer les types (`int`, `float`, `datetime`).
Chaque colonne reste une string.

Pourquoi ? Parce que **le typage est la responsabilité du layer dbt staging**,
pas de l'ingestion. Cette règle crée un contrat clair entre les couches :

- L'ingestion garantit que les données arrivent **complètes et intègres**
  (colonnes présentes, clé naturelle non nulle).
- Le layer staging dbt garantit que les types sont **corrects** (`hire_date`
  est un `DATE`, `salary` est un `NUMERIC`).

Si l'ingestion castait `salary` en `float`, un fichier avec `"N/A"` dans
cette colonne ferait planter `pd.read_csv` au niveau ingestion. Avec `dtype=str`,
le fichier est atterri tel quel — et c'est le modèle dbt staging qui gère le
cas `"N/A"` avec un `CAST(NULLIF(salary, 'N/A') AS NUMERIC)`.

!!! tip "Principe de responsabilité unique appliqué aux couches de données"
    Ingestion = completeness.  Staging dbt = conformity.  Intermediate = business logic.
    Chaque couche a une responsabilité, une seule.

**Parquet comme format de staging (pas CSV, pas write direct en DB)**

Trois raisons expliquent ce choix :

1. **Columnar et schématisé** : Parquet encode les types natifs. Même si toutes
   les colonnes sont des strings ici, Parquet préserve cela fidèlement — un CSV
   de staging introduirait une ambiguïté de re-parsing.

2. **Résistance aux pannes** : les tâches `extract_region` tournent en parallèle
   (dynamic task mapping). Si un worker crashe entre l'écriture du Parquet et la
   fin de la tâche, le fichier est sur le disque. La tâche peut être relancée et
   `extract_to_parquet` est **idempotente** (écrase le fichier existant). Un write
   direct en DuckDB avec `mode="append"` ne serait pas idempotent — une relance
   doublerait les données.

3. **DuckDB writer lock** : DuckDB n'autorise qu'un seul writer à la fois. Les
   tâches `extract_region` tournent en parallèle — si elles écrivaient
   directement en DuckDB, elles se disputeraient le lock. En atterrissant en
   Parquet, chaque tâche travaille sur son propre fichier, sans contention. Le
   write en DB n'a lieu qu'en une seule tâche séquentielle (`load_raw`).

**Les trois colonnes d'audit**

```python
frame["_source_region"] = source_file.region    # "eu", "us", "apac"
frame["_source_file"]   = source_file.path.name # "employees_eu.csv"
frame["_ingested_at"]   = pd.Timestamp.now(tz="UTC")
```

Ces colonnes répondent à la question **"d'où vient cette ligne ?"** — ce qu'on
appelle la **data lineage** au niveau row. En production, si une anomalie est
détectée sur un employé, on peut remonter immédiatement à la région source et
au timestamp d'ingestion.

Le préfixe `_` signale au modèle dbt staging que ce sont des colonnes
d'infrastructure, pas des données métier. Le modèle staging les utilise (pour
filtrer par région ou pour un audit) mais ne les expose pas dans les marts.

**`index=False`**

L'index pandas est un artefact technique interne (0, 1, 2…). L'écrire dans
le Parquet ajouterait une colonne sans valeur métier. `index=False` est la
bonne pratique par défaut.

---

### 3.8 `load_raw_employees` — consolidation et déduplication

```python
def load_raw_employees(
    warehouse: Warehouse, raw_schema: str, extracts: list[RegionExtract]
) -> IngestionResult:
    if not extracts:
        raise IngestionError("load_raw_employees called with no extracts.")

    combined = pd.concat(
        [pd.read_parquet(extract.parquet_path) for extract in extracts], ignore_index=True
    )
    rows_before = len(combined)

    combined["updated_at"] = pd.to_datetime(combined["updated_at"])
    combined = (
        combined.sort_values("updated_at")
        .drop_duplicates(subset="employee_id", keep="last")
        .reset_index(drop=True)
    )
    rows_loaded = warehouse.load_dataframe(raw_schema, RAW_EMPLOYEES_TABLE, combined, mode="replace")
    rows_deduplicated = rows_before - rows_loaded
    ...
```

**Pourquoi `pd.to_datetime()` avant le sort ?**

C'est un piège classique. Tous les types en sortie de `extract_to_parquet` sont
des strings (`dtype=str`). Trier des strings ISO 8601 _semble_ fonctionner
parce que `"2026-05-01"` < `"2026-05-10"` lexicographiquement. Mais les HRIS
régionaux produisent des formats inconsistants : `"2026-5-1 9:00:00"` (sans
zéro de padding) ne se trie pas correctement en string car `"2026-5"` >
`"2026-05"` lexicographiquement.

Parser en `datetime` d'abord garantit un ordre chronologique correct quelle que
soit la région source.

**Last-write-wins : la sémantique de déduplication**

```python
combined.sort_values("updated_at")          # ordre croissant
    .drop_duplicates(subset="employee_id", keep="last")  # garde le plus récent
```

Un même employé peut apparaître dans plusieurs régions (cas d'un transfert
interne). `sort_values` ascending + `keep="last"` signifie que l'enregistrement
avec le `updated_at` le plus récent survit. C'est la sémantique **last-write-wins** :
la mise à jour la plus récente gagne, quelle que soit la région.

!!! warning "Hypothèse implicite"
    Ce comportement suppose que `updated_at` est fiable dans tous les HRIS
    régionaux. Si un système régional a un horloge décalée, le mauvais
    enregistrement peut "gagner". Le check de qualité `assert_recent_source_updates`
    dans le projet dbt surveille ce risque.

**`mode="replace"`**

La table `raw_hr.employees` est **entièrement remplacée** à chaque run. Ce
choix rend le pipeline **idempotent** : relancer le DAG produit exactement
le même résultat. L'alternative (append + déduplication SQL) serait plus
complexe et moins prévisible.

**`rows_deduplicated = rows_before - rows_loaded`**

Cette métrique apparaît dans les logs et dans le XCom retourné par `load_raw`.
Elle est utile pour le monitoring : si `rows_deduplicated` est soudainement
élevé (beaucoup de doublons inter-régions), c'est un signal d'alerte.

---

## 4. Connexions

| Direction | Module / Classe |
|---|---|
| **Appelé par** | `dags/ingest_hr_sources.py` — les trois tâches `discover_sources`, `extract_region`, `load_raw` |
| **Appelle** | `hr_pipeline.warehouse.Warehouse.load_dataframe` pour le write final |
| **Contrat vers** | `include/dbt/models/staging/sources.yml` — définit les mêmes colonnes que `REQUIRED_COLUMNS` |
| **Testé par** | `tests/test_ingestion.py` |

---

## 5. Pièges & gotchas

**Piège 1 — `dtype=str` modifie aussi `_ingested_at`**

`_ingested_at` est ajouté comme `pd.Timestamp`, mais le reste du DataFrame est
en strings. Parquet stockera `_ingested_at` comme `datetime64[ns, UTC]` et les
autres colonnes comme `object` (string). Ce comportement est intentionnel et
n'est pas un problème — mais il peut surprendre en inspection.

**Piège 2 — `staging_dir` n'est pas nettoyé**

Les fichiers Parquet de staging (`include/data/_staging/`) ne sont pas
supprimés après le run. Ils persistent entre les DAG runs. C'est voulu : ils
servent de "sauvegarde locale" en cas de relance. La task de maintenance
`platform_maintenance` peut les nettoyer périodiquement (voir
`dags/platform_maintenance.py`).

**Piège 3 — Lecture de tous les Parquet en mémoire**

```python
combined = pd.concat(
    [pd.read_parquet(extract.parquet_path) for extract in extracts], ignore_index=True
)
```

Tous les Parquet sont lus en mémoire avant le concat. Pour le dataset HR
actuel (< 1M lignes), c'est acceptable. Pour des volumes plus importants, la
bonne approche serait d'utiliser `duckdb.read_parquet()` directement, qui
opère en streaming sans charger tout en RAM.

**Piège 4 — `load_dataframe` retourne `len(frame)`, pas les rows réellement écrits**

Dans `DuckDBWarehouse.load_dataframe`, la valeur de retour est `len(frame)` —
le count du DataFrame passé, pas une confirmation de la DB. Pour DuckDB, les
deux sont équivalents (le write est synchrone). Pour BigQuery, le job est
asynchrone mais `.result()` est appelé, donc le count reste fiable.

**Piège 5 — Le `region` extrait du nom de fichier peut être `"unknown"`**

Si un fichier `employees.csv` (sans underscore) est déposé dans le dossier raw,
`SourceFile.from_path` lui assigne la région `"unknown"`. Il passera la
validation si ses colonnes sont correctes. C'est un edge case à surveiller en
monitoring.
