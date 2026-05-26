# DAG deep-dive : `dags/ingest_hr_sources.py`

> Ce DAG est le point d'entrée du pipeline HR.
> Il illustre trois features majeures d'Airflow 3 : TaskFlow API,
> dynamic task mapping, et asset-driven scheduling.

---

## 1. Pourquoi ce fichier existe

Le pipeline a besoin d'un orchestrateur qui lance l'ingestion quotidiennement,
parallélise le traitement par région, et notifie le DAG dbt en aval quand les
données sont prêtes. Ces trois besoins sont des problèmes d'orchestration, pas
des problèmes de données — et c'est précisément ce que fait ce DAG.

**Ce que le DAG ne fait pas.** Il ne lit pas de CSV. Il ne valide pas de
schéma. Il ne touche pas à DuckDB directement. Toute cette logique réside dans
`src/hr_pipeline/ingestion.py`. Le DAG ne fait qu'appeler les fonctions du
package, en leur passant la configuration issue de l'environnement.

Cette séparation est la **convention la plus importante du projet** : un DAG
qui contient de la logique métier est un DAG qui ne peut pas être testé
unitairement. Voir `tests/test_dag_integrity.py` pour la vérification de cette
invariant.

---

## 2. Vue d'ensemble

### Forme du DAG

```
discover_sources
      │
      ▼ [{"path": "...", "region": "eu"}, {"path": "...", "region": "us"}, ...]
      │
      ├──► extract_region[0]  (employees_eu.csv   → employees_eu.parquet)
      ├──► extract_region[1]  (employees_us.csv   → employees_us.parquet)
      └──► extract_region[2]  (employees_apac.csv → employees_apac.parquet)
                │
                ▼ (fan-in : attend toutes les instances mappées)
           load_raw  ──► émet l'Asset RAW_HR_EMPLOYEES
                              │
                              ▼ (déclenche automatiquement)
                    transform_hr_dbt (DAG suivant)
```

**Fan-out / fan-in** : `discover_sources` produit N éléments → N instances de
`extract_region` tournent en parallèle → `load_raw` attend le résultat de
toutes les instances. C'est le pattern classique de parallelisation en
orchestration.

### Localisation des fichiers clés

| Fichier | Rôle |
|---|---|
| `dags/ingest_hr_sources.py` | DAG lui-même |
| `dags/common.py` | DEFAULT_ARGS, start_date, failure callback |
| `src/hr_pipeline/ingestion.py` | logique d'ingestion (extract, validate, load) |
| `src/hr_pipeline/config.py` | `Settings.from_env()` |
| `src/hr_pipeline/assets.py` | définition de `RAW_HR_EMPLOYEES` |
| `src/hr_pipeline/warehouse.py` | abstraction DuckDB / BigQuery |

---

## 3. Walkthrough complet

### 3.1 `dags/common.py` — les fondations partagées

Avant de regarder le DAG lui-même, il faut comprendre `common.py` car tout DAG
du projet en dépend.

```python
PROJECT_OWNER = "data-platform"
DEFAULT_START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")
```

**`PROJECT_OWNER`** : la configuration de cluster dans
`config/airflow_local_settings.py` contient une **cluster policy** qui rejette
tout DAG dont `owner` est `"airflow"` (la valeur par défaut Airflow). Forcer
un owner explicite est une bonne pratique en équipe : en production, on peut
router les alertes par owner. Centraliser la valeur ici garantit qu'un nouveau
DAG ne peut pas oublier de la définir.

**`DEFAULT_START_DATE`** : toujours défini avec `pendulum.datetime(..., tz="UTC")`,
jamais avec `datetime.datetime(...)`. Pendulum force le timezone-awareness —
une `datetime` naive (sans timezone) dans Airflow cause des comportements
subtils selon la timezone du serveur. Fixer `tz="UTC"` une fois pour toutes
supprime cette ambiguïté.

**Pourquoi une date passée (2026-01-01) si tous les DAGs ont `catchup=False` ?**

La `start_date` est le point de référence qu'Airflow utilise pour calculer les
run IDs. Avec `catchup=False`, Airflow ne crée pas de runs pour les intervalles
passés — mais il a besoin d'une `start_date` valide pour savoir à partir de
quand le DAG est "actif". Une date dans le passé récent est la convention.

---

```python
def on_failure_callback(context: dict[str, Any]) -> None:
    _alert_log.error(
        "TASK FAILURE | dag=%s | task=%s | run=%s | try=%s",
        getattr(dag, "dag_id", "unknown"),
        getattr(task_instance, "task_id", "unknown"),
        context.get("run_id", "unknown"),
        getattr(task_instance, "try_number", "?"),
    )
```

**Pourquoi `getattr(..., "unknown")` plutôt qu'un accès direct ?**

Le callback `on_failure_callback` est appelé dans des contextes où certains
objets du contexte Airflow peuvent être `None` (race conditions rares, edge
cases de scheduling). `getattr(obj, attr, default)` est défensif — la callback
ne lève jamais d'exception elle-même, ce qui éviterait d'obscurcir l'erreur
originale.

**Pattern `TASK FAILURE | key=value | key=value`** : ce format structuré
(key=value, séparateur `|`) est conçu pour être greppé dans les logs :
`grep "TASK FAILURE" | grep "dag=ingest"`. En production, on peut parser
ces lignes avec un log shipper (Datadog, CloudWatch) et créer des alertes sur
le pattern.

---

```python
DEFAULT_ARGS: dict[str, Any] = {
    "owner": PROJECT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "on_failure_callback": on_failure_callback,
}
```

**La politique de retry — exponential backoff**

`retry_delay=2min` + `retry_exponential_backoff=True` + `max_retry_delay=15min`
définit une progression : 2 min → 4 min → 8 min → capped à 15 min.

Pourquoi ce pattern ? Un échec transitoire (réseau, lock DuckDB temporaire) se
résout souvent en quelques secondes. Retenter immédiatement est donc utile pour
le premier retry. Mais si l'erreur persiste, retenter toutes les 2 minutes
pendant 30 minutes ne sert à rien et consomme des ressources. Le cap à 15 min
évite de bloquer un slot worker trop longtemps.

**`depends_on_past: False`**

Si `depends_on_past: True`, une tâche refuserait de s'exécuter si la même
tâche du run précédent a échoué. Pour ce pipeline d'ingestion idempotent (la
table est remplacée à chaque run), ce comportement serait contre-productif :
un échec le lundi ne devrait pas bloquer le run du mardi. `False` est le
comportement correct ici.

---

### 3.2 Décorateur `@dag` — la configuration du DAG

```python
@dag(
    dag_id="ingest_hr_sources",
    description="Extract regional HR CSV extracts and load raw_hr.employees.",
    schedule="@daily",
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=_DOC,
    tags=["hr", "ingestion", "layer:raw"],
)
def ingest_hr_sources() -> None:
```

**`@dag` (TaskFlow API, Airflow 2.0+)**

La syntaxe traditionnelle était `with DAG("id", ...) as dag:`. Le décorateur
`@dag` est son équivalent moderne : la fonction décorée devient le DAG,
ses tâches internes sont définies comme fonctions imbriquées. C'est plus
Pythonique et évite l'anti-pattern `dag = DAG(...)` en variable globale.

**`schedule="@daily"`**

Ce DAG a un schedule temporel (`@daily`), contrairement aux DAGs `transform_hr_dbt`
et `data_quality_hr` qui sont déclenchés par des Assets. Pourquoi ? L'ingestion
est la source — elle n'a pas de producteur en amont dans ce pipeline. Elle doit
donc s'appuyer sur une horloge. Tous les autres DAGs sont déclenchés par les
données, pas par le temps.

!!! note "Asset + schedule temporel : les deux sont possibles"
    Un DAG peut combiner `schedule=[ASSET]` et une expression cron avec
    `timetable`. Ici, `@daily` suffit car l'ingestion suit un rythme journalier
    fixe.

**`catchup=False` — obligatoire pour un pipeline idempotent par replace**

La table `raw_hr.employees` est entièrement remplacée (`mode="replace"`) à
chaque run. Si `catchup=True`, Airflow créerait des runs historiques depuis
`start_date` jusqu'à aujourd'hui. Chaque run remplacerait la table avec les
données du jour concerné — et le dernier run "backfill" serait celui du
2026-01-01, écrasant les données actuelles avec des données vieilles de mois.
`catchup=False` est donc une nécessité logique, pas une préférence.

**`max_active_runs=1`**

DuckDB n'autorise qu'**un seul writer à la fois** sur un fichier. Si deux runs
du DAG se chevauchaient (run retardé + run du jour suivant), les deux
`load_raw` tenteraient d'écrire dans DuckDB simultanément — le second
obtiendrait un `duckdb.IOException: database is locked`. `max_active_runs=1`
empêche ce race condition au niveau du scheduler Airflow, avant même que les
workers soient impliqués.

**`doc_md=_DOC`**

La variable `_DOC` est un docstring Markdown affiché dans l'UI Airflow sous
l'onglet "Docs" du DAG. C'est une bonne pratique de documenter directement
dans le DAG ce qu'il fait, son schedule, et ses dépendances — les nouveaux
membres de l'équipe n'ont pas à lire le code pour comprendre le rôle du DAG.

**`tags=["hr", "ingestion", "layer:raw"]`**

Les tags permettent de filtrer les DAGs dans l'UI Airflow. La convention
`layer:raw` / `layer:staging` / `layer:marts` reproduit le vocabulaire des
layers de données directement dans l'orchestrateur — cohérence entre la
nomenclature dbt et la nomenclature Airflow.

---

### 3.3 Tâche `discover_sources` — la porte d'entrée du dynamic mapping

```python
@task
def discover_sources() -> list[dict[str, str]]:
    settings = Settings.from_env()
    files = discover_source_files(settings.raw_data_dir)
    return [{"path": str(f.path), "region": f.region} for f in files]
```

**`Settings.from_env()` à l'intérieur de la tâche — pas au niveau module**

C'est la règle la plus importante à retenir pour écrire des DAGs Airflow.

Le scheduler Airflow parse les fichiers de `dags/` en boucle toutes les
quelques secondes (le **DAG file processor**). Lors du parsing, le code au
niveau module est exécuté — mais dans un contexte minimal, sans les variables
d'environnement du worker, sans accès aux secrets, sans le runtime Docker
complet.

Si `Settings.from_env()` était appelé au niveau module :

```python
# MAUVAIS — appelé au parse time, dans le contexte du scheduler
settings = Settings.from_env()

@dag(...)
def ingest_hr_sources() -> None:
    ...
```

Le scheduler lèverait une `ValueError` ou trouverait des chemins invalides à
chaque cycle de parsing. Le DAG serait marqué comme "import error" dans l'UI.

En plaçant `Settings.from_env()` **à l'intérieur de chaque `@task`**, l'appel
n'a lieu qu'au **runtime**, dans le contexte du worker qui a accès aux bonnes
variables d'environnement.

!!! warning "Règle universelle : pas d'effets de bord au niveau module dans un DAG"
    Pas d'appels réseau, pas de lecture de fichiers, pas de connexions DB
    en dehors des fonctions `@task`. Le scheduler exécute le niveau module.
    Les workers exécutent les fonctions.

**Retour de `list[dict[str, str]]` et non de `list[SourceFile]`**

Voir la section sur XCom dans l'ingestion deep-dive. Les dataclasses ne sont
pas JSON-sérialisables. Le retour est un dict plat : `{"path": str, "region": str}`.

---

### 3.4 Tâche `extract_region` — le dynamic task mapping

```python
@task
def extract_region(source: dict[str, str]) -> dict[str, object]:
    settings = Settings.from_env()
    source_file = SourceFile(path=Path(source["path"]), region=source["region"])
    extract = extract_to_parquet(source_file, settings.staging_dir)
    return extract.as_dict()

# Dans le corps du DAG :
sources = discover_sources()
extracts = extract_region.expand(source=sources)
```

**`.expand()` — le dynamic task mapping d'Airflow 3**

`.expand(source=sources)` est la syntaxe du **dynamic task mapping**. Airflow
crée autant d'instances de `extract_region` qu'il y a d'éléments dans la liste
retournée par `discover_sources`.

Ce qui est remarquable ici, c'est que **le nombre de tâches n'est pas connu au
moment de l'écriture du DAG**. Lors du parsing, Airflow voit `extract_region.expand(...)`
et sait qu'il s'agit d'une tâche mappée — mais c'est seulement lors de
l'exécution de `discover_sources` que le nombre d'instances est déterminé.

**Conséquence pratique** : ajouter un quatrième fichier `employees_latam.csv`
dans le dossier raw crée automatiquement une quatrième tâche `extract_region[3]`
au prochain run. **Zéro modification de code nécessaire.**

**Nommage des instances mappées dans l'UI**

Dans l'UI Airflow, les instances apparaissent comme `extract_region[0]`,
`extract_region[1]`, `extract_region[2]`. Ce n'est pas idéal pour le debugging
(on préférerait `extract_region[eu]`). Airflow 3 introduit `expand_kwargs` pour
des clés nommées, mais c'est une optimisation secondaire.

**Fan-in automatique**

```python
extracts = extract_region.expand(source=sources)
load_raw(extracts)
```

`load_raw(extracts)` passe la sortie de toutes les instances mappées comme
argument. Airflow attend **toutes** les instances de `extract_region` avant de
déclencher `load_raw`. Si une seule instance échoue (par exemple, le CSV US
est corrompu), `load_raw` n'est pas déclenché — comportement correct pour
éviter un chargement partiel.

---

### 3.5 Tâche `load_raw` — producteur d'Asset

```python
@task(outlets=[RAW_HR_EMPLOYEES])
def load_raw(extracts: list[dict[str, object]]) -> dict[str, object]:
    settings = Settings.from_env()
    region_extracts = [RegionExtract.from_dict(item) for item in extracts]
    with get_warehouse(settings) as warehouse:
        result = load_raw_employees(warehouse, settings.raw_schema, region_extracts)
    return result.as_dict()
```

**`outlets=[RAW_HR_EMPLOYEES]` — le câblage asset-driven en un argument**

C'est la décision de design la plus élégante de ce DAG. `outlets=[RAW_HR_EMPLOYEES]`
déclare que cette tâche **produit** l'Asset `RAW_HR_EMPLOYEES`. Quand la tâche
se termine avec succès, Airflow marque cet Asset comme mis à jour dans sa base
de métadonnées.

Le DAG `transform_hr_dbt` est configuré avec `schedule=[RAW_HR_EMPLOYEES]` — il
se déclenche automatiquement quand l'Asset est mis à jour. C'est
l'**asset-driven scheduling** d'Airflow 3.

L'alternative historique était `TriggerDagRunOperator` ou `ExternalTaskSensor`.
Ces approches couplaient les DAGs par leur ID ou par un timestamp. L'Asset
coupling est supérieur : le déclencheur est **les données**, pas le temps.
Si l'ingestion rate son run de 2h, `transform_hr_dbt` attend — sans polling
infini ni timeout arbitraire.

!!! note "Où est défini `RAW_HR_EMPLOYEES` ?"
    Dans `src/hr_pipeline/assets.py`. Le fait que la définition soit dans le
    package `hr_pipeline` (et non dans `dags/`) garantit que producteur et
    consommateur importent le même objet Python — une source de bugs silencieux
    si les deux DAGs définissaient leur propre `Asset(name="raw_hr_employees")`
    séparément.

**`with get_warehouse(settings) as warehouse:`**

`get_warehouse` retourne soit un `DuckDBWarehouse` soit un `BigQueryWarehouse`,
selon `settings.is_production`. Les deux sont des context managers (`__enter__`
/ `__exit__` appellent `close()`). Le `with` garantit que la connexion est
fermée même si `load_raw_employees` lève une exception — important pour DuckDB
qui maintiendrait un lock sur le fichier si la connexion n'était pas fermée
proprement.

**Reconstruction des dataclasses avec `from_dict`**

```python
region_extracts = [RegionExtract.from_dict(item) for item in extracts]
```

`extracts` est une `list[dict]` reconstituée depuis XCom (voir le piège XCom
ci-dessus). `RegionExtract.from_dict` restaure les types corrects (notamment
`row_count` en `int`) avant de passer les objets à `load_raw_employees`. Cette
reconversion est obligatoire : JSON ne distingue pas `int` de `float`, et le
sérialiseur de XCom peut altérer les types numériques.

---

### 3.6 L'appel final `ingest_hr_sources()`

```python
@dag(...)
def ingest_hr_sources() -> None:
    ...

ingest_hr_sources()  # ← cette ligne est nécessaire
```

**Pourquoi appeler la fonction décorée ?**

Le décorateur `@dag` transforme `ingest_hr_sources` en une factory qui, quand
appelée, crée et enregistre l'objet DAG dans le registre global d'Airflow. Sans
cet appel, le DAG n'existe pas — la fonction est définie mais jamais exécutée.

C'est une source de confusion pour les débutants : le fichier DAG est du code
Python ordinaire, exécuté au parse time. L'appel `ingest_hr_sources()` est
l'acte de création du DAG.

---

## 4. Connexions

| Direction | Module / Élément |
|---|---|
| **Importe** | `common.DEFAULT_ARGS`, `common.DEFAULT_START_DATE` |
| **Importe** | `hr_pipeline.assets.RAW_HR_EMPLOYEES` |
| **Importe** | `hr_pipeline.config.Settings` |
| **Importe** | `hr_pipeline.ingestion.*` — les 5 symboles publics |
| **Importe** | `hr_pipeline.warehouse.get_warehouse` |
| **Produit l'Asset** | `RAW_HR_EMPLOYEES` → déclenche `transform_hr_dbt` |
| **Testé par** | `tests/test_dag_integrity.py` (import, nb de tâches, tags) |

---

## 5. Pièges & gotchas

**Piège 1 — `Settings.from_env()` au niveau module**

Voir §3.3. C'est le piège numéro 1 dans l'écriture de DAGs Airflow.
Symptôme dans les logs scheduler : `"Import error"` sur le DAG, avec une
trace pointant vers `Settings.from_env()` ou tout appel qui lit
l'environnement/le filesystem au parse time.

**Piège 2 — Retourner un dataclass depuis un `@task`**

Airflow 3 utilise JSON pour sérialiser les XComs par défaut. Tout objet
non-JSON-sérialisable (dataclass, `Path`, `datetime` sans isoformat) lèvera
une `TypeError` à la fin de la tâche. Toujours retourner des dicts / scalars /
listes de dicts depuis les tâches TaskFlow.

**Piège 3 — Modifier `outlets` sans comprendre l'impact en aval**

Retirer `outlets=[RAW_HR_EMPLOYEES]` de `load_raw` casse silencieusement le
déclenchement de `transform_hr_dbt`. Il n'y a pas d'erreur Airflow — le DAG
aval ne se déclenche simplement plus. Ce type de régression est difficile à
détecter sans monitoring des assets dans l'UI Airflow.

**Piège 4 — `max_active_runs=1` ne protège pas contre les runs manuels concurrents**

`max_active_runs=1` empêche deux runs schedulés de se chevaucher. Mais si un
opérateur déclenche manuellement un run depuis l'UI alors qu'un run automatique
tourne déjà, Airflow mettra le run manuel en queue (il attendra). Ce n'est pas
un bug — c'est le comportement attendu — mais il peut surprendre.

**Piège 5 — L'ordre des instances mappées n'est pas garanti**

`extract_region[0]` correspond au premier fichier retourné par
`discover_sources()`, qui est le premier élément de `sorted(glob(...))`. Cet
ordre est déterministe sur un même OS. Mais ne pas dépendre de cet ordre dans
`load_raw` est une bonne pratique — la consolidation doit fonctionner quelle
que soit l'ordre des extracts dans la liste.

**Piège 6 — `common.py` doit être dans `.airflowignore` (ou géré)**

`common.py` n'est pas un DAG (pas de `@dag`). Sans configuration, le DAG
file processor peut logger des warnings sur ce fichier ("no DAG found"). Le
fichier `.airflowignore` à la racine de `dags/` ou la configuration
`DAG_DISCOVERY_SAFE_MODE=False` gère ce cas. Vérifier que les logs du scheduler
ne contiennent pas de warnings répétés sur `common.py`.
