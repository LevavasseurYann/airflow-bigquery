# DAG data_quality_hr

> **Fichier source** : `dags/data_quality_hr.py`

---

## 1. Pourquoi ce fichier existe

### Décision architecturale

dbt teste déjà les marts pendant le build — `not_null`, `unique`, `relationships` s'exécutent dans `transform_hr_dbt`. Alors pourquoi un DAG de qualité séparé ?

**Trois raisons distinctes :**

**1. Couche de contrôle indépendante.** Dans une architecture de data platform, les tests dbt appartiennent à l'équipe transformation. Ce DAG appartient à la plateforme — il s'exécute _après_ publication, indépendamment du build. C'est le modèle "trust but verify" : dbt certifie que le build est propre, ce DAG certifie que ce qui est publié est conforme.

**2. Audit trail.** dbt ne laisse pas d'historique de test structuré dans votre warehouse. Chaque exécution de `data_quality_hr` écrit une ligne dans `quality.check_runs` — une table requêtable pour répondre à "combien de fois la freshness a échoué ce mois ?" ou "quel jour le check d'unicité a commencé à échouer ?".

**3. Granularité et observabilité Airflow.** Un test dbt qui échoue pendant le build arrête tout le build. Ici, chaque check est une tâche Airflow indépendante : les 10 checks s'exécutent en parallèle, les résultats sont visibles individuellement dans l'UI, on peut re-run un seul check échoué sans relancer le build dbt entier.

!!! note "Complémentarité dbt / DAG qualité"
    Les tests dbt bloquent la promotion d'un modèle défaillant pendant la transformation.
    Ce DAG enregistre l'état des marts _publiés_ et produit un SLA de qualité mesurable dans le temps.
    Les deux sont nécessaires ; ils ne se remplacent pas.

---

## 2. Vue d'ensemble

### Graphe du DAG

```
HR_MARTS asset (produit par transform_hr_dbt)
    │
    ▼  [schedule trigger]
data_quality_hr DAG run
    │
    ├── check__fct_active__employee_id_not_null     ─┐
    ├── check__fct_active__employee_id_unique         │
    ├── check__fct_active__salary_non_negative        │  Tâches parallèles
    ├── check__fct_active__no_future_hire_dates       │  (DataQualityCheckOperator)
    ├── check__fct_active__years_of_service_non_negative │
    ├── check__fct_active__department_referential_integrity │
    ├── check__dim_departments__department_unique     │
    ├── check__headcount__non_negative                │
    ├── check__fct_active__not_empty                  │
    └── check__fct_active__freshness               ──┘
                │ (tous → trigger_rule="all_done")
                ▼
        publish_quality_report
        (agrège XCom, écrit quality.check_runs)
```

**Toutes les tâches `check__*` s'exécutent en parallèle** — aucune dépendance entre elles. `publish_quality_report` attend que toutes soient terminées (succès ou échec) avant de s'exécuter.

### Position dans la chaîne des assets

```
ingest_hr_sources  →  RAW_HR_EMPLOYEES
                              │
                              ▼
transform_hr_dbt   →  HR_MARTS
                              │
                              ▼
data_quality_hr    (triggered by HR_MARTS)
```

---

## 3. Walkthrough complet

### Les imports

```python
import logging
import os

from airflow.sdk import dag, get_current_context, task

from common import DEFAULT_ARGS, DEFAULT_START_DATE
from hr_pipeline.assets import HR_MARTS
from hr_pipeline.config import Settings
from hr_pipeline.operators import DataQualityCheckOperator
from hr_pipeline.quality import build_marts_quality_suite
from hr_pipeline.quality.checks import CheckResult, QualityReport, Severity
from hr_pipeline.warehouse import get_warehouse
```

**`from airflow.sdk import dag, get_current_context, task`** — Airflow 3 a réorganisé son API publique dans `airflow.sdk`. Le decorator `@dag` remplace le pattern `with DAG(...) as dag:` des versions précédentes. `get_current_context()` remplace l'injection de `**context` dans les callables des PythonOperator.

**`from common import DEFAULT_ARGS, DEFAULT_START_DATE`** — tous les DAGs importent leurs valeurs partagées depuis `common.py`. Ne jamais redéfinir `DEFAULT_ARGS` dans chaque DAG — une modification de la politique de retry (`retries: 2 → 3`) doit se faire en un seul endroit.

**`from hr_pipeline.assets import HR_MARTS`** — l'asset est importé depuis `hr_pipeline/assets.py` qui est le module canonique. Si `data_quality_hr.py` définissait son propre `Asset(name="hr_marts", ...)`, ce serait un objet _différent_ aux yeux d'Airflow même avec le même nom — le scheduling ne fonctionnerait pas. Un seul module, une seule définition.

**`import os` en haut, `import pandas as pd` dans la fonction** — asymétrie intentionnelle. `os` est un module stdlib ultra-léger, son import n'a pas d'impact mesurable. pandas est lourd (~100ms). L'import de `os` en tête de fichier est idiomatique Python. L'import de pandas est différé pour protéger le parse time.

---

### Construction de la suite au parse time

```python
_QUALITY_SUITE = build_marts_quality_suite(
    marts_schema=os.getenv("BQ_DATASET_MARTS", "marts"),
)
```

**Pourquoi construire la suite ici, au niveau module ?**

Le DAG génère des tâches à partir de la suite dans la boucle `for check in _QUALITY_SUITE`. Airflow a besoin de connaître le graphe complet des tâches **au parse time** — c'est ainsi qu'il construit la vue du DAG dans l'UI, génère les slots de task instance, etc. Si `_QUALITY_SUITE` était construit à l'intérieur d'une fonction de task (donc au runtime), Airflow ne verrait jamais les tâches individuelles.

**`os.getenv("BQ_DATASET_MARTS", "marts")` et non `Settings.from_env()`**

`Settings.from_env()` peut lever des exceptions si des variables obligatoires manquent (ex : `GCP_PROJECT` en mode production). Au parse time, les variables d'environnement production peuvent ne pas être définies (le scheduler tourne dans un container différent du worker). `os.getenv()` ne raise jamais — si la variable n'existe pas, il retourne le défaut `"marts"`.

!!! danger "Settings.from_env() au parse time"
    Appeler `Settings.from_env()` au niveau module (hors d'une fonction de task) peut casser le parsing de **tous** vos DAGs si une variable d'environnement est manquante. Le scheduler log une erreur de parse et le DAG disparaît de l'UI silencieusement.
    Règle : au parse time, uniquement `os.getenv()`. Dans les tâches, `Settings.from_env()`.

**La suite est construite une seule fois** (module singleton). Si le module est mis en cache par le DAG processor, `_QUALITY_SUITE` n'est pas recalculé. C'est safe parce que `build_marts_quality_suite()` est pure et déterministe pour des paramètres donnés.

---

### `_result_from_xcom()` — reconstruction depuis XCom

```python
def _result_from_xcom(
    check_name: str,
    payload: dict | None,
    suite_lookup: dict,
) -> CheckResult:
    if payload is None:
        check = suite_lookup[check_name]
        return CheckResult(
            name=check.name,
            description=check.description,
            severity=check.severity,
            failing_rows=-1,
            passed=False,
        )
    return CheckResult(
        name=str(payload["name"]),
        description=str(payload["description"]),
        severity=Severity(payload["severity"]),
        failing_rows=int(payload["failing_rows"]),
        passed=bool(payload["passed"]),
    )
```

**Pourquoi cette fonction existe**

`xcom_pull()` retourne `None` dans deux cas :
1. Le check task n'a pas poussé de XCom (erreur d'infra avant le push, ex : OOM, timeout réseau)
2. Le check task a été skippé (n'arrive pas avec `trigger_rule="all_done"`, mais défensif)

Dans le cas `None`, on synthétise un `CheckResult` avec `failing_rows=-1` et `passed=False`. La valeur `-1` est un **sentinel** — elle ne peut pas être produite par `run_check()` (qui retourne `int(raw or 0)`, toujours ≥ 0). Elle signifie "le check n'a pas pu s'exécuter", distinguable de "le check a trouvé 0 lignes failing".

**`suite_lookup`** est un dictionnaire `{check.name: check}` construit juste avant l'appel, permettant de retrouver le `QualityCheck` original par nom sans boucle linéaire.

**Pourquoi typer explicitement `str(...)`, `int(...)`, `bool(...)`** dans la branche de désérialisation ? XCom JSON peut introduire des surprises de type Python : un `int` stocké comme entier JSON peut être lu comme `int` ou `float` selon la version du sérialiseur, `"true"` (string) au lieu de `True` (bool), etc. Les casts explicites sont des garde-fous.

---

### Le decorator `@dag` et ses paramètres

```python
@dag(
    dag_id="data_quality_hr",
    description="Independent post-build data quality gate on the HR marts.",
    schedule=[HR_MARTS],
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=_DOC,
    tags=["hr", "data-quality", "layer:marts"],
)
def data_quality_hr() -> None:
```

**`schedule=[HR_MARTS]`** — asset-driven scheduling Airflow 3. La liste contient l'asset `HR_MARTS` (objet Python importé depuis `hr_pipeline.assets`). Le DAG ne se déclenche pas sur un cron — il se déclenche dès que `transform_hr_dbt` marque l'asset comme produit. Couplage par données, pas par temps.

!!! tip "Asset-driven vs cron"
    Avec un cron (`schedule="0 6 * * *"`), le DAG de qualité tourne à 6h00 même si le dbt build a pris du retard et n'a fini qu'à 6h30. Résultat : le check de freshness échoue systématiquement, créant du bruit.
    Avec `schedule=[HR_MARTS]`, le DAG de qualité démarre exactement quand les marts sont prêts. Pas avant, jamais trop tôt.

**`catchup=False`** — si le scheduler redémarre après un arrêt de 3 jours, il ne crée pas 3 DAG runs de qualité en retard. La qualité des données est une vérification du présent, pas un backfill historique.

**`max_active_runs=1`** — une seule exécution concurrente du DAG. Les checks de qualité lisent les mêmes tables — deux runs parallèles ne causeraient pas de corruption (lecture seule), mais ils produiraient deux lignes quasi-identiques dans `quality.check_runs` et doubleraient la charge sur le warehouse. Limite à 1 pour la cohérence.

---

### `publish_quality_report` — le reporter

```python
@task(trigger_rule="all_done")
def publish_quality_report() -> dict[str, object]:
    import pandas as pd  # deferred — DAG files are parsed frequently

    context = get_current_context()
    task_instance = context["ti"]
    suite_lookup = {check.name: check for check in _QUALITY_SUITE}

    results = [
        _result_from_xcom(
            check.name,
            task_instance.xcom_pull(task_ids=f"check__{check.name}", key="check_result"),
            suite_lookup,
        )
        for check in _QUALITY_SUITE
    ]
    report = QualityReport(results=results)
    logger.info("\n%s", report.summary())

    settings = Settings.from_env()
    run_row = pd.DataFrame([{
        "run_id": str(context["run_id"]),
        "generated_at": pd.Timestamp.now(tz="UTC"),
        "total_checks": len(report.results),
        "passed": sum(1 for r in report.results if r.passed),
        "blocking_failures": len(report.blocking_failures),
        "warnings": len(report.warnings),
    }])
    with get_warehouse(settings) as warehouse:
        warehouse.load_dataframe("quality", "check_runs", run_row, mode="append")

    if report.has_blocking_failures():
        logger.error(
            "%s blocking data quality failure(s) — see the failed check tasks.",
            len(report.blocking_failures),
        )
    return report.as_dict()
```

#### `trigger_rule="all_done"` — le détail qui change tout

Par défaut, toutes les tâches Airflow utilisent `trigger_rule="all_success"`. Une tâche downstream avec ce trigger rule est **skippée** si une de ses upstream est en état FAILED ou UPSTREAM_FAILED.

Avec `trigger_rule="all_done"`, la tâche s'exécute dès que **toutes** ses upstream sont terminées, quel que soit leur état (SUCCESS, FAILED, SKIPPED). C'est exactement le comportement voulu ici : le reporter doit toujours s'exécuter, surtout quand des checks échouent.

| trigger_rule | Comportement quand un check échoue |
|---|---|
| `all_success` (défaut) | `publish_quality_report` est SKIPPED — pas d'audit trail |
| `all_done` | `publish_quality_report` s'exécute toujours — audit trail garanti |

!!! warning "Sans all_done, les échecs disparaissent"
    Si `publish_quality_report` était skippé à chaque échec de check, la table `quality.check_runs` n'aurait de lignes que pour les runs parfaits. Exactement les runs qui ont le moins besoin d'audit. Avec `all_done`, les pires runs sont les mieux documentés.

#### `import pandas as pd` — import différé

Le processeur de DAGs Airflow parse les fichiers DAG toutes les `dag_dir_list_interval` secondes (défaut : 30s). À chaque parse, le module est réimporté. Un import pandas au niveau module coûte ~100ms à chaque cycle. Sur un scheduler avec 50 DAGs, c'est 5 secondes de CPU par cycle consacrées aux imports.

L'import à l'intérieur de la fonction `publish_quality_report` ne s'exécute que quand la tâche tourne sur un worker — une à deux fois par jour.

#### Reconstruction des résultats depuis XCom

```python
results = [
    _result_from_xcom(
        check.name,
        task_instance.xcom_pull(task_ids=f"check__{check.name}", key="check_result"),
        suite_lookup,
    )
    for check in _QUALITY_SUITE
]
```

**`task_ids=f"check__{check.name}"`** : le task_id de chaque opérateur de check est préfixé `check__`. Le double underscore est une convention de nommage qui sépare le préfixe fonctionnel du nom du check (qui peut lui-même contenir des underscores : `fct_active__employee_id_not_null`).

**`key="check_result"`** : clé XCom explicite. L'opérateur push avec `xcom_push(key="check_result", value=result_dict)`. Sans clé explicite, Airflow utilise `"return_value"` pour les retours de `execute()`. L'utilisation d'une clé nommée est plus intentionnelle et résistante aux changements de la convention default.

**L'ordre de la liste `results` correspond à l'ordre de `_QUALITY_SUITE`**. C'est intentionnel : `report.summary()` affiche les checks dans cet ordre, et `QualityReport.as_dict()` les sérialise dans cet ordre. L'ordre du rapport est déterministe.

#### Écriture de l'audit trail

```python
run_row = pd.DataFrame([{
    "run_id": str(context["run_id"]),
    "generated_at": pd.Timestamp.now(tz="UTC"),
    "total_checks": len(report.results),
    "passed": sum(1 for r in report.results if r.passed),
    "blocking_failures": len(report.blocking_failures),
    "warnings": len(report.warnings),
}])
with get_warehouse(settings) as warehouse:
    warehouse.load_dataframe("quality", "check_runs", run_row, mode="append")
```

**`pd.Timestamp.now(tz="UTC")`** — timezone UTC explicite. Sans timezone, le timestamp dépend du timezone du worker (qui peut être configuré différemment en local vs en prod, ou varier selon le DST). UTC est le seul choix défensif pour les données temporelles dans un pipeline.

**`mode="append"`** — contrairement aux marts qui sont reconstruits entièrement (`mode="replace"`), la table d'audit est **additive**. On ne veut jamais perdre l'historique des runs qualité.

**`str(context["run_id"])`** — le `run_id` Airflow est une chaîne du type `scheduled__2024-01-15T06:00:00+00:00`. Le `str()` est défensif — dans Airflow 3, le type peut varier selon le contexte d'exécution (scheduled, manual, backfill).

#### Logging des blocking failures

```python
if report.has_blocking_failures():
    logger.error(
        "%s blocking data quality failure(s) — see the failed check tasks.",
        len(report.blocking_failures),
    )
```

Ce `logger.error` n'est pas le seul mécanisme — les tâches `check__*` individuelles ont déjà été FAILED dans Airflow, et l'alerte `on_failure_callback` de `DEFAULT_ARGS` a été déclenchée sur chacune d'elles. Ce log est une information consolidée dans le log de la tâche reporter — utile pour les opérateurs qui regardent les logs du reporter plutôt que des tasks individuelles.

---

### Le générateur de tâches

```python
report = publish_quality_report()

for check in _QUALITY_SUITE:
    check_task = DataQualityCheckOperator(
        task_id=f"check__{check.name}",
        quality_check=check,
    )
    check_task >> report
```

**Cette boucle s'exécute au parse time**, pas au runtime. C'est la mécanique du DAG factory pattern en Airflow : quand le processeur de DAG importe ce module, la boucle s'exécute et crée 10 objets `DataQualityCheckOperator`, chacun connecté à `report` via l'opérateur `>>` (bit shift surchargé pour exprimer la dépendance).

**`check_task >> report`** est équivalent à `report.set_upstream(check_task)`. Le `>>` est purement du sucre syntaxique défini dans `BaseOperator.__rshift__`.

**Pourquoi ne pas utiliser `TaskGroup` ?** Un `TaskGroup` regrouperait les checks dans un sous-groupe visuel dans l'UI Airflow. C'est une décision de présentation valide pour 10+ checks. Avec 10 checks, les checks individuels visibles directement dans le graphe sont plus lisibles — on voit immédiatement quel check a échoué sans cliquer sur un groupe.

!!! note "Ajout d'un check = ajout automatique de tâche"
    Ajouter un `QualityCheck` dans `build_marts_quality_suite()` créera automatiquement une nouvelle tâche `check__<name>` dans le DAG au prochain cycle de parsing. Aucune modification du DAG n'est nécessaire.

---

### Appel du factory

```python
data_quality_hr()
```

En Airflow 3, une fonction décorée `@dag` retourne un objet DAG quand elle est appelée. Cet appel à la fin du module est obligatoire — il déclenche la construction du DAG et enregistre l'objet dans le namespace global du module, où le DAG processor peut le trouver. Sans cet appel, le DAG ne serait jamais créé.

---

## 4. Connexions

**En amont :**
- `transform_hr_dbt` publie l'asset `HR_MARTS` qui déclenche ce DAG
- `hr_pipeline/assets.py` définit `HR_MARTS` (importé ici)
- `hr_pipeline/quality/suite.py` fournit `_QUALITY_SUITE` via `build_marts_quality_suite()`

**En aval :**
- `quality.check_runs` dans le warehouse — une ligne par run, requêtable pour l'observabilité
- Les tâches `check__*` individuelles alimentent `publish_quality_report` via XCom

**Transversal :**
- `dags/common.py` — `DEFAULT_ARGS`, `DEFAULT_START_DATE`
- `hr_pipeline/operators/data_quality.py` — `DataQualityCheckOperator`
- `hr_pipeline/config.py` — `Settings.from_env()` (dans les tâches uniquement)
- `hr_pipeline/warehouse.py` — `get_warehouse()` pour l'écriture de l'audit

---

## 5. Pièges & gotchas

!!! danger "trigger_rule manquant sur publish_quality_report"
    Sans `trigger_rule="all_done"`, si un seul check échoue, `publish_quality_report` est **skippé**. L'audit trail n'est pas écrit. Les checks suivants ne sont pas résumés. Le DAG run finit en état FAILED sans rapport consolidé. C'est le piège le plus courant sur ce type de pattern.

!!! warning "XCom et taille des payloads"
    Chaque check push un dict `as_dict()` dans XCom. Avec 10 checks, c'est 10 XCom entries de quelques centaines d'octets chacune — très loin des limites. Mais si vous ajoutez des checks avec des descriptions très longues ou des métadonnées volumineuses, surveillez la taille des XCom (limite Airflow par défaut : 48KB par valeur).

!!! warning "parse time vs runtime — frontière critique"
    Tout ce qui est en dehors des fonctions `@task` et `execute()` s'exécute au parse time :
    - `_QUALITY_SUITE = build_marts_quality_suite(...)` → parse time ✓
    - `os.getenv(...)` → parse time ✓ (ne raise jamais)
    - `Settings.from_env()` au niveau module → parse time ✗ (peut raise)
    - `get_warehouse(settings)` au niveau module → parse time ✗ (connexion réseau)
    La règle : aucune I/O, aucune connexion, aucun appel qui peut échouer en dehors d'une fonction de tâche.

!!! tip "Déboguer un check qui échoue"
    1. Trouver la tâche `check__<name>` dans l'UI Airflow
    2. Lire ses logs — le message d'erreur inclut le nombre de `failing_rows`
    3. Récupérer le SQL depuis `_QUALITY_SUITE` (ou `suite.py`) et l'exécuter manuellement sur le warehouse
    4. Pour DuckDB local : `docker compose exec airflow-worker duckdb /opt/airflow/include/data/warehouse.duckdb`

!!! note "Re-run d'un check individuel"
    Grâce à l'architecture avec tâches individuelles, on peut re-run un seul check depuis l'UI (clic droit sur la tâche → Clear) sans relancer le build dbt complet. `publish_quality_report` se recalcule avec le nouveau résultat.
