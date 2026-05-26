# DataQualityCheckOperator

> **Fichier source** : `src/hr_pipeline/operators/data_quality.py`

---

## 1. Pourquoi ce fichier existe

### Décision architecturale

Airflow offre deux façons principales d'encapsuler de la logique dans un DAG : le decorator `@task` (Python TaskFlow) et un opérateur custom héritant de `BaseOperator`. Ce n'est pas une question de style — ce sont deux outils avec des cas d'usage différents.

`DataQualityCheckOperator` est un opérateur custom parce que le cas d'usage l'exige sur trois points :

**1. Réutilisation paramétrée.** Le DAG instancie cet opérateur en boucle, une fois par check. Chaque instance reçoit un `QualityCheck` différent. Avec `@task`, on créerait soit une fonction générique appelée 10 fois (mais `@task` ne génère pas plusieurs tâches dynamiquement de la même façon), soit 10 fonctions quasi-identiques. L'opérateur custom est le pattern idiomatique pour "même logique, paramètres différents, N instances".

**2. Identité visuelle dans l'UI.** `ui_color = "#4a90d9"` donne une couleur distincte aux tâches de check dans le graphe Airflow. En un coup d'oeil, l'opérateur distingue les checks (bleu) des autres tâches (vert/gris). Impossible avec `@task`.

**3. Auto-documentation.** La docstring de classe apparaît dans l'UI Airflow (onglet "Details" d'une tâche). Un opérateur custom est un contrat documenté : son comportement sur ERROR vs WARN est décrit dans le code et visible sans ouvrir le source.

!!! tip "Règle de décision @task vs BaseOperator"
    | Critère | `@task` | `BaseOperator` |
    |---|---|---|
    | Logique one-shot, non réutilisée | ✓ | |
    | Paramétrée, instanciée en boucle | | ✓ |
    | Couleur/icône custom dans l'UI | | ✓ |
    | Docstring visible dans l'UI | | ✓ |
    | Tests unitaires simples | ✓ | (plus verbeux) |
    | Partage entre DAGs/projets | | ✓ |
    `DataQualityCheckOperator` remplit les 4 critères de la colonne droite.

---

## 2. Vue d'ensemble

```
DataQualityCheckOperator
│
├── __init__(quality_check: QualityCheck)
│   └── Instancié au parse time par la boucle du DAG
│       Paramètre stocké dans self.quality_check
│
└── execute(context)
    ├── [parse time safe: aucun import lourd]
    ├── Imports différés (Settings, run_check, get_warehouse)
    ├── get_warehouse(settings, read_only=True)
    │   └── Connexion DuckDB ou BigQuery selon HR_ENV
    ├── run_check(warehouse, self.quality_check)
    │   └── fetch_scalar(sql) → int (failing rows)
    ├── xcom_push("check_result", result.as_dict())  ← AVANT tout raise
    ├── Si passed → return result_dict
    ├── Si Severity.ERROR → raise AirflowException
    └── Si Severity.WARN → log warning, return result_dict
```

---

## 3. Walkthrough complet

### Imports au niveau module

```python
from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from hr_pipeline.quality.checks import QualityCheck, Severity
```

**`from __future__ import annotations`** : active le mode "postponed evaluation of annotations" (PEP 563). Les annotations de type sont évaluées en chaîne de caractères plutôt qu'immédiatement. Avantage : permet d'écrire des annotations faisant référence à des types définis plus loin dans le fichier, et réduit légèrement le coût d'import (les annotations ne sont pas évaluées à l'import du module, seulement si `typing.get_type_hints()` est appelé).

**`from airflow.exceptions import AirflowException`** : l'exception que l'on raise pour signaler un échec ERROR. C'est l'exception standard Airflow pour marquer une tâche comme FAILED — le scheduler la capture et transite la tâche dans l'état FAILED (puis déclenche les retries selon `default_args`). Une exception Python générique (`RuntimeError`, `ValueError`) fonctionnerait aussi pour le statut, mais `AirflowException` est sémantiquement plus précis et peut être filtré spécifiquement dans les tests.

**`from airflow.sdk import BaseOperator`** : en Airflow 3, `BaseOperator` a été déplacé dans `airflow.sdk`. Dans Airflow 2.x, c'était `from airflow.models import BaseOperator`. Vérifier la version si vous portez cet opérateur.

**`from hr_pipeline.quality.checks import QualityCheck, Severity`** : seules ces deux classes sont importées au niveau module. Pourquoi pas `run_check`, `Settings`, `get_warehouse` ? Parce qu'ils sont utilisés dans `execute()`, pas dans `__init__`. Importer `Settings` au niveau module déclencherait son import (et potentiellement celui de `google.cloud.bigquery` etc.) à chaque parse du DAG. Les imports lourds restent dans `execute()`.

---

### Attributs de classe

```python
class DataQualityCheckOperator(BaseOperator):
    ui_color = "#4a90d9"
    ui_fgcolor = "#ffffff"
```

**`ui_color`** et **`ui_fgcolor`** sont des attributs de classe reconnus par l'UI Airflow. `ui_color` est la couleur de fond de la tâche dans le graphe. `ui_fgcolor` est la couleur du texte. Ces deux valeurs sont lues par `airflow.www.utils.get_params_with_defaults()` qui génère le rendu HTML du graphe.

`"#4a90d9"` est un bleu standard — distinctif sans être agressif. En pratique, dans un DAG qui mélange des `PythonOperator` (gris), des opérateurs Cosmos/dbt (orange) et ces checks (bleu), la couleur permet une lecture rapide du graphe : "tous les bleus sont passés, le rouge est dans la tâche de transformation".

!!! note "Hex colors dans l'UI Airflow"
    L'UI Airflow accepte n'importe quelle couleur CSS valide : `#4a90d9`, `blue`, `rgb(74, 144, 217)`. La convention dans ce projet est le hex 6 chiffres — consistant, précis, diffable en git.

---

### `__init__`

```python
def __init__(self, *, quality_check: QualityCheck, **kwargs: Any) -> None:
    super().__init__(**kwargs)
    self.quality_check = quality_check
```

**`*` comme premier paramètre** : force tous les arguments après `self` à être passés par nom. Appeler `DataQualityCheckOperator(some_check)` est une erreur de syntaxe. L'appelant doit écrire `DataQualityCheckOperator(quality_check=some_check, task_id="check__foo")`. Pour un opérateur appelé dans une boucle, ça évite les erreurs d'ordre d'arguments.

**`super().__init__(**kwargs)`** : `BaseOperator.__init__` accepte des dizaines de paramètres : `task_id`, `retries`, `retry_delay`, `pool`, `queue`, `priority_weight`, `on_failure_callback`, `sla`… La convention `**kwargs` délègue tous ces paramètres au parent sans les re-déclarer. Si Airflow ajoute un nouveau paramètre à `BaseOperator`, notre opérateur le supporte automatiquement sans modification.

!!! warning "task_id est dans **kwargs"
    `task_id` est le paramètre le plus important — il identifie la tâche dans le graphe et dans XCom. Il est passé via `**kwargs` au `super().__init__()`. Ne pas tenter de le gérer dans `DataQualityCheckOperator.__init__` directement.

**`self.quality_check = quality_check`** : le `QualityCheck` est stocké comme attribut d'instance. Airflow sérialise les attributs d'instance dans sa base de données (via `pickle` ou `json` selon la configuration). Pour que la sérialisation fonctionne, `QualityCheck` doit être sérialisable — ce qu'il est, étant une dataclass `frozen=True` avec des champs de types simples.

---

### `execute()` — la mécanique complète

```python
def execute(self, context: Any) -> dict[str, Any]:
    from hr_pipeline.config import Settings
    from hr_pipeline.quality.checks import run_check
    from hr_pipeline.warehouse import get_warehouse

    settings = Settings.from_env()
    self.log.info("Running data quality check '%s'", self.quality_check.name)

    with get_warehouse(settings, read_only=True) as warehouse:
        result = run_check(warehouse, self.quality_check)

    result_dict = result.as_dict()
    context["ti"].xcom_push(key="check_result", value=result_dict)

    if result.passed:
        self.log.info("PASS — %s", self.quality_check.description)
        return result_dict

    message = (
        f"Data quality check '{result.name}' failed: "
        f"{result.failing_rows} failing row(s). {result.description}"
    )
    if self.quality_check.severity is Severity.ERROR:
        raise AirflowException(message)

    self.log.warning("%s — severity=WARN, not blocking the DAG.", message)
    return result_dict
```

#### Imports différés

```python
from hr_pipeline.config import Settings
from hr_pipeline.quality.checks import run_check
from hr_pipeline.warehouse import get_warehouse
```

Ces trois imports sont à l'intérieur de `execute()`, pas au niveau module. `execute()` est appelé par le worker Airflow quand la tâche s'exécute. `__init__()` est appelé par le scheduler quand il parse le DAG.

Conséquence pratique : au parse time, le module de l'opérateur importe uniquement `BaseOperator`, `AirflowException`, `QualityCheck`, `Severity` — tous légers. `Settings` peut importer `pydantic` et des dépendances GCP. `get_warehouse` peut importer `duckdb` et `google.cloud.bigquery`. Ces imports n'arrivent que sur le worker, au moment de l'exécution.

Sur un scheduler qui parse 50 DAGs toutes les 30 secondes, la différence entre "imports lourds au niveau module" et "imports différés dans execute" peut représenter plusieurs secondes de lag du scheduler.

#### `self.log` vs `logging.getLogger(__name__)`

```python
self.log.info("Running data quality check '%s'", self.quality_check.name)
```

Dans un opérateur custom, `self.log` est le logger de l'instance — il est configuré par Airflow pour écrire dans le log de la task instance, accessible dans l'UI. C'est différent de `logger = logging.getLogger(__name__)` qui écrit dans le logger du module. Dans `execute()`, toujours utiliser `self.log` pour que les messages apparaissent dans les logs de la tâche Airflow.

#### `read_only=True` — DuckDB et la contention d'écriture

```python
with get_warehouse(settings, read_only=True) as warehouse:
    result = run_check(warehouse, self.quality_check)
```

DuckDB utilise un verrou de fichier exclusif pour les connexions en écriture. Si deux connexions tentent d'ouvrir le même fichier `.duckdb` en mode lecture-écriture simultanément, la deuxième obtient une `duckdb.IOException: database is locked`.

Les 10 tâches `check__*` s'exécutent **en parallèle** dans Airflow. Sans `read_only=True`, les 9 premières connections bloquent la 10ème. Avec `read_only=True`, DuckDB autorise plusieurs connexions simultanées — le verrou exclusif n'est pas requis pour la lecture seule.

Sur BigQuery, ce paramètre n'a pas d'effet (BigQuery est un service distribué sans contention de ce type), mais il documente l'intention : ces tâches ne modifient jamais les données.

!!! danger "Omettre read_only=True avec DuckDB"
    Sans `read_only=True`, les checks peuvent échouer de façon intermittente avec `database is locked` selon l'ordre de démarrage des workers. L'erreur est non-déterministe et difficile à diagnostiquer car elle dépend du timing du scheduler.

#### XCom push avant raise — l'ordre critique

```python
# Push the result BEFORE any raise, so the downstream report task can
# account for a failed check too.
result_dict = result.as_dict()
context["ti"].xcom_push(key="check_result", value=result_dict)

if result.passed:
    self.log.info("PASS — %s", self.quality_check.description)
    return result_dict

# ...
if self.quality_check.severity is Severity.ERROR:
    raise AirflowException(message)
```

C'est la décision de design la plus importante de cet opérateur. L'ordre est : **push XCom, puis décider de raise ou non**.

Si on échangeait les deux (`raise` puis `xcom_push`), l'exception interromprait l'exécution avant le push. La tâche serait FAILED, et `publish_quality_report` lirait `None` depuis XCom pour ce check. `_result_from_xcom()` synthétiserait un résultat avec `failing_rows=-1` — informatif, mais on perdrait le vrai nombre de lignes failing.

Avec l'ordre actuel :
1. Le résultat est toujours dans XCom, même pour les checks FAILED
2. Le reporter voit `failing_rows=3` (vrai nombre) et non `failing_rows=-1` (sentinel)
3. Le DAG run final contient le compte exact des violations dans `quality.check_runs`

!!! warning "Idempotence du XCom push"
    Si la tâche est re-run (Clear depuis l'UI), `execute()` est rappelé. Le `xcom_push()` écrase la valeur précédente (même run_id, même task_id, même key). C'est le comportement voulu.

#### Logique de décision finale

```python
if result.passed:
    self.log.info("PASS — %s", self.quality_check.description)
    return result_dict                        # ← succès, XCom via return

message = (
    f"Data quality check '{result.name}' failed: "
    f"{result.failing_rows} failing row(s). {result.description}"
)
if self.quality_check.severity is Severity.ERROR:
    raise AirflowException(message)          # ← tâche FAILED

self.log.warning("%s — severity=WARN, not blocking the DAG.", message)
return result_dict                           # ← succès malgré l'échec du check
```

Trois cas :

| État | `passed` | `severity` | Action | Statut tâche |
|---|---|---|---|---|
| Check passé | `True` | any | `return result_dict` | SUCCESS |
| Check échoué, bloquant | `False` | `ERROR` | `raise AirflowException` | FAILED |
| Check échoué, non-bloquant | `False` | `WARN` | `log.warning` + `return` | SUCCESS |

**`return result_dict`** dans les cas SUCCESS déclenche le push XCom automatique d'Airflow (via `"return_value"`). Mais on a déjà poussé manuellement avec `key="check_result"` — les deux XCom coexistent. Le reporter utilise `key="check_result"` explicitement, pas le return value. Le double push est redondant mais inoffensif.

**`is Severity.ERROR`** plutôt que `== Severity.ERROR`** : avec `StrEnum`, les deux sont équivalents. L'opérateur `is` teste l'identité de l'objet (même référence en mémoire). Avec les enums, tous les membres d'une enum sont des singletons — `Severity.ERROR is Severity.ERROR` est toujours `True`. `is` est légèrement plus explicite sémantiquement pour les comparaisons d'enum.

---

## 4. Connexions

**Instancié par :**
```python
# dags/data_quality_hr.py
for check in _QUALITY_SUITE:
    check_task = DataQualityCheckOperator(
        task_id=f"check__{check.name}",
        quality_check=check,
    )
    check_task >> report
```

**Appelle :**
- `hr_pipeline.config.Settings.from_env()` — récupère la configuration (backend, chemins)
- `hr_pipeline.warehouse.get_warehouse(settings, read_only=True)` — connexion au warehouse
- `hr_pipeline.quality.checks.run_check(warehouse, check)` — exécute le SQL, retourne un `CheckResult`

**Produit :**
- XCom `key="check_result"` — dict JSON contenant `{name, description, severity, failing_rows, passed}`
- Logs Airflow via `self.log` — visibles dans l'UI tâche par tâche

**Consommé par :**
- `publish_quality_report` (dans `data_quality_hr.py`) via `xcom_pull(task_ids="check__...", key="check_result")`

**Exporté via :**
```python
# src/hr_pipeline/operators/__init__.py
from hr_pipeline.operators.data_quality import DataQualityCheckOperator
__all__ = ["DataQualityCheckOperator"]
```

Le DAG importe `from hr_pipeline.operators import DataQualityCheckOperator` — il n'a pas besoin de connaître le sous-module exact.

---

## 5. Pièges & gotchas

!!! danger "Ne jamais raise avant xcom_push"
    Si vous modifiez `execute()` pour ajouter une validation et levez une exception avant `xcom_push()`, le reporter recevra `None` depuis XCom pour ce check. `_result_from_xcom` retournera `failing_rows=-1`. L'audit trail sera inexact.
    Règle : `xcom_push` toujours en premier. Toute exception vient après.

!!! warning "Sérialisation de QualityCheck par Airflow"
    Airflow sérialise les attributs d'instance des opérateurs dans sa métadatabase (pour la reprise sur panne, les re-runs, etc.). `self.quality_check` est un `QualityCheck` (dataclass). Airflow utilise son propre sérialiseur — en cas de problème, ajouter un `template_fields = ()` explicite et vérifier que `QualityCheck` est correctement sérialisable.

!!! warning "read_only=True obligatoire en parallèle DuckDB"
    Tout opérateur qui ouvre une connexion DuckDB dans un contexte parallèle doit utiliser `read_only=True` s'il ne fait que lire. La concurrence DuckDB sur un seul fichier en écriture est exclusive. Sans ce paramètre, les tâches parallèles se bloquent mutuellement de façon non-déterministe.

!!! tip "Tester DataQualityCheckOperator en isolation"
    ```python
    from unittest.mock import MagicMock, patch
    from hr_pipeline.operators.data_quality import DataQualityCheckOperator
    from hr_pipeline.quality.checks import QualityCheck, CheckResult, Severity

    check = QualityCheck(
        name="test_check",
        description="test",
        sql="SELECT 0",
        severity=Severity.ERROR,
    )
    op = DataQualityCheckOperator(task_id="test", quality_check=check)

    mock_result = CheckResult(
        name="test_check",
        description="test",
        severity=Severity.ERROR,
        failing_rows=0,
        passed=True,
    )
    context = {"ti": MagicMock()}

    with patch("hr_pipeline.operators.data_quality.Settings"), \
         patch("hr_pipeline.operators.data_quality.get_warehouse"), \
         patch("hr_pipeline.operators.data_quality.run_check", return_value=mock_result):
        result = op.execute(context)

    assert result["passed"] is True
    context["ti"].xcom_push.assert_called_once_with(
        key="check_result", value=mock_result.as_dict()
    )
    ```

!!! tip "Étendre l'opérateur pour BigQuery"
    Si BigQuery nécessite une gestion différente (ex : timeout plus long, retry sur `503 Service Unavailable`), sous-classer `DataQualityCheckOperator` :
    ```python
    class BigQueryQualityCheckOperator(DataQualityCheckOperator):
        retries = 3
        retry_delay = timedelta(minutes=1)
    ```
    Le `execute()` est hérité sans modification.

!!! note "Pourquoi pas un provider Airflow existant ?"
    Airflow propose `BigQueryCheckOperator`, `SQLCheckOperator`, etc. Ces opérateurs génériques fonctionnent sur le principe "la requête retourne au moins une ligne = succès". Le contrat ici est différent : "retourner 0 = succès, N > 0 = N lignes en échec". De plus, l'intégration avec `QualityCheck`, la gestion `Severity.WARN`, et le push XCom structuré nécessitent des comportements non-disponibles dans les opérateurs génériques. L'opérateur custom est justifié par ces exigences spécifiques.
