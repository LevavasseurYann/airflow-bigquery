# Moteur de qualité des données

> **Fichiers sources** : `src/hr_pipeline/quality/checks.py` · `src/hr_pipeline/quality/suite.py`

---

## 1. Pourquoi ce fichier existe

### Décision architecturale

Le moteur de qualité répond à une question fondamentale : **comment savoir qu'un mart est fiable ?**

dbt offre des tests intégrés (`not_null`, `unique`, `relationships`…) qui s'exécutent pendant le build. Ces tests sont excellents mais ils ont une limite structurelle : ils sont couplés à la transformation. Si un test dbt échoue, le build s'arrête — mais il n'y a pas de registre d'audit, pas d'historique, pas de notion de "ce check a échoué 3 fois cette semaine".

`checks.py` et `suite.py` forment une couche indépendante — un moteur de vérification qui :

1. **Sépare** la définition d'un check (une dataclass immuable) de son exécution (une fonction pure)
2. **Normalise** le résultat en un format structuré JSON-sérialisable pour XCom
3. **Différencie** les échecs bloquants (ERROR) des avertissements non-bloquants (WARN)
4. **Reste agnostique** au moteur de stockage — le même objet `QualityCheck` tourne contre DuckDB ou BigQuery

La `suite.py` est le catalogue des checks spécifiques au projet HR. Elle utilise le moteur mais n'en fait pas partie — séparation de responsabilités entre le "comment vérifier" et le "quoi vérifier".

---

## 2. Vue d'ensemble

```
checks.py                          suite.py
┌──────────────────────────┐      ┌──────────────────────────────┐
│  Severity (enum)         │      │  FCT_ACTIVE                  │
│  QualityCheck (dataclass)│      │  DIM_DEPARTMENTS             │
│  CheckResult  (dataclass)│◄─────│  FCT_HEADCOUNT               │
│  QualityReport(dataclass)│      │                              │
│                          │      │  build_marts_quality_suite() │
│  run_check()             │      │  → list[QualityCheck]        │
│  run_suite()             │      └──────────────────────────────┘
└──────────────────────────┘
         ▲
         │  utilisé par
┌────────────────────────────┐
│  DataQualityCheckOperator  │  (operators/data_quality.py)
│  data_quality_hr (DAG)     │  (dags/data_quality_hr.py)
└────────────────────────────┘
```

**Flux de données** : un `QualityCheck` entre dans `run_check()` → un `CheckResult` en sort → plusieurs `CheckResult` sont agrégés dans un `QualityReport`.

---

## 3. Walkthrough complet

### `checks.py`

#### Import et logger

```python
import logging
from dataclasses import dataclass
from enum import StrEnum
from hr_pipeline.warehouse import Warehouse

logger = logging.getLogger(__name__)
```

`logging.getLogger(__name__)` utilise le nom de module complet comme identifiant du logger (`hr_pipeline.quality.checks`). Dans Airflow, les logs de chaque task sont capturés par le logging framework. En utilisant `__name__`, les messages apparaissent avec le bon préfixe dans le logging hiérarchique de Python — on peut configurer finement le niveau de log pour ce module sans toucher aux autres.

`Warehouse` est importé depuis `hr_pipeline.warehouse` : c'est l'abstraction commune DuckDB/BigQuery. Le moteur de qualité ne sait pas quel backend est utilisé — il appelle `warehouse.fetch_scalar(sql)` et le polymorphisme fait le reste.

---

#### `Severity` — l'enum StrEnum

```python
class Severity(StrEnum):
    ERROR = "error"  # failure fails the task (and the DAG)
    WARN = "warn"    # failure is logged but task still succeeds
```

**Pourquoi `StrEnum` et pas `Enum` ?**

`StrEnum` (Python 3.11+) fait en sorte que chaque membre est aussi une `str`. Concrètement :

```python
Severity.ERROR == "error"      # True avec StrEnum
Severity.ERROR == "error"      # False avec Enum normal
str(Severity.ERROR) == "error" # True dans les deux cas
```

L'avantage concret ici : quand on sérialise `severity.value` dans un dict pour XCom, puis qu'on désérialise avec `Severity(payload["severity"])`, la reconstruction fonctionne sans conversion explicite. Et `severity.value` renvoie directement `"error"` ou `"warn"` — lisible dans les logs sans appeler `.value` partout.

!!! note "Comparaison avec Enum classique"
    Avec `Enum`, `Severity.ERROR` est `<Severity.ERROR: 'error'>`. Dans les logs Airflow, ça produit des messages verbeux. Avec `StrEnum`, le même log affiche simplement `"error"`.

---

#### `QualityCheck` — la définition d'un check

```python
@dataclass(frozen=True, slots=True)
class QualityCheck:
    name: str
    description: str
    sql: str
    severity: Severity = Severity.ERROR
```

**`frozen=True`** : l'instance est immuable après construction. Tenter de modifier `check.sql` après création lève une `FrozenInstanceError`. Pourquoi c'est important :

- Les checks sont construits **une fois** au parse time du DAG (dans `_QUALITY_SUITE`), puis réutilisés à chaque exécution de task. Si un check pouvait être muté, un bug subtil pourrait modifier le SQL en cours d'exécution dans un environnement concurrent.
- Immuable = thread-safe by design. Airflow exécute plusieurs tasks en parallèle.

**`slots=True`** : au lieu de stocker les attributs dans un `__dict__`, Python utilise des slots mémoire dédiés. Pour une dataclass, ça réduit l'empreinte mémoire d'environ 20-30 % et accélère l'accès aux attributs. Dans un DAG qui instancie 10 objets `QualityCheck` à chaque parse (toutes les quelques secondes), c'est un gain mesurable.

**`severity: Severity = Severity.ERROR`** : le défaut est ERROR, pas WARN. Philosophie "fail safe" — tout nouveau check est bloquant jusqu'à décision explicite de l'abaisser à WARN. Ça force la réflexion : "est-ce que ce check doit être un WARNING ou un vrai blocage ?"

---

#### `CheckResult` — le résultat d'un check

```python
@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    description: str
    severity: Severity
    failing_rows: int
    passed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "severity": self.severity.value,
            "failing_rows": self.failing_rows,
            "passed": self.passed,
        }
```

**Pourquoi `CheckResult` répète `name`, `description`, `severity` déjà dans `QualityCheck` ?**

Un `CheckResult` est autonome — il peut être lu dans les logs, stocké en BDD, transmis par XCom, affiché dans l'UI, **sans avoir besoin de retrouver le `QualityCheck` original**. Si on stockait juste `failing_rows` et `passed`, l'audit trail serait inutilisable : "check #3 a échoué avec 42 lignes" ne dit rien sans le nom et la description.

**`passed: bool`** est calculé à la construction (`failing_rows == 0`), mais stocké comme champ explicite. Alternative : une `@property`. Le choix du champ stocké est délibéré : le résultat est sérialisé dans XCom, et une `@property` n'est pas sérialisable directement. Avoir `passed` comme champ permet de l'inclure dans `as_dict()` et de le lire côté consommateur sans recalcul.

**`as_dict()` et la contrainte XCom** : Airflow XCom sérialise les valeurs retournées par `execute()` en JSON. Les dataclasses Python ne sont pas JSON-sérialisables nativement — il faut une conversion explicite. La convention `as_dict()` sur chaque dataclass du module garantit que la sérialisation est toujours disponible et cohérente.

!!! warning "Sérialisation XCom"
    Airflow 3 utilise un `ObjectSerializer` configurable, mais le défaut reste JSON. Si vous retournez une dataclass directement depuis `execute()`, vous obtiendrez une `TypeError` à l'exécution, pas au parse. `as_dict()` évite ce piège.

---

#### `QualityReport` — l'agrégat

```python
@dataclass(frozen=True, slots=True)
class QualityReport:
    results: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(result.passed for result in self.results)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity is Severity.WARN]

    def has_blocking_failures(self) -> bool:
        return bool(self.blocking_failures)
```

**`passed` est une `@property`** plutôt qu'un champ calculé à la construction. Pourquoi ? `QualityReport` est construit dans le DAG à partir des résultats XCom — à ce moment, tous les `CheckResult` sont déjà figés. La propriété exprime clairement que `passed` est **dérivé** des résultats, pas une valeur indépendante.

**`has_blocking_failures()`** est une méthode et non une propriété, contrairement à `blocking_failures`. Convention : les méthodes booléennes préfixées `has_` / `is_` sont des méthodes (pas des propriétés) dans ce codebase, pour rester cohérent avec le style "question" (`report.has_blocking_failures()` se lit comme une question, `report.blocking_failures` comme un accès à une collection).

**`summary()`** — le format humain :

```python
def summary(self) -> str:
    passed = sum(1 for r in self.results if r.passed)
    lines = [f"Data quality: {passed}/{len(self.results)} checks passed."]
    for result in self.results:
        if not result.passed:
            lines.append(
                f"  - [{result.severity.value.upper()}] {result.name}: "
                f"{result.failing_rows} failing row(s) — {result.description}"
            )
    return "\n".join(lines)
```

Ce format est conçu pour les logs Airflow. Exemple de sortie :

```
Data quality: 8/10 checks passed.
  - [ERROR] fct_active__employee_id_unique: 3 failing row(s) — employee_id is the grain...
  - [WARN] fct_active__freshness: 1 failing row(s) — The marts must have been rebuilt...
```

Lisible dans le log viewer d'Airflow, exportable dans un message d'alerte Slack, compréhensible sans contexte.

---

#### `run_check()` — la fonction d'exécution

```python
def run_check(warehouse: Warehouse, check: QualityCheck) -> CheckResult:
    raw = warehouse.fetch_scalar(check.sql)
    failing_rows = int(raw or 0)
    return CheckResult(
        name=check.name,
        description=check.description,
        severity=check.severity,
        failing_rows=failing_rows,
        passed=failing_rows == 0,
    )
```

**Le contrat SQL : retourner un entier**

C'est le cœur de tout le système. Chaque check SQL doit retourner **exactement un entier** : le nombre de lignes qui violent la règle. Zéro = succès. N > 0 = N lignes problématiques.

```sql
-- Pattern simple : COUNT des violations
SELECT count(*) FROM marts.fct_employees_active WHERE salary < 0

-- Pattern duplicates : sous-requête pour compter les clés en double
SELECT count(*) FROM (
  SELECT employee_id FROM marts.fct_employees_active
  GROUP BY employee_id HAVING count(*) > 1
) duplicated_keys

-- Pattern inversé : table vide → retourner 1
SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END
FROM marts.fct_employees_active
```

!!! note "La même convention que dbt"
    Les tests dbt (`not_null`, `unique`, etc.) compilent en SQL qui retourne le nombre de lignes en échec. Zéro = success. N > 0 = test failure. En adoptant la même convention, n'importe quel développeur familier avec dbt comprend immédiatement comment écrire un check.

**`int(raw or 0)` — la défense contre None**

`fetch_scalar()` peut retourner `None` dans deux cas :
- La requête SQL ne renvoie aucune ligne (table vide dans la sous-requête de freshness par exemple)
- La requête renvoie `NULL` (ex : `SELECT max(col) FROM table_vide` → `NULL`)

`None or 0` vaut `0` en Python. Puis `int(0)` vaut `0`. Résultat : une table vide fait passer le check plutôt que de lever une exception. Est-ce le bon comportement ? Pour la majorité des checks, oui — si la table est vide, il n'y a pas de lignes avec `salary < 0`. Pour le check `fct_active__not_empty`, le SQL est justement conçu pour retourner `1` si la table est vide :

```sql
SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM marts.fct_employees_active
```

**`run_check` ne raise jamais** — il retourne toujours un `CheckResult`. La décision de lever une exception appartient à l'opérateur Airflow, pas à cette fonction. C'est la séparation de responsabilités : le moteur calcule, l'orchestrateur décide.

---

#### `run_suite()` — l'exécution en lot

```python
def run_suite(warehouse: Warehouse, checks: list[QualityCheck]) -> QualityReport:
    results: list[CheckResult] = []
    for check in checks:
        result = run_check(warehouse, check)
        if result.passed:
            logger.info("PASS  %s", check.name)
        elif check.severity is Severity.ERROR:
            logger.error("FAIL  %s — %s failing row(s)", check.name, result.failing_rows)
        else:
            logger.warning("WARN  %s — %s failing row(s)", check.name, result.failing_rows)
        results.append(result)
    return QualityReport(results=results)
```

`run_suite` n'est pas utilisée directement dans le DAG principal (qui génère des tasks individuelles), mais elle existe pour des usages hors-Airflow : scripts de validation manuelle, tests d'intégration, exécution locale en dehors du cluster.

Le logging utilise des niveaux distincts selon la sévérité : `logger.info` pour les succès, `logger.error` pour les ERROR, `logger.warning` pour les WARN. Dans un système d'observabilité (Datadog, CloudWatch…), les `ERROR` remontent en alerte automatiquement.

---

### `suite.py`

#### Les constantes de table

```python
FCT_ACTIVE = "fct_employees_active"
DIM_DEPARTMENTS = "dim_departments"
FCT_HEADCOUNT = "fct_employee_headcount_monthly"
```

Ces constantes sont définies au niveau module, pas dans la fonction. Pourquoi ? Si le mart `fct_employees_active` est renommé en `fct_employees_current`, une seule ligne change et tous les checks de la suite sont mis à jour automatiquement. Sans cette indirection, il faudrait chercher/remplacer dans 6 SQL différents — avec le risque d'en oublier un.

---

#### `build_marts_quality_suite()` — la factory

```python
def build_marts_quality_suite(
    *,
    marts_schema: str = "marts",
    freshness_max_age_hours: int = 24,
) -> list[QualityCheck]:
```

**`*` pour les keyword-only arguments** : force l'appelant à nommer les paramètres. `build_marts_quality_suite("marts", 48)` est une erreur ; `build_marts_quality_suite(marts_schema="marts", freshness_max_age_hours=48)` est obligatoire. Pour une fonction publique utilisée dans un DAG, c'est une protection contre les erreurs d'ordre d'arguments.

**Retourne une `list`, pas une classe dédiée** : les checks sont des données. Une liste est le conteneur minimal, iterable, indexable, compatible avec tout. Un objet `QualitySuite` serait une abstraction prématurée — il faudrait lui ajouter des méthodes `.add()`, `.filter_by_severity()`, etc. qui ne servent pas encore.

**Paramètre `marts_schema`** : permet d'exécuter la même suite contre `marts` (DuckDB local) ou `my_project.marts` (BigQuery). Le DAG passe `os.getenv("BQ_DATASET_MARTS", "marts")` — la suite elle-même ne sait pas d'où vient la valeur.

---

#### Les checks ERROR — anatomie

| Check | SQL pattern | Règle métier |
|---|---|---|
| `employee_id_not_null` | `WHERE col IS NULL` | Intégrité de la clé primaire |
| `employee_id_unique` | `GROUP BY … HAVING count(*) > 1` | Grain de la fact table |
| `salary_non_negative` | `WHERE salary < 0` | Contrainte domaine |
| `no_future_hire_dates` | `WHERE hire_date > CURRENT_DATE` | Règle métier RH |
| `years_of_service_non_negative` | `WHERE years_of_service < 0` | Dérivé cohérent |
| `department_referential_integrity` | `LEFT JOIN … WHERE d.dept IS NULL` | Intégrité référentielle |
| `dim_departments__department_unique` | `GROUP BY … HAVING count(*) > 1` | Grain de la dimension |
| `headcount__non_negative` | `WHERE headcount < 0` | Impossible physiquement |

**Le pattern LEFT JOIN pour l'intégrité référentielle :**

```sql
SELECT count(*) FROM marts.fct_employees_active f
  LEFT JOIN marts.dim_departments d ON f.department = d.department
  WHERE d.department IS NULL
```

Un `INNER JOIN` retournerait les lignes qui ont une correspondance. Un `LEFT JOIN` garde **toutes** les lignes de `fct_employees_active`, même celles sans correspondance dans `dim_departments`. Le `WHERE d.department IS NULL` filtre justement ces orphelines — c'est le pattern standard pour détecter des violations de clé étrangère en SQL analytique.

!!! tip "Pourquoi pas une contrainte FK en base ?"
    DuckDB et BigQuery ne font pas respecter les contraintes de clé étrangère au niveau du moteur. Les FK sont déclaratives (pour la documentation / l'optimisation de requêtes) mais pas enforced. Un check SQL explicite est la seule façon de détecter ces violations.

---

#### Les checks WARN — pourquoi ce niveau ?

```python
QualityCheck(
    name="fct_active__not_empty",
    ...
    severity=Severity.WARN,
),
QualityCheck(
    name="fct_active__freshness",
    ...
    severity=Severity.WARN,
),
```

**`fct_active__not_empty` est WARN** parce qu'une table vide peut avoir une explication légitime : premier déploiement, environnement de test, régression connue en cours de correction. Fail le DAG automatiquement pour une table vide empêcherait les builds de passer en CI. Le WARN alerte sans bloquer.

**`fct_active__freshness` est WARN** : si les marts ont été buildés il y a 25h au lieu de 24h (un pipeline qui a pris du retard), le DAG de qualité ne doit pas re-fail et créer du bruit d'alerte en cascade. La fraîcheur est une métrique de monitoring, pas un critère bloquant.

---

#### Le check de fraîcheur — un bug potentiel évité

```python
sql=(
    f"SELECT count(*) FROM ("
    f"  SELECT max(dbt_loaded_at) AS last_loaded_at FROM {fct}"
    f") latest"
    f"  WHERE last_loaded_at IS NULL"
    f"  OR CAST(last_loaded_at AS TIMESTAMP)"
    f"   < CURRENT_TIMESTAMP - INTERVAL '{freshness_max_age_hours}' HOUR"
),
```

**Le piège du timestamp figé**

Mauvaise implémentation (ne pas faire) :

```python
from datetime import datetime, timedelta
threshold = datetime.now() - timedelta(hours=freshness_max_age_hours)
sql = f"SELECT count(*) FROM ... WHERE last_loaded_at < '{threshold}'"
```

Problème : `build_marts_quality_suite()` est appelée **au parse time** du DAG (dans `_QUALITY_SUITE`). Le processeur Airflow parse les DAGs toutes les quelques secondes et met les modules en cache. Si le module est en cache, `datetime.now()` est appelé une seule fois et la valeur est figée dans le SQL. La "fraîcheur" serait comparée à un timestamp de plusieurs heures ou jours — le check serait toujours en échec ou jamais en échec selon le timing.

**La bonne approche** : `CURRENT_TIMESTAMP` est une fonction SQL évaluée par le moteur de base de données **au moment où la requête s'exécute**, pas au moment où la chaîne SQL est construite. Le SQL est une chaîne statique (safe au cache), l'évaluation temporelle est dynamique.

**`CAST(last_loaded_at AS TIMESTAMP)`** : `dbt_loaded_at` est de type `timestamptz` (timestamp with timezone) dans DuckDB et `TIMESTAMP` dans BigQuery — deux types différents. Le `CAST` normalise pour que la comparaison avec `CURRENT_TIMESTAMP` fonctionne dans les deux backends sans erreur de type.

**`OR last_loaded_at IS NULL`** : si `max(dbt_loaded_at)` retourne NULL (table vide ou colonne jamais remplie), la comparaison `NULL < CURRENT_TIMESTAMP - INTERVAL '24' HOUR` est `NULL` (pas `TRUE` en SQL). La clause `OR IS NULL` rend le check explicitement non-frais quand la colonne n'a pas de valeur.

---

## 4. Connexions

```
suite.py
  └── build_marts_quality_suite()
        │ retourne list[QualityCheck]
        ▼
dags/data_quality_hr.py
  └── _QUALITY_SUITE (parse time)
        │ itération → DataQualityCheckOperator(quality_check=check)
        ▼
operators/data_quality.py
  └── DataQualityCheckOperator.execute()
        └── run_check(warehouse, check)  ← checks.py
              └── warehouse.fetch_scalar(check.sql)
                    └── DuckDBWarehouse ou BigQueryWarehouse  ← warehouse.py
```

**`include/dbt/`** : dbt teste les mêmes contraintes pendant le build (`not_null`, `unique`, `relationships`). Cette suite les re-vérifie après coup pour produire un audit trail indépendant. Les deux couches se complètent : dbt bloque la promotion d'un modèle défaillant, ce moteur enregistre l'état des marts après publication.

---

## 5. Pièges & gotchas

!!! danger "SQL retournant plusieurs lignes"
    Si le SQL d'un check retourne plusieurs lignes (erreur de requête), `fetch_scalar()` ne prend que la première valeur. Le check peut passer ou échouer de façon aléatoire selon l'ordre retourné. Tous les checks de la suite utilisent `SELECT count(*)` ou une sous-requête qui garantit une seule ligne.

!!! warning "Le type de retour de `fetch_scalar()`"
    `fetch_scalar()` retourne un `Any`. Sur BigQuery, les entiers peuvent être retournés comme `Decimal` ou `int64` selon la version du client. Le `int(raw or 0)` dans `run_check()` normalise explicitement en `int` Python. Si vous utilisez `raw == 0` sans cast, une comparaison `Decimal('0') == 0` peut être surprenante.

!!! warning "Ordre des checks dans la suite"
    Les checks sont retournés dans l'ordre de la liste. Dans le DAG, ils sont convertis en tâches Airflow indépendantes (pas de dépendances entre elles). L'ordre dans la liste ne garantit pas l'ordre d'exécution. Si deux checks sont logiquement dépendants (ex : vérifier l'unicité avant l'intégrité référentielle), c'est acceptable ici car un échec d'unicité ne crashe pas les autres checks — ils s'exécutent tous.

!!! tip "Ajouter un check"
    1. Écrire le SQL sur la base de données (DuckDB ou BigQuery) et vérifier qu'il retourne 0 sur des données propres
    2. Ajouter un `QualityCheck(...)` dans `build_marts_quality_suite()`
    3. Décider `Severity.ERROR` ou `Severity.WARN` — le défaut est ERROR
    4. `pytest -m "not dags"` pour vérifier que les tests passent
    5. Le nouveau check apparaîtra automatiquement dans le DAG comme une nouvelle tâche `check__<name>`

!!! note "Tests unitaires de la suite"
    `tests/test_quality.py` teste `run_check()` et `run_suite()` avec un warehouse DuckDB en mémoire. La suite entière peut être testée localement sans Airflow ni BigQuery. Voir la section Tests de la documentation pour les patterns de mock.
