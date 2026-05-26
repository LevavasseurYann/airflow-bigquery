# Deep Dive — `src/hr_pipeline/assets.py`

> Annotation complète des Airflow Assets. Comprendre pourquoi l'Asset-driven scheduling remplace ExternalTaskSensor et les offsets cron.

---

## Pourquoi ce fichier existe

Avant Airflow 3 (et son précurseur les "Datasets" d'Airflow 2.4+), orchestrer des dépendances inter-DAGs nécessitait soit :

- **`ExternalTaskSensor`** : une tâche qui poll l'état d'une tâche dans un autre DAG. Fragile (couplage sur les noms de DAG et de tâche), bloquant (occupe un worker slot pendant le poll), et silencieusement cassé si l'upstream change de nom.
- **Offsets cron** : `DAG_A` à `0 2 * * *`, `DAG_B` à `0 4 * * *`. Fonctionne si les durées sont prévisibles, échoue dès qu'un DAG prend plus de temps que prévu.

Airflow 3 introduit les **Assets** (anciennement "Datasets") : un mécanisme de scheduling basé sur l'état des données plutôt que sur le temps. Un DAG producteur **émet** un asset quand il termine avec succès. Tous les DAGs consommateurs de cet asset sont **automatiquement déclenchés**.

`assets.py` est le module qui définit ces objets Asset partagés entre producteurs et consommateurs.

!!! note "Pourquoi un module dédié ?"
    Producteur et consommateur doivent référencer **exactement le même objet Asset** (même `uri`).
    Si chacun crée sa propre instance `Asset(uri="warehouse://raw_hr/employees")` dans son propre fichier,
    Airflow pourrait traiter les deux comme des assets différents en cas de désynchronisation.
    Un module central garantit qu'il n'existe qu'une seule définition — importée partout.

---

## Vue d'ensemble

`assets.py` définit deux objets Asset représentant des étapes clés du pipeline HR :

- `RAW_HR_EMPLOYEES` : données brutes des employés chargées dans le warehouse (output de l'ingestion).
- `HR_MARTS` : marts dbt transformés et prêts pour la consommation analytique (output de Cosmos/dbt).

Ces deux objets sont le **câblage déclaratif** du pipeline : ils remplacent toute configuration de dépendance dans Airflow UI ou dans des fichiers de configuration séparés. Le pipeline entier est décrit dans les signatures des DAGs qui référencent ces assets.

---

## Walkthrough complet

### Import

```python
from __future__ import annotations
from airflow.sdk import Asset
```

`Asset` est importé depuis `airflow.sdk` — le nouveau point d'entrée Airflow 3 pour les types publics. En Airflow 2.x, l'équivalent était `airflow.datasets.Dataset`. Le renommage `Dataset` → `Asset` reflète l'élargissement du concept : un asset peut représenter n'importe quelle donnée produite, pas seulement un "dataset" au sens table SQL.

!!! warning "Compatibilité Airflow 2 vs 3"
    `from airflow.sdk import Asset` ne fonctionne qu'en Airflow 3.x.
    En Airflow 2.4–2.9, c'est `from airflow import Dataset`.
    Ce projet cible Airflow 3.1 — l'import `airflow.sdk` est correct et intentionnel.

---

### `RAW_HR_EMPLOYEES`

```python
RAW_HR_EMPLOYEES = Asset(
    name="raw_hr_employees",
    uri="warehouse://raw_hr/employees",
    group="raw",
)
```

**`name`** : identifiant lisible par les humains, affiché dans l'Airflow UI dans la vue Assets. Doit être unique dans le deployment Airflow.

**`uri`** : identifiant technique de l'asset. C'est la clé de matching entre producteurs et consommateurs — deux `Asset` avec le même `uri` sont le même asset pour Airflow. Le schéma `warehouse://` est un **pseudo-protocole** inventé pour ce projet : Airflow ne l'interprète pas, il est purement documentaire. Il indique "cette donnée vit dans notre warehouse" sans préciser DuckDB ou BigQuery (qui sont des détails d'implémentation cachés derrière `warehouse.py`).

!!! note "L'URI n'est pas une URL réelle"
    Airflow ne fait pas de requête HTTP sur `warehouse://raw_hr/employees`.
    C'est un identifiant opaque — seule sa valeur de chaîne compte pour le matching.
    La convention `warehouse://schema/table` est lisible et documentaire, mais
    `urn:hr:raw_hr:employees` ou `raw_hr.employees` seraient également valides.

**`group`** : métadonnée de catégorisation pour l'Airflow UI. Les assets du groupe `raw` apparaissent ensemble dans la vue Assets. Pas d'impact fonctionnel sur le scheduling.

---

### `HR_MARTS`

```python
HR_MARTS = Asset(
    name="hr_marts",
    uri="warehouse://marts/hr",
    group="marts",
)
```

Même structure que `RAW_HR_EMPLOYEES`. Représente l'ensemble des marts dbt (`dim_departments`, `fct_employees_active`, `fct_employee_headcount_monthly`) comme un seul asset — une simplification délibérée. Une modélisation plus fine créerait un Asset par mart, permettant à `data_quality_hr` de ne se déclencher que quand un mart spécifique change. Pour un pipeline HR de cette taille, la granularité par couche (raw / marts) est suffisante.

---

## Connexions — le câblage du pipeline

Voici comment les DAGs utilisent ces assets :

### DAG 1 : `ingest_hr_sources` — producteur de `RAW_HR_EMPLOYEES`

```python
# dags/ingest_hr_sources.py
from hr_pipeline.assets import RAW_HR_EMPLOYEES

with DAG(
    dag_id="ingest_hr_sources",
    schedule="@daily",           # déclenchement cron classique (entrée du pipeline)
    outlets=[RAW_HR_EMPLOYEES],  # émet cet asset à la fin
    ...
) as dag:
    ...
```

`outlets=[RAW_HR_EMPLOYEES]` signifie : quand ce DAG termine avec succès, Airflow marque `RAW_HR_EMPLOYEES` comme "updated". C'est le signal de départ pour les DAGs en aval.

### DAG 2 : `transform_hr_dbt` — consommateur de `RAW_HR_EMPLOYEES`, producteur de `HR_MARTS`

```python
# dags/transform_hr_dbt.py
from hr_pipeline.assets import RAW_HR_EMPLOYEES, HR_MARTS

DbtDag(
    dag_id="transform_hr_dbt",
    schedule=[RAW_HR_EMPLOYEES],  # déclenché quand RAW_HR_EMPLOYEES est mis à jour
    outlets=[HR_MARTS],           # émet HR_MARTS à la fin
    ...
)
```

`schedule=[RAW_HR_EMPLOYEES]` remplace `schedule="@daily"` : ce DAG ne se déclenche pas à heure fixe, il se déclenche **dès que les données brutes sont prêtes**. Si l'ingestion prend 2h au lieu de 30 min, `transform_hr_dbt` attend automatiquement.

### DAG 3 : `data_quality_hr` — consommateur de `HR_MARTS`

```python
# dags/data_quality_hr.py
from hr_pipeline.assets import HR_MARTS

with DAG(
    dag_id="data_quality_hr",
    schedule=[HR_MARTS],  # déclenché quand les marts sont prêts
    ...
) as dag:
    ...
```

### Vue graphique du pipeline

```
[cron @daily]
      │
      ▼
┌─────────────────────┐
│  ingest_hr_sources  │ ──outlets──► RAW_HR_EMPLOYEES
└─────────────────────┘
                                           │
                                    schedule trigger
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │    transform_hr_dbt    │ ──outlets──► HR_MARTS
                              │    (Cosmos/dbt)        │
                              └────────────────────────┘
                                                              │
                                                       schedule trigger
                                                              │
                                                              ▼
                                                 ┌────────────────────────┐
                                                 │    data_quality_hr     │
                                                 └────────────────────────┘
```

!!! tip "Asset-driven scheduling en pratique"
    Dans l'Airflow UI (Airflow 3), la vue **Assets** affiche le graphe de dépendances entre assets
    et DAGs. On peut y voir en temps réel quels assets ont été mis à jour et quels DAGs ont été
    déclenchés. C'est une vue du pipeline orientée données, complémentaire à la vue DAG orientée
    tâches.

---

## Comparaison avec les approches alternatives

| Approche | Avantages | Inconvénients |
|---|---|---|
| **Asset-driven (ce projet)** | Découplage temporel, UI dédiée, fail-safe | Airflow 3 uniquement, moins de contrôle fin |
| **ExternalTaskSensor** | Compatible toutes versions | Couplage fort sur noms, bloque un worker slot |
| **Offset cron** (`0 4 * * *` après `0 2 * * *`) | Simple à comprendre | Fragile si durées variables, heure fixe |
| **TriggerDagRunOperator** | Contrôle total | Code impératif, difficile à visualiser |

---

## Pièges & gotchas

!!! warning "Le piège de la double définition"
    ```python
    # dags/ingest_hr_sources.py — MAUVAIS
    from airflow.sdk import Asset
    RAW_HR_EMPLOYEES = Asset(uri="warehouse://raw_hr/employees")  # redéfinition locale !

    # dags/transform_hr_dbt.py — MAUVAIS
    from airflow.sdk import Asset
    RAW_HR_EMPLOYEES = Asset(uri="warehouse://raw_hr/employees")  # autre instance !
    ```
    Airflow compare les URIs comme strings — deux instances avec le même URI *devraient* fonctionner.
    Mais si l'URI diverge par une faute de frappe (`raw_hr_employees` vs `raw_hr/employees`),
    le lien de scheduling est silencieusement cassé. Toujours importer depuis `hr_pipeline.assets`.

!!! warning "`outlets` doit être une liste, pas un scalaire"
    ```python
    # MAUVAIS — passe silencieusement mais ne fonctionne pas
    outlets=RAW_HR_EMPLOYEES

    # CORRECT
    outlets=[RAW_HR_EMPLOYEES]
    ```
    Airflow 3 accepte les deux syntaxes dans certaines versions, mais la liste est le comportement
    documenté. Préférer toujours la liste.

!!! warning "Asset non émis si une tâche upstream échoue"
    Si `ingest_hr_sources` échoue à mi-chemin, l'asset `RAW_HR_EMPLOYEES` n'est **pas** émis.
    `transform_hr_dbt` ne se déclenche pas. C'est le comportement souhaité (on ne transforme pas
    des données partielles), mais il faut être conscient qu'un échec en amont bloque silencieusement
    toute la chaîne.

!!! tip "Simuler un asset update pour déboguer"
    En développement, si on veut déclencher manuellement `transform_hr_dbt` sans relancer
    l'ingestion complète, on peut "marquer" l'asset comme mis à jour via l'UI Airflow :
    **Assets → RAW_HR_EMPLOYEES → Mark as updated**.
    Cela déclenche le DAG consommateur sans exécuter le producteur.

!!! tip "Ajouter un nouvel asset"
    Pour ajouter un asset représentant, par exemple, les données RH enrichies :
    ```python
    # src/hr_pipeline/assets.py
    HR_ENRICHED = Asset(
        name="hr_enriched",
        uri="warehouse://enriched/hr",
        group="enriched",
    )
    ```
    Puis dans le DAG producteur : `outlets=[HR_ENRICHED]`.
    Puis dans le DAG consommateur : `schedule=[HR_ENRICHED]`.
    Aucune autre configuration n'est nécessaire — le câblage est déclaratif.
