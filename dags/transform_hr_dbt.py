"""DAG ``transform_hr_dbt`` — run the embedded dbt project through Cosmos.

This is the orchestration centrepiece. Instead of a single opaque
``dbt build`` task, **Cosmos** parses the dbt project and renders every model,
seed, snapshot and test as a *native Airflow task*. You get per-model retries,
logs, lineage and observability — dbt and Airflow fully integrated.

Airflow 3 features on display:

* **asset-driven scheduling** — ``schedule=[RAW_HR_EMPLOYEES]``: the DAG runs
  the moment ``ingest_hr_sources`` publishes fresh raw data, with no clock and
  no sensor;
* **asset producer** — ``publish_marts`` carries ``outlets=[HR_MARTS]``, which
  in turn triggers ``data_quality_hr``.

DuckDB is single-writer, so every dbt task runs in the ``dbt`` pool (size 1):
the per-model graph is fully visible in the UI, but execution is serialised.
On BigQuery the pool can simply be widened — no code change.
"""

from __future__ import annotations

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode

from common import DEFAULT_ARGS, DEFAULT_START_DATE
from hr_pipeline.assets import HR_MARTS, RAW_HR_EMPLOYEES
from hr_pipeline.config import Settings
from hr_pipeline.ingestion import RAW_EMPLOYEES_TABLE
from hr_pipeline.warehouse import get_warehouse

# Resolved once at parse time — the DAG processor has the full environment.
_settings = Settings.from_env()

# ── Cosmos configuration ────────────────────────────────────────────────────
# ProfileConfig points Cosmos at the in-repo profiles.yml; the target (dev /
# prod) follows HR_ENV, so the very same DAG drives DuckDB or BigQuery.
_profile_config = ProfileConfig(
    profile_name="hr_analytics",
    target_name=_settings.dbt_target,
    profiles_yml_filepath=str(_settings.dbt_project_dir / "profiles.yml"),
)
_project_config = ProjectConfig(dbt_project_path=str(_settings.dbt_project_dir))
# LOCAL: Cosmos runs the dbt installed alongside Airflow in the worker image.
_execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

_DOC = """
### `transform_hr_dbt`

Runs the embedded **dbt** project (`include/dbt`) via **Cosmos**: staging →
intermediate → marts, plus seeds, the SCD2 snapshot and every dbt test, each as
its own Airflow task.

* **Schedule** — asset-driven on `raw_hr_employees`.
* **Produces asset** — `hr_marts` → triggers `data_quality_hr`.
* **Pool** — every dbt task runs in the `dbt` pool (size 1) to respect DuckDB's
  single-writer model.
"""


@dag(
    dag_id="transform_hr_dbt",
    description="Transform raw HR data into curated marts with dbt (via Cosmos).",
    schedule=[RAW_HR_EMPLOYEES],
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=_DOC,
    tags=["hr", "transformation", "dbt", "layer:marts"],
)
def transform_hr_dbt() -> None:
    @task
    def verify_raw_source() -> dict[str, int]:
        """Fail fast if the raw table dbt depends on is missing or empty."""
        settings = Settings.from_env()
        with get_warehouse(settings, read_only=True) as warehouse:
            if not warehouse.table_exists(settings.raw_schema, RAW_EMPLOYEES_TABLE):
                raise AirflowException(
                    f"{settings.raw_schema}.{RAW_EMPLOYEES_TABLE} does not exist — "
                    "run the ingest_hr_sources DAG first."
                )
            rows = warehouse.row_count(settings.raw_schema, RAW_EMPLOYEES_TABLE)
        if rows == 0:
            raise AirflowException(f"{settings.raw_schema}.{RAW_EMPLOYEES_TABLE} is empty.")
        return {"raw_rows": rows}

    # Cosmos renders the whole dbt project as a task group.
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        operator_args={"pool": "dbt"},
        default_args={"retries": 1},
    )

    @task(outlets=[HR_MARTS])
    def publish_marts() -> dict[str, int]:
        """Confirm the marts were built and publish the `hr_marts` asset."""
        settings = Settings.from_env()
        marts = (
            "fct_employees_active",
            "dim_departments",
            "fct_employee_headcount_monthly",
        )
        with get_warehouse(settings, read_only=True) as warehouse:
            counts = {mart: warehouse.row_count(settings.marts_schema, mart) for mart in marts}
        for mart, count in counts.items():
            if count == 0:
                raise AirflowException(f"Mart {mart} is empty after the dbt run.")
        return counts

    verify_raw_source() >> dbt_transform >> publish_marts()


transform_hr_dbt()
