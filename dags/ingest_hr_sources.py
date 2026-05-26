"""DAG ``ingest_hr_sources`` — land regional HR extracts into ``raw_hr.employees``.

Airflow 3 features on display here:

* the **TaskFlow API** (``@dag`` / ``@task``) for clean, Pythonic tasks;
* **dynamic task mapping** (``.expand``) — one parallel extract task per
  discovered source file, decided at run time, not at authoring time;
* **asset producer** — the final task carries ``outlets=[RAW_HR_EMPLOYEES]``,
  which automatically triggers the ``transform_hr_dbt`` DAG.

Pipeline shape::

    discover_sources  ─►  extract_region (mapped, parallel)  ─►  load_raw
"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import dag, task

from common import DEFAULT_ARGS, DEFAULT_START_DATE
from hr_pipeline.assets import RAW_HR_EMPLOYEES
from hr_pipeline.config import Settings
from hr_pipeline.ingestion import (
    RegionExtract,
    SourceFile,
    discover_source_files,
    extract_to_parquet,
    load_raw_employees,
)
from hr_pipeline.warehouse import get_warehouse

_DOC = """
### `ingest_hr_sources`

Extracts every `employees_*.csv` regional extract, validates and lands each one
as Parquet **in parallel**, then consolidates them into the `raw_hr.employees`
table the dbt project reads from.

* **Schedule** — daily.
* **Produces asset** — `raw_hr_employees` → triggers `transform_hr_dbt`.
* **Idempotent** — the raw table is fully replaced on every run; duplicates
  across regions collapse last-write-wins on `updated_at`.
"""


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
    @task
    def discover_sources() -> list[dict[str, str]]:
        """Find every regional extract on disk. Returns plain dicts for XCom."""
        settings = Settings.from_env()
        files = discover_source_files(settings.raw_data_dir)
        return [{"path": str(f.path), "region": f.region} for f in files]

    @task
    def extract_region(source: dict[str, str]) -> dict[str, object]:
        """Validate and land one region file as Parquet (a mapped task)."""
        settings = Settings.from_env()
        source_file = SourceFile(path=Path(source["path"]), region=source["region"])
        extract = extract_to_parquet(source_file, settings.staging_dir)
        return extract.as_dict()

    @task(outlets=[RAW_HR_EMPLOYEES])
    def load_raw(extracts: list[dict[str, object]]) -> dict[str, object]:
        """Consolidate every landed Parquet into raw_hr.employees."""
        settings = Settings.from_env()
        region_extracts = [RegionExtract.from_dict(item) for item in extracts]
        with get_warehouse(settings) as warehouse:
            result = load_raw_employees(warehouse, settings.raw_schema, region_extracts)
        return result.as_dict()

    sources = discover_sources()
    # .expand fans the task out — one mapped instance per discovered file.
    extracts = extract_region.expand(source=sources)
    load_raw(extracts)


ingest_hr_sources()
