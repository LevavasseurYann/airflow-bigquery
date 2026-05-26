"""Seed the local DuckDB warehouse with raw_hr.employees — no Airflow needed.

Two use cases:

* **CI** — gives `dbt build` its source table without spinning up Airflow.
* **Exploration** — lets you poke at the dbt project on a laptop directly.

Usage::

    # from the repo root
    HR_RAW_DATA_DIR=./include/data/raw \\
    DUCKDB_PATH=./include/data/warehouse.duckdb \\
    python scripts/seed_local_warehouse.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src/ importable when the script is run straight from a checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hr_pipeline.config import Settings
from hr_pipeline.ingestion import (
    discover_source_files,
    extract_to_parquet,
    load_raw_employees,
)
from hr_pipeline.warehouse import get_warehouse


def main() -> int:
    settings = Settings.from_env()
    print(f"Environment  : {settings.environment.value}")
    print(f"Raw data dir : {settings.raw_data_dir}")
    print(f"DuckDB path  : {settings.duckdb_path}")

    sources = discover_source_files(settings.raw_data_dir)
    extracts = [extract_to_parquet(source, settings.staging_dir) for source in sources]

    with get_warehouse(settings) as warehouse:
        result = load_raw_employees(warehouse, settings.raw_schema, extracts)

    print(
        f"Loaded {result.rows_loaded} row(s) into {result.table} "
        f"({result.rows_deduplicated} duplicate(s) collapsed) "
        f"from regions: {', '.join(result.regions)}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
