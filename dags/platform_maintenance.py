"""DAG ``platform_maintenance`` — weekly housekeeping for the data platform.

A portfolio pipeline is not just the happy path. Real platforms also need
operational DAGs: connectivity probes, inventory reporting, scratch-space
cleanup. This DAG demonstrates that operational maturity.

It is **time-scheduled** (weekly), unlike the asset-driven data DAGs — a
deliberate contrast between the two scheduling models Airflow 3 offers.
"""

from __future__ import annotations

import logging
import time

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

from common import DEFAULT_ARGS, DEFAULT_START_DATE
from hr_pipeline.config import Settings
from hr_pipeline.warehouse import get_warehouse

logger = logging.getLogger(__name__)

#: (schema, table) pairs probed by the inventory report.
_INVENTORY_TARGETS = (
    ("raw_hr", "employees"),
    ("staging", "stg_employees"),
    ("marts", "fct_employees_active"),
    ("marts", "dim_departments"),
    ("marts", "fct_employee_headcount_monthly"),
    ("quality", "check_runs"),
)

_DOC = """
### `platform_maintenance`

Weekly platform hygiene:

* **check_warehouse_connectivity** — proves the warehouse answers a query.
* **report_warehouse_inventory** — logs row counts for every known table.
* **prune_staging_files** — deletes Parquet scratch files older than 7 days.
"""


@dag(
    dag_id="platform_maintenance",
    description="Weekly housekeeping: connectivity, inventory, scratch cleanup.",
    schedule="@weekly",
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=_DOC,
    tags=["platform", "maintenance"],
)
def platform_maintenance() -> None:
    @task
    def check_warehouse_connectivity() -> str:
        """Smoke test — the warehouse must answer a trivial query."""
        settings = Settings.from_env()
        with get_warehouse(settings) as warehouse:
            value = warehouse.fetch_scalar("SELECT 1")
        if value != 1:
            raise AirflowException(f"Warehouse smoke test returned {value!r}, expected 1.")
        logger.info("Warehouse reachable (environment=%s).", settings.environment.value)
        return settings.environment.value

    @task
    def report_warehouse_inventory() -> dict[str, int]:
        """Log the row count of every known table — a quick platform overview."""
        settings = Settings.from_env()
        inventory: dict[str, int] = {}
        with get_warehouse(settings) as warehouse:
            for schema, table in _INVENTORY_TARGETS:
                if warehouse.table_exists(schema, table):
                    inventory[f"{schema}.{table}"] = warehouse.row_count(schema, table)
        logger.info("Warehouse inventory: %s", inventory or "no tables built yet")
        return inventory

    @task
    def prune_staging_files(retention_days: int = 7) -> dict[str, object]:
        """Delete Parquet scratch files older than the retention window."""
        settings = Settings.from_env()
        staging_dir = settings.staging_dir
        cutoff = time.time() - retention_days * 86_400
        removed: list[str] = []
        if staging_dir.exists():
            for parquet in staging_dir.glob("*.parquet"):
                if parquet.stat().st_mtime < cutoff:
                    parquet.unlink()
                    removed.append(parquet.name)
        logger.info("Pruned %s stale staging file(s): %s", len(removed), removed)
        return {"removed_count": len(removed), "removed": removed}

    connectivity = check_warehouse_connectivity()
    connectivity >> report_warehouse_inventory()
    connectivity >> prune_staging_files()


platform_maintenance()
