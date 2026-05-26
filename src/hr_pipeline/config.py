"""Environment-driven configuration.

A single immutable :class:`Settings` object resolves every path, schema name and
credential from environment variables. The **same DAG code** therefore runs
unchanged in both targets — only the environment differs:

* ``HR_ENV=local``      → DuckDB warehouse, zero credentials (default).
* ``HR_ENV=production`` → Google BigQuery (see ``docs/production-gcp.md``).

This is the project's central design decision: configuration is injected, never
hard-coded. See ``docs/adr/0004-duckdb-local-bigquery-production.md``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class Environment(StrEnum):
    """Deployment target the pipeline runs against."""

    LOCAL = "local"
    PRODUCTION = "production"


@dataclass(frozen=True, slots=True)
class Settings:
    """Fully-resolved runtime configuration.

    Instances are immutable: build one with :meth:`from_env` at the start of a
    task and pass it down explicitly rather than re-reading the environment.
    """

    environment: Environment

    # ── Local profile (DuckDB) ──────────────────────────────────────────────
    duckdb_path: Path
    raw_data_dir: Path

    # ── dbt / Cosmos ────────────────────────────────────────────────────────
    dbt_project_dir: Path
    dbt_profiles_dir: Path

    # ── Production profile (BigQuery) ───────────────────────────────────────
    gcp_project: str | None
    bq_location: str

    # ── Warehouse layout (identical in both targets) ────────────────────────
    raw_schema: str = "raw_hr"
    marts_schema: str = "marts"

    # ── Source data contract ────────────────────────────────────────────────
    # Freshness SLA on the raw landing table — mirrors the dbt source freshness
    # block in the sibling dbt-bigquery repo.
    freshness_warn_hours: int = 12
    freshness_error_hours: int = 24

    @property
    def is_production(self) -> bool:
        """True when targeting BigQuery."""
        return self.environment is Environment.PRODUCTION

    @property
    def dbt_target(self) -> str:
        """dbt profile target name (``dev`` for DuckDB, ``prod`` for BigQuery)."""
        return "prod" if self.is_production else "dev"

    @property
    def staging_dir(self) -> Path:
        """Scratch folder where the ingestion DAG lands extracted Parquet files.

        Using the local filesystem as a landing zone keeps the parallel
        extract tasks free of DuckDB's single-writer lock.
        """
        return self.raw_data_dir.parent / "_staging"

    def validate(self) -> None:
        """Fail fast on an inconsistent configuration."""
        if self.is_production and not self.gcp_project:
            raise ValueError(
                "HR_ENV=production requires GCP_PROJECT to be set. "
                "See docs/production-gcp.md for the production setup."
            )

    @classmethod
    def from_env(cls) -> Settings:
        """Build :class:`Settings` from the process environment.

        Every variable has a sensible local default, so a fresh checkout runs
        with no ``.env`` tweaking at all.
        """
        raw_env = os.getenv("HR_ENV", "local").strip().lower()
        try:
            environment = Environment(raw_env)
        except ValueError as exc:  # pragma: no cover - defensive
            valid = ", ".join(e.value for e in Environment)
            raise ValueError(f"Invalid HR_ENV={raw_env!r}. Expected one of: {valid}.") from exc

        settings = cls(
            environment=environment,
            duckdb_path=Path(
                os.getenv("DUCKDB_PATH", "/opt/airflow/include/data/warehouse.duckdb")
            ),
            raw_data_dir=Path(os.getenv("HR_RAW_DATA_DIR", "/opt/airflow/include/data/raw")),
            dbt_project_dir=Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/include/dbt")),
            dbt_profiles_dir=Path(os.getenv("DBT_PROFILES_DIR", "/opt/airflow/include/dbt")),
            gcp_project=os.getenv("GCP_PROJECT") or None,
            bq_location=os.getenv("BQ_LOCATION", "EU"),
            raw_schema=os.getenv("BQ_DATASET_RAW", "raw_hr"),
        )
        settings.validate()
        return settings
