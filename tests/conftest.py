"""Shared pytest fixtures.

The pure-Python core of hr_pipeline (config, warehouse, ingestion, quality) is
tested here with no Airflow involved — fast, hermetic unit tests.
"""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
import pytest

from hr_pipeline.config import Environment, Settings
from hr_pipeline.warehouse import DuckDBWarehouse


@pytest.fixture
def sample_employees() -> pd.DataFrame:
    """A small raw-employees DataFrame — all columns as strings, like ingestion."""
    return pd.DataFrame(
        {
            "employee_id": ["E1", "E2", "E3", "E4"],
            "email": ["a@example.com", "b@example.com", "c@example.com", "d@example.com"],
            "hire_date": ["2020-01-01", "2021-06-15", "2019-03-30", "2023-09-01"],
            "updated_at": ["2026-05-20 10:00:00"] * 4,
            "is_active": ["true", "true", "false", "true"],
            "department": ["engineering", "sales", "hr", "finance"],
            "salary": ["90000", "70000", "60000", "110000"],
        }
    )


@pytest.fixture
def duckdb_warehouse(tmp_path) -> Iterator[DuckDBWarehouse]:
    """A throwaway DuckDB warehouse backed by a temp file."""
    warehouse = DuckDBWarehouse(tmp_path / "test.duckdb")
    try:
        yield warehouse
    finally:
        warehouse.close()


@pytest.fixture
def local_settings(tmp_path) -> Settings:
    """A local Settings object pointing entirely at the temp directory."""
    return Settings(
        environment=Environment.LOCAL,
        duckdb_path=tmp_path / "warehouse.duckdb",
        raw_data_dir=tmp_path / "raw",
        dbt_project_dir=tmp_path / "dbt",
        dbt_profiles_dir=tmp_path / "dbt",
        gcp_project=None,
        bq_location="EU",
    )
