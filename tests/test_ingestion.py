"""Unit tests for hr_pipeline.ingestion."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hr_pipeline.ingestion import (
    IngestionError,
    SourceFile,
    discover_source_files,
    extract_to_parquet,
    load_raw_employees,
)

_HEADER = "employee_id,email,hire_date,updated_at,is_active,department,salary"


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join([_HEADER, *rows]) + "\n", encoding="utf-8")


def test_source_file_region_is_parsed_from_name():
    assert SourceFile.from_path(Path("/data/employees_eu.csv")).region == "eu"
    assert SourceFile.from_path(Path("/data/employees_apac.csv")).region == "apac"


def test_discover_raises_when_no_files(tmp_path):
    with pytest.raises(IngestionError, match="No source files"):
        discover_source_files(tmp_path)


def test_discover_finds_and_sorts_files(tmp_path):
    _write_csv(
        tmp_path / "employees_us.csv",
        ["E1,a@x.com,2020-01-01,2026-05-20 10:00:00,true,sales,50000"],
    )
    _write_csv(
        tmp_path / "employees_eu.csv", ["E2,b@x.com,2021-01-01,2026-05-20 10:00:00,true,hr,60000"]
    )
    regions = [sf.region for sf in discover_source_files(tmp_path)]
    assert regions == ["eu", "us"]  # sorted


def test_extract_to_parquet_adds_audit_columns(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_csv(
        raw / "employees_eu.csv",
        [
            "E1,a@x.com,2020-01-01,2026-05-20 10:00:00,true,sales,50000",
            "E2,b@x.com,2021-01-01,2026-05-20 11:00:00,true,hr,60000",
        ],
    )
    extract = extract_to_parquet(
        SourceFile.from_path(raw / "employees_eu.csv"), tmp_path / "_staging"
    )
    assert extract.row_count == 2
    assert extract.region == "eu"

    frame = pd.read_parquet(extract.parquet_path)
    assert {"_source_region", "_source_file", "_ingested_at"} <= set(frame.columns)
    assert (frame["_source_region"] == "eu").all()


def test_extract_rejects_missing_required_column(tmp_path):
    bad = tmp_path / "employees_eu.csv"
    bad.write_text("employee_id,email\nE1,a@x.com\n", encoding="utf-8")
    with pytest.raises(IngestionError, match="missing required column"):
        extract_to_parquet(SourceFile.from_path(bad), tmp_path / "_staging")


def test_load_raw_employees_deduplicates_last_write_wins(tmp_path, duckdb_warehouse):
    raw = tmp_path / "raw"
    raw.mkdir()
    # E1 appears twice; the US extract is newer (updated_at) and must win.
    _write_csv(
        raw / "employees_eu.csv", ["E1,a@x.com,2020-01-01,2026-05-20 10:00:00,true,sales,50000"]
    )
    _write_csv(
        raw / "employees_us.csv",
        [
            "E1,a@x.com,2020-01-01,2026-05-21 10:00:00,true,sales,55000",
            "E2,b@x.com,2021-01-01,2026-05-20 11:00:00,true,hr,60000",
        ],
    )
    extracts = [extract_to_parquet(sf, tmp_path / "_staging") for sf in discover_source_files(raw)]
    result = load_raw_employees(duckdb_warehouse, "raw_hr", extracts)

    assert result.rows_loaded == 2
    assert result.rows_deduplicated == 1
    assert sorted(result.regions) == ["eu", "us"]

    winning_salary = duckdb_warehouse.fetch_scalar(
        "SELECT salary FROM raw_hr.employees WHERE employee_id = 'E1'"
    )
    assert str(winning_salary) == "55000"
