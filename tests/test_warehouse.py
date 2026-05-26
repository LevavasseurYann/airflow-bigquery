"""Unit tests for hr_pipeline.warehouse (DuckDB backend)."""

from __future__ import annotations

from hr_pipeline.warehouse import DuckDBWarehouse, get_warehouse


def test_load_and_query_roundtrip(duckdb_warehouse, sample_employees):
    rows = duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees)
    assert rows == len(sample_employees)
    assert duckdb_warehouse.table_exists("raw_hr", "employees")
    assert duckdb_warehouse.row_count("raw_hr", "employees") == len(sample_employees)


def test_fetch_scalar(duckdb_warehouse, sample_employees):
    duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees)
    count = duckdb_warehouse.fetch_scalar("SELECT count(*) FROM raw_hr.employees")
    assert count == len(sample_employees)


def test_fetch_all(duckdb_warehouse, sample_employees):
    duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees)
    rows = duckdb_warehouse.fetch_all(
        "SELECT employee_id FROM raw_hr.employees ORDER BY employee_id"
    )
    assert [row[0] for row in rows] == ["E1", "E2", "E3", "E4"]


def test_replace_mode_overwrites(duckdb_warehouse, sample_employees):
    duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees)
    duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees.head(2))
    assert duckdb_warehouse.row_count("raw_hr", "employees") == 2


def test_append_mode_adds_rows(duckdb_warehouse, sample_employees):
    duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees, mode="replace")
    duckdb_warehouse.load_dataframe("raw_hr", "employees", sample_employees, mode="append")
    assert duckdb_warehouse.row_count("raw_hr", "employees") == 2 * len(sample_employees)


def test_table_exists_is_false_for_missing_table(duckdb_warehouse):
    assert duckdb_warehouse.table_exists("nope", "missing") is False


def test_get_warehouse_returns_duckdb_for_local(local_settings):
    with get_warehouse(local_settings) as warehouse:
        assert isinstance(warehouse, DuckDBWarehouse)
