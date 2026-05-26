"""Ingestion — extract raw HR source extracts and land them in the warehouse.

The flow mirrors a real "extract → land → load" pattern:

1. :func:`discover_source_files` finds the per-region CSV extracts.
2. :func:`extract_to_parquet` validates one file and lands it as Parquet on the
   local filesystem. These steps run **in parallel** (dynamic task mapping) —
   landing to files, not the database, keeps them clear of DuckDB's writer lock.
3. :func:`load_raw_employees` consolidates every Parquet into a single
   ``raw_hr.employees`` table — the source the dbt project reads from.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from hr_pipeline.warehouse import Warehouse

logger = logging.getLogger(__name__)

RAW_EMPLOYEES_TABLE = "employees"

#: Columns the dbt source ``raw_hr.employees`` contracts on. Audit columns
#: (``_source_region`` …) are added on top and ignored by the staging model.
REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {"employee_id", "email", "hire_date", "updated_at", "is_active", "department", "salary"}
)


class IngestionError(RuntimeError):
    """Raised when a source file fails structural validation."""


@dataclass(frozen=True, slots=True)
class SourceFile:
    """A discovered raw extract and the region it belongs to."""

    path: Path
    region: str

    @classmethod
    def from_path(cls, path: Path) -> SourceFile:
        """Derive the region from ``employees_<region>.csv``."""
        stem = path.stem
        region = stem.split("_", 1)[1] if "_" in stem else "unknown"
        return cls(path=path, region=region)


@dataclass(frozen=True, slots=True)
class RegionExtract:
    """Result of landing one region file as Parquet. XCom-friendly via dicts."""

    region: str
    parquet_path: str
    row_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "region": self.region,
            "parquet_path": self.parquet_path,
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RegionExtract:
        return cls(
            region=str(payload["region"]),
            parquet_path=str(payload["parquet_path"]),
            row_count=int(payload["row_count"]),  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Outcome of the consolidation step."""

    table: str
    rows_loaded: int
    rows_deduplicated: int
    regions: list[str]

    def as_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "rows_loaded": self.rows_loaded,
            "rows_deduplicated": self.rows_deduplicated,
            "regions": self.regions,
        }


def discover_source_files(raw_dir: Path) -> list[SourceFile]:
    """Return every ``employees_*.csv`` extract in ``raw_dir``, sorted."""
    files = sorted(Path(raw_dir).glob("employees_*.csv"))
    if not files:
        raise IngestionError(f"No source files matching 'employees_*.csv' found in {raw_dir}.")
    logger.info("Discovered %s source file(s): %s", len(files), [f.name for f in files])
    return [SourceFile.from_path(path) for path in files]


def _validate(frame: pd.DataFrame, source_file: SourceFile) -> None:
    """Structural data-contract check before a file is allowed into the lake."""
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise IngestionError(
            f"{source_file.path.name} is missing required column(s): {sorted(missing)}"
        )
    if frame.empty:
        raise IngestionError(f"{source_file.path.name} contains no rows.")
    null_ids = int(frame["employee_id"].isna().sum())
    if null_ids:
        raise IngestionError(
            f"{source_file.path.name} has {null_ids} row(s) with a null employee_id."
        )


def extract_to_parquet(source_file: SourceFile, staging_dir: Path) -> RegionExtract:
    """Read, validate and land one region CSV as Parquet.

    The raw layer is kept as strings on purpose: casting is the responsibility
    of the dbt staging models, never of ingestion.
    """
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    frame = pd.read_csv(source_file.path, dtype=str)
    _validate(frame, source_file)

    # Audit lineage columns — answer "where did this row come from, and when?".
    frame["_source_region"] = source_file.region
    frame["_source_file"] = source_file.path.name
    frame["_ingested_at"] = pd.Timestamp.now(tz="UTC")

    destination = staging_dir / f"{source_file.path.stem}.parquet"
    frame.to_parquet(destination, index=False)
    logger.info("Extracted %s rows from %s → %s", len(frame), source_file.path.name, destination)
    return RegionExtract(
        region=source_file.region, parquet_path=str(destination), row_count=len(frame)
    )


def load_raw_employees(
    warehouse: Warehouse, raw_schema: str, extracts: list[RegionExtract]
) -> IngestionResult:
    """Consolidate every landed Parquet into ``<raw_schema>.employees``.

    De-duplicates on the natural key with last-write-wins semantics, so an
    employee present in two regional extracts keeps their most recent record.
    """
    if not extracts:
        raise IngestionError("load_raw_employees called with no extracts.")

    # Note: all columns are dtype=str (ingestion is cast-free by design).
    # Parquet files are read into memory all at once — acceptable for the HR
    # dataset scale (<1 M rows).  For larger volumes, prefer DuckDB's native
    # read_parquet() or a streaming merge approach.
    combined = pd.concat(
        [pd.read_parquet(extract.parquet_path) for extract in extracts], ignore_index=True
    )
    rows_before = len(combined)
    # Parse updated_at to datetime before sorting so that deduplication uses
    # correct chronological order regardless of the string format a regional
    # HRIS may use (e.g. "2026-5-1 9:00:00" vs "2026-05-01 09:00:00").
    combined["updated_at"] = pd.to_datetime(combined["updated_at"])
    combined = (
        combined.sort_values("updated_at")
        .drop_duplicates(subset="employee_id", keep="last")
        .reset_index(drop=True)
    )
    rows_loaded = warehouse.load_dataframe(
        raw_schema, RAW_EMPLOYEES_TABLE, combined, mode="replace"
    )
    rows_deduplicated = rows_before - rows_loaded
    logger.info(
        "Loaded %s rows into %s.%s (%s duplicate row(s) collapsed)",
        rows_loaded,
        raw_schema,
        RAW_EMPLOYEES_TABLE,
        rows_deduplicated,
    )
    return IngestionResult(
        table=f"{raw_schema}.{RAW_EMPLOYEES_TABLE}",
        rows_loaded=rows_loaded,
        rows_deduplicated=rows_deduplicated,
        regions=sorted({extract.region for extract in extracts}),
    )
