"""Warehouse abstraction — one interface, two backends.

The pipeline talks to a :class:`Warehouse`, never to DuckDB or BigQuery
directly. :func:`get_warehouse` picks the concrete implementation from
:class:`~hr_pipeline.config.Settings`, so swapping local ↔ production is a pure
configuration change.

Every implementation is a context manager — always use ``with``::

    with get_warehouse(settings) as wh:
        wh.load_dataframe("raw_hr", "employees", df)
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal, Protocol

import pandas as pd

LoadMode = Literal["replace", "append"]

# Identifiers are interpolated into SQL with double-quote quoting. Double-quoting
# is sufficient for valid identifiers, but exploitable if the name itself contains
# a double quote. Restricting to letters, digits and underscores eliminates the
# risk entirely.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_id(name: str) -> str:
    """Return *name* unchanged, or raise if it is not a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Unsafe SQL identifier {name!r}: only letters, digits and underscores are allowed."
        )
    return name


class WarehouseSettings(Protocol):
    """Structural type for the subset of Settings consumed by :func:`get_warehouse`.

    Using a Protocol avoids a circular import between ``warehouse`` and
    ``config`` while keeping the call-site statically typed.
    """

    is_production: bool
    gcp_project: str | None
    bq_location: str
    duckdb_path: Path


class Warehouse(ABC):
    """Minimal warehouse contract shared by every backend."""

    @abstractmethod
    def ensure_namespace(self, schema: str) -> None:
        """Create the schema / BigQuery dataset if it does not exist."""

    @abstractmethod
    def load_dataframe(
        self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace"
    ) -> int:
        """Load a DataFrame into ``schema.table`` and return the row count."""

    @abstractmethod
    def fetch_scalar(self, sql: str) -> Any:
        """Run ``sql`` and return the first column of the first row."""

    @abstractmethod
    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        """Run ``sql`` and return every row as a tuple."""

    @abstractmethod
    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether ``schema.table`` exists."""

    @abstractmethod
    def close(self) -> None:
        """Release the underlying connection / client."""

    def row_count(self, schema: str, table: str) -> int:
        """Convenience helper — number of rows in ``schema.table``."""
        return int(
            self.fetch_scalar(f'SELECT count(*) FROM "{_safe_id(schema)}"."{_safe_id(table)}"')
        )

    def __enter__(self) -> Warehouse:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class DuckDBWarehouse(Warehouse):
    """Local, file-based warehouse — the default for ``HR_ENV=local``.

    DuckDB is an embedded OLAP engine: a single file, no server, no
    credentials. Its concurrency model drives two project conventions:

    * **one writer at a time** — write tasks are serialised (the ``dbt`` pool);
    * **many concurrent readers** — read-only tasks (the data quality checks)
      open the connection with ``read_only=True`` so they can run in parallel.
    """

    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        import duckdb

        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._conn = duckdb.connect(str(path), read_only=read_only)

    def ensure_namespace(self, schema: str) -> None:
        self._conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{_safe_id(schema)}"')

    def load_dataframe(
        self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace"
    ) -> int:
        self.ensure_namespace(schema)
        qualified = f'"{_safe_id(schema)}"."{_safe_id(table)}"'
        # register() exposes the DataFrame to SQL without copying it.
        self._conn.register("_hr_frame", frame)
        try:
            if mode == "replace":
                self._conn.execute(
                    f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _hr_frame"
                )
            else:
                self._conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _hr_frame WHERE 1=0"
                )
                self._conn.execute(f"INSERT INTO {qualified} SELECT * FROM _hr_frame")
        finally:
            self._conn.unregister("_hr_frame")
        return len(frame)

    def fetch_scalar(self, sql: str) -> Any:
        row = self._conn.execute(sql).fetchone()
        return None if row is None else row[0]

    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        return self._conn.execute(sql).fetchall()

    def table_exists(self, schema: str, table: str) -> bool:
        result = self._conn.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table],
        ).fetchone()
        return bool(result and result[0])

    def close(self) -> None:
        self._conn.close()


class BigQueryWarehouse(Warehouse):
    """Production warehouse — the backend for ``HR_ENV=production``.

    ``google-cloud-bigquery`` is imported lazily so that a local checkout never
    needs the dependency installed, nor any GCP credentials, to run.
    """

    def __init__(self, project: str, location: str) -> None:
        from google.cloud import bigquery

        self._bigquery = bigquery
        self._project = project
        self._client = bigquery.Client(project=project, location=location)

    def ensure_namespace(self, schema: str) -> None:
        _safe_id(schema)
        dataset = self._bigquery.Dataset(f"{self._project}.{schema}")
        dataset.location = self._client.location
        self._client.create_dataset(dataset, exists_ok=True)

    def load_dataframe(
        self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace"
    ) -> int:
        self.ensure_namespace(schema)  # validates schema
        _safe_id(table)
        table_id = f"{self._project}.{schema}.{table}"
        disposition = "WRITE_TRUNCATE" if mode == "replace" else "WRITE_APPEND"
        job_config = self._bigquery.LoadJobConfig(write_disposition=disposition)
        self._client.load_table_from_dataframe(frame, table_id, job_config=job_config).result()
        return len(frame)

    def fetch_scalar(self, sql: str) -> Any:
        for row in self._client.query(sql).result():
            return row[0]
        return None

    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        return [tuple(row.values()) for row in self._client.query(sql).result()]

    def table_exists(self, schema: str, table: str) -> bool:
        _safe_id(schema)
        _safe_id(table)
        from google.cloud.exceptions import NotFound

        try:
            self._client.get_table(f"{self._project}.{schema}.{table}")
            return True
        except NotFound:
            return False

    def close(self) -> None:
        self._client.close()


def get_warehouse(settings: WarehouseSettings, *, read_only: bool = False) -> Warehouse:
    """Return the warehouse implementation matching ``settings``.

    Args:
        settings: any object that satisfies :class:`WarehouseSettings` — in
            practice always a :class:`~hr_pipeline.config.Settings` instance.
        read_only: open a read-only connection. Honoured by DuckDB so that
            parallel query-only tasks do not contend for the writer lock; a
            no-op for BigQuery, which has no such constraint.
    """
    if settings.is_production:
        return BigQueryWarehouse(project=settings.gcp_project, location=settings.bq_location)
    return DuckDBWarehouse(path=settings.duckdb_path, read_only=read_only)
