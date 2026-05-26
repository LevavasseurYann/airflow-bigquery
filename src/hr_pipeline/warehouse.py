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

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal

import pandas as pd

LoadMode = Literal["replace", "append"]


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
        return int(self.fetch_scalar(f'SELECT count(*) FROM "{schema}"."{table}"'))

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
        self._conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    def load_dataframe(
        self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace"
    ) -> int:
        self.ensure_namespace(schema)
        qualified = f'"{schema}"."{table}"'
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
        dataset = self._bigquery.Dataset(f"{self._project}.{schema}")
        dataset.location = self._client.location
        self._client.create_dataset(dataset, exists_ok=True)

    def load_dataframe(
        self, schema: str, table: str, frame: pd.DataFrame, *, mode: LoadMode = "replace"
    ) -> int:
        self.ensure_namespace(schema)
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
        from google.cloud.exceptions import NotFound

        try:
            self._client.get_table(f"{self._project}.{schema}.{table}")
        except NotFound:
            return False
        return True

    def close(self) -> None:
        self._client.close()


def get_warehouse(settings: Any, *, read_only: bool = False) -> Warehouse:
    """Return the warehouse implementation matching ``settings``.

    Args:
        settings: a :class:`~hr_pipeline.config.Settings` instance (typed as
            :class:`~typing.Any` to keep this module import-light).
        read_only: open a read-only connection. Honoured by DuckDB so that
            parallel query-only tasks do not contend for the writer lock; a
            no-op for BigQuery, which has no such constraint.
    """
    if settings.is_production:
        return BigQueryWarehouse(project=settings.gcp_project, location=settings.bq_location)
    return DuckDBWarehouse(path=settings.duckdb_path, read_only=read_only)
