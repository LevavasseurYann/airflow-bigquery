"""DAG integrity tests — the single most valuable Airflow test you can write.

It loads every DAG file through a ``DagBag`` and asserts they import cleanly and
respect project conventions. It catches the most common breakage — a typo, a
bad import, a Cosmos misconfiguration — before it ever reaches the scheduler.

Marked ``dags`` so it can be selected/skipped (``pytest -m dags``); it requires
Airflow and Cosmos, so it is skipped automatically when they are absent.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.dags

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DAGS_DIR = _REPO_ROOT / "dags"

EXPECTED_DAG_IDS = {
    "ingest_hr_sources",
    "transform_hr_dbt",
    "data_quality_hr",
    "platform_maintenance",
}


@pytest.fixture(scope="module")
def dagbag():
    """Build a DagBag from the dags/ folder, with the repo's real paths."""
    pytest.importorskip("airflow", reason="Airflow is required for DAG tests")
    pytest.importorskip("cosmos", reason="astronomer-cosmos is required for DAG tests")

    # Point the DAGs at the real in-repo assets so Cosmos can render the project.
    os.environ.setdefault("HR_ENV", "local")
    os.environ["DBT_PROJECT_DIR"] = str(_REPO_ROOT / "include" / "dbt")
    os.environ["DBT_PROFILES_DIR"] = str(_REPO_ROOT / "include" / "dbt")
    os.environ["HR_RAW_DATA_DIR"] = str(_REPO_ROOT / "include" / "data" / "raw")
    os.environ["DUCKDB_PATH"] = str(_REPO_ROOT / "include" / "data" / "ci_warehouse.duckdb")

    # The dags/ folder must be importable so `import common` resolves.
    if str(_DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(_DAGS_DIR))

    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(_DAGS_DIR), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_all_expected_dags_are_present(dagbag):
    assert EXPECTED_DAG_IDS.issubset(set(dagbag.dag_ids))


def test_every_dag_is_tagged_and_documented(dagbag):
    for dag_id in EXPECTED_DAG_IDS:
        dag = dagbag.get_dag(dag_id)
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.description, f"{dag_id} has no description"
        assert dag.doc_md, f"{dag_id} has no doc_md"


def test_no_dag_uses_the_default_owner(dagbag):
    for dag_id in EXPECTED_DAG_IDS:
        dag = dagbag.get_dag(dag_id)
        assert dag.owner and dag.owner != "airflow", f"{dag_id} has no explicit owner"


def test_data_dags_have_no_backfill(dagbag):
    for dag_id in EXPECTED_DAG_IDS:
        dag = dagbag.get_dag(dag_id)
        assert dag.catchup is False, f"{dag_id} should not back-fill"
