"""hr_pipeline — reusable orchestration library for the airflow-bigquery platform.

The package deliberately contains **no Airflow imports at the top level** so that
its core logic (configuration, warehouse access, ingestion, data quality) can be
unit-tested with plain ``pytest`` — no scheduler, no metadata database.

Airflow-specific code is quarantined in two submodules that are only imported
from within DAGs:

* :mod:`hr_pipeline.assets`    — Airflow Asset definitions (data-aware scheduling)
* :mod:`hr_pipeline.operators` — custom operators
"""

from __future__ import annotations

__version__ = "1.0.0"
__author__ = "Yann Levavasseur"
