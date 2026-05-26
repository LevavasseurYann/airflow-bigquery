"""Custom Airflow operators.

Importing this subpackage requires Airflow to be installed — it is only ever
imported from within DAG files, never from the pure-Python core modules.
"""

from hr_pipeline.operators.data_quality import DataQualityCheckOperator

__all__ = ["DataQualityCheckOperator"]
