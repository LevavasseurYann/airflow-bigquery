"""Shared building blocks for every DAG in this project.

This module is **not** a DAG file: the DAG processor parses it, finds no DAG
object, and moves on (it is listed in ``.airflowignore`` to keep the logs
clean). DAG files import it as a plain module — Airflow puts the ``dags/``
folder on ``sys.path``.

Centralising the defaults here means ``owner``, retry policy and the failure
hook are defined exactly once.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum

#: Owner stamped on every DAG. The cluster policy in
#: ``config/airflow_local_settings.py`` rejects any DAG left on the default.
PROJECT_OWNER = "data-platform"

#: Shared start date. Every DAG runs with ``catchup=False`` — no backfilling.
DEFAULT_START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")

_alert_log = logging.getLogger("hr_pipeline.alerts")


def on_failure_callback(context: dict[str, Any]) -> None:
    """Emit one structured, greppable line whenever a task fails.

    This is the single integration point for real alerting: wire Slack,
    PagerDuty or Opsgenie here and nothing else in the project has to change.
    """
    task_instance = context.get("task_instance")
    dag = context.get("dag")
    _alert_log.error(
        "TASK FAILURE | dag=%s | task=%s | run=%s | try=%s",
        getattr(dag, "dag_id", "unknown"),
        getattr(task_instance, "task_id", "unknown"),
        context.get("run_id", "unknown"),
        getattr(task_instance, "try_number", "?"),
    )


#: default_args merged into every task of every DAG.
#: Retries use exponential backoff: a transient blip is retried fast, a
#: persistent failure backs off so it does not hammer the warehouse.
DEFAULT_ARGS: dict[str, Any] = {
    "owner": PROJECT_OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "on_failure_callback": on_failure_callback,
}
