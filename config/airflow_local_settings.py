"""Airflow cluster policies — org-wide guardrails enforced at parse time.

Airflow auto-discovers ``airflow_local_settings.py`` on the ``$AIRFLOW_HOME/config``
path and calls the policy functions by name. Cluster policies are how a
platform team enforces standards centrally, without trusting every DAG author
to remember them.

Two policies are defined here:

* :func:`dag_policy`  — **rejects** DAGs that are not tagged, described and owned;
* :func:`task_policy` — **mutates** every task to apply a safe execution timeout.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowClusterPolicyViolation

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG

log = logging.getLogger("airflow.policies")

# Owner value Airflow uses when a DAG author forgets to set one.
_PLACEHOLDER_OWNER = "airflow"

# No task in this project may run longer than this without opting in explicitly.
_DEFAULT_TASK_TIMEOUT = timedelta(hours=2)


def dag_policy(dag: DAG) -> None:
    """Reject any DAG that is not minimally documented, tagged and owned.

    Runs for every DAG the processor parses. A non-compliant DAG surfaces as an
    import error in the UI instead of silently shipping.
    """
    violations: list[str] = []

    if not dag.tags:
        violations.append("must declare at least one tag")

    if not getattr(dag, "description", None):
        violations.append("must have a non-empty description")

    owner = (getattr(dag, "owner", "") or "").strip()
    if not owner or owner == _PLACEHOLDER_OWNER:
        violations.append(f"must set an explicit owner (not '{_PLACEHOLDER_OWNER}')")

    if violations:
        raise AirflowClusterPolicyViolation(
            f"DAG '{dag.dag_id}' violates project policy: " + "; ".join(violations)
        )


def task_policy(task: BaseOperator) -> None:
    """Apply a safe default execution timeout to every task.

    Mutating and non-blocking: a task that never sets ``execution_timeout`` can
    otherwise hang a worker slot indefinitely. This guarantees a ceiling.
    """
    if task.execution_timeout is None:
        task.execution_timeout = _DEFAULT_TASK_TIMEOUT
        log.debug(
            "task_policy: applied default execution_timeout to %s.%s",
            task.dag_id,
            task.task_id,
        )
