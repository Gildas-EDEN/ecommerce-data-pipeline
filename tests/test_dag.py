"""
Test Case 8 : validation du DAG Airflow.
  Entrée  : la définition du DAG et ses dépendances
  Attendu : le DAG est chargé sans erreur et les tâches s'enchaînent dans le bon ordre

Ce test nécessite Apache Airflow : il est ignoré (« skipped ») sur un PC Windows
sans Airflow et s'exécute dans le conteneur Docker :
    docker compose exec -w /opt/project airflow python -m pytest tests/test_dag.py -v
"""

from __future__ import annotations

import inspect
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Set

import pytest

pytest.importorskip("airflow", reason="Airflow non installé : lancer ce test dans le conteneur Docker")
pytestmark = pytest.mark.airflow

DAG_FOLDER = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "automated_data_pipeline"

EXPECTED_UPSTREAM: Dict[str, Set[str]] = {
    "check_data_sources": set(),
    "wait_for_product_file": set(),
    "extract_customers": {"check_data_sources"},
    "extract_orders": {"check_data_sources"},
    "extract_products": {"wait_for_product_file"},
    "dbt_snapshot": {"check_data_sources"},
    "validate_data": {"extract_customers", "extract_orders", "extract_products"},
    "transform_customers_scd2": {"validate_data"},
    "compute_order_metrics_spark": {"validate_data"},
    "load_warehouse": {"transform_customers_scd2", "compute_order_metrics_spark"},
    "dbt_run": {"load_warehouse", "dbt_snapshot"},
    "dbt_test": {"dbt_run"},
    "generate_quality_report": {"dbt_test"},
    "cleanup_old_checkpoints": {"generate_quality_report"},
}


@pytest.fixture(scope="module")
def dag():
    try:  # Airflow 3.x
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # Airflow 2.x
        from airflow.models import DagBag
    # L'option include_examples n'existe plus dans Airflow 3 (exemples désactivés par la config).
    options = {"include_examples": False} if "include_examples" in inspect.signature(DagBag).parameters else {}
    dagbag = DagBag(dag_folder=str(DAG_FOLDER), **options)
    assert dagbag.import_errors == {}, f"Erreurs d'import du DAG : {dagbag.import_errors}"
    assert DAG_ID in dagbag.dags
    return dagbag.dags[DAG_ID]


def topological_order(upstream: Dict[str, Set[str]]) -> List[str]:
    """Ordre d'exécution possible (algorithme de Kahn) : lève une erreur en cas de cycle."""
    remaining = {task: set(parents) for task, parents in upstream.items()}
    order: List[str] = []
    while remaining:
        ready = sorted(task for task, parents in remaining.items() if not parents)
        if not ready:
            raise AssertionError(f"Cycle détecté entre : {sorted(remaining)}")
        for task in ready:
            order.append(task)
            del remaining[task]
        for parents in remaining.values():
            parents.difference_update(ready)
    return order


def test_tc8_dag_parses_with_expected_tasks(dag) -> None:
    assert set(dag.task_ids) == set(EXPECTED_UPSTREAM)


def test_tc8_task_dependencies(dag) -> None:
    for task_id, expected in EXPECTED_UPSTREAM.items():
        assert set(dag.get_task(task_id).upstream_task_ids) == expected, task_id


def test_tc8_task_sequence_is_acyclic_and_ordered(dag) -> None:
    upstream = {task.task_id: set(task.upstream_task_ids) for task in dag.tasks}
    order = topological_order(upstream)
    position = {task: index for index, task in enumerate(order)}
    sequence = ["check_data_sources", "extract_customers", "validate_data", "transform_customers_scd2",
                "load_warehouse", "dbt_run", "dbt_test", "generate_quality_report", "cleanup_old_checkpoints"]
    assert [position[t] for t in sequence] == sorted(position[t] for t in sequence)


def test_tc8_scheduling_and_resilience_settings(dag) -> None:
    # Airflow 3 : dag.schedule ; Airflow 2 : dag.schedule_interval
    schedule = getattr(dag, "schedule", None)
    if not isinstance(schedule, str):
        schedule = getattr(dag, "schedule_interval", None)
    assert schedule == "0 2 * * *"                    # tous les jours à 2 h
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(hours=2)
    extract = dag.get_task("extract_orders")
    assert extract.retries == 2
    assert extract.retry_exponential_backoff
    assert extract.on_failure_callback  # alerte en cas d'échec
    assert dag.get_task("validate_data").retries == 0  # porte qualité : pas de relance
