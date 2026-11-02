"""
### Pipeline de données automatisé (Spark, dbt, Airflow)

Un **DAG** (Directed Acyclic Graph, graphe orienté sans cycle) décrit des tâches
et l'ordre dans lequel elles s'exécutent. Airflow lance celui-ci chaque jour à
2 h (UTC), relance automatiquement les tâches en échec et garde l'historique
de chaque exécution (interface : http://localhost:8080).

**Ordre des tâches**
1. `check_data_sources` (base + API joignables ?) et, en parallèle, `wait_for_product_file`
2. `extract_customers`, `extract_orders`, `extract_products` en parallèle, plus `dbt_snapshot`
3. `validate_data` : **porte qualité**, tout s'arrête si les données sont mauvaises
4. `transform_customers_scd2` (pandas) et `compute_order_metrics_spark` (Spark) en parallèle
5. `load_warehouse` : écriture atomique dans PostgreSQL (COMMIT ou ROLLBACK)
6. `dbt_run` puis `dbt_test` : modèles SQL d'analyse et tests de cohérence
7. `generate_quality_report` puis `cleanup_old_checkpoints`

**Bonnes pratiques appliquées**
- le fichier reste léger : les imports lourds (pandas, pyspark) sont faits DANS les
  fonctions, car Airflow relit ce fichier régulièrement ;
- les tâches s'échangent des fichiers (checkpoints) et non de gros volumes par XCom ;
- chaque tâche est rejouable sans effet de bord (idempotence) : « Clear » sur une tâche
  en échec la relance sans refaire les étapes précédentes.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta
from typing import Any, Dict

import pendulum

try:  # Airflow 3.x (version utilisée dans Docker)
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.sensors.filesystem import FileSensor
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x (version 2.8.1 utilisée dans le cours)
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    from airflow.sensors.filesystem import FileSensor

log = logging.getLogger(__name__)

DAG_ID = "automated_data_pipeline"

# Chemins et réglages lus dans l'environnement (définis dans docker-compose.yml).
PROJECT_DIR = os.getenv("PIPELINE_PROJECT_DIR", "/opt/project")
INPUT_DIR = os.getenv("INPUT_PATH", f"{PROJECT_DIR}/data/input")
PRODUCTS_FILE = os.path.join(INPUT_DIR, os.getenv("PRODUCTS_FILE_NAME", "products.csv"))
DBT_BIN = os.getenv("DBT_BIN", "dbt")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", f"{PROJECT_DIR}/dbt_project")
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT_BIN", "spark-submit")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "").strip()


# =============================================================================
# Fonctions exécutées par les tâches
# =============================================================================
def _ensure_project_on_path() -> None:
    """Rend les modules du projet (pipeline.py, utils.py...) importables depuis une tâche."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)


def run_pipeline_stage(stage: str, **context: Any) -> Dict[str, Any]:
    """Exécute une étape du pipeline.

    Le run_id d'Airflow (unique par exécution du DAG) relie les tâches entre elles :
    toutes écrivent et relisent les checkpoints du même dossier data/checkpoints/<run_id>/.
    La valeur renvoyée (petit dictionnaire de métadonnées) est stockée en XCom.
    """
    _ensure_project_on_path()
    from pipeline import run_stage  # import paresseux : le DAG reste rapide à analyser

    return run_stage(stage, run_id=context["run_id"])


def check_sources(**_context: Any) -> Dict[str, Any]:
    _ensure_project_on_path()
    from pipeline import check_data_sources

    return check_data_sources()


def generate_quality_report(**context: Any) -> Dict[str, Any]:
    """Rapport final. Montre aussi l'usage des XCom : on relit ce qu'ont renvoyé les extractions."""
    _ensure_project_on_path()
    from pipeline import run_stage

    ti = context["ti"]
    for task_id in ("extract_customers", "extract_orders", "extract_products"):
        meta = ti.xcom_pull(task_ids=task_id) or {}
        log.info("XCom %-18s : %s ligne(s), mode %s", task_id, meta.get("rows"), meta.get("mode"))
    return run_stage("report", run_id=context["run_id"])


def cleanup_checkpoints(**_context: Any) -> Dict[str, Any]:
    _ensure_project_on_path()
    from pipeline import cleanup_old_checkpoints

    return cleanup_old_checkpoints()


def notify_failure(context: Dict[str, Any]) -> None:
    """Appelée par Airflow quand une tâche échoue définitivement (après ses relances)."""
    _ensure_project_on_path()
    from utils import send_alert

    ti = context.get("task_instance") or context.get("ti")
    send_alert("error", f"Échec de la tâche {getattr(ti, 'task_id', '?')} (DAG {DAG_ID})", {
        "run_id": context.get("run_id"),
        "task_id": getattr(ti, "task_id", None),
        "try_number": getattr(ti, "try_number", None),
        "exception": str(context.get("exception")),
    })


def notify_success(context: Dict[str, Any]) -> None:
    """Appelée par Airflow quand toute l'exécution du DAG a réussi."""
    _ensure_project_on_path()
    from utils import send_alert

    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    send_alert("info", f"DAG {DAG_ID} terminé avec succès", {"run_id": run_id})


# =============================================================================
# Définition du DAG
# =============================================================================
default_args: Dict[str, Any] = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": int(os.getenv("PIPELINE_RETRIES", "2")),
    "retry_delay": timedelta(minutes=float(os.getenv("PIPELINE_RETRY_DELAY_MINUTES", "5"))),
    "retry_exponential_backoff": True,          # attente doublée à chaque relance...
    "max_retry_delay": timedelta(minutes=30),   # ...dans la limite de 30 minutes
    "execution_timeout": timedelta(minutes=30), # une tâche bloquée est arrêtée au bout de 30 min
    "on_failure_callback": notify_failure,
}
if ALERT_EMAIL:  # e-mails seulement si une adresse est configurée (nécessite un serveur SMTP)
    default_args.update({"email": [ALERT_EMAIL], "email_on_failure": True, "email_on_retry": False})

with DAG(
    dag_id=DAG_ID,
    description="Pipeline quotidien : extraction, qualité, SCD2, Spark, dbt",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=os.getenv("PIPELINE_SCHEDULE", "0 2 * * *"),  # tous les jours à 2 h (format cron)
    catchup=False,                      # ne pas rattraper toutes les journées depuis 2024
    max_active_runs=1,                  # jamais deux exécutions simultanées (watermarks, SCD2)
    dagrun_timeout=timedelta(hours=2),  # fenêtre d'exécution de 2 h du cahier des charges
    on_success_callback=notify_success,
    tags=["data-engineering", "etl", "scd2", "spark", "dbt"],
    doc_md=__doc__,
) as dag:

    check_data_sources = PythonOperator(
        task_id="check_data_sources",
        python_callable=check_sources,
        doc_md="Vérifie que PostgreSQL et l'API répondent avant de lancer les extractions.",
    )

    # Un « sensor » attend une condition : ici, la présence du fichier produits.
    # mode="reschedule" : entre deux vérifications, la tâche libère sa place d'exécution.
    wait_for_product_file = FileSensor(
        task_id="wait_for_product_file",
        filepath=PRODUCTS_FILE,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    extract_customers = PythonOperator(
        task_id="extract_customers", python_callable=run_pipeline_stage, op_kwargs={"stage": "extract_customers"},
    )
    extract_orders = PythonOperator(
        task_id="extract_orders", python_callable=run_pipeline_stage, op_kwargs={"stage": "extract_orders"},
    )
    extract_products = PythonOperator(
        task_id="extract_products", python_callable=run_pipeline_stage, op_kwargs={"stage": "extract_products"},
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=run_pipeline_stage,
        op_kwargs={"stage": "validate"},
        retries=0,  # des données invalides ne se corrigent pas toutes seules : inutile de relancer
        doc_md="Porte qualité : si une table est en FAIL, les tâches suivantes ne s'exécutent pas.",
    )

    transform_customers_scd2 = PythonOperator(
        task_id="transform_customers_scd2",
        python_callable=run_pipeline_stage,
        op_kwargs={"stage": "transform_customers"},
    )

    # Spark est lancé comme en production, avec spark-submit.
    # {{ run_id }} est un modèle Jinja remplacé par Airflow au moment de l'exécution.
    compute_order_metrics_spark = BashOperator(
        task_id="compute_order_metrics_spark",
        bash_command=(
            SPARK_SUBMIT
            + " --master 'local[*]' --driver-memory 1g --conf spark.ui.enabled=false "
            + PROJECT_DIR + "/spark_jobs/order_metrics.py --run-id '{{ run_id }}'"
        ),
    )

    load_warehouse = PythonOperator(
        task_id="load_warehouse", python_callable=run_pipeline_stage, op_kwargs={"stage": "load"},
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} snapshot --profiles-dir {DBT_PROJECT_DIR}",
        doc_md="SCD2 « natif » de dbt sur la table source customers (à comparer avec notre SCD2 Python).",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --profiles-dir {DBT_PROJECT_DIR}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test --profiles-dir {DBT_PROJECT_DIR}",
    )

    generate_quality_report_task = PythonOperator(
        task_id="generate_quality_report", python_callable=generate_quality_report,
    )
    cleanup_old_checkpoints = PythonOperator(
        task_id="cleanup_old_checkpoints", python_callable=cleanup_checkpoints,
    )

    # --- Dépendances (« >> » signifie « puis ») ---------------------------------
    check_data_sources >> [extract_customers, extract_orders, dbt_snapshot]
    wait_for_product_file >> extract_products
    [extract_customers, extract_orders, extract_products] >> validate_data
    validate_data >> [transform_customers_scd2, compute_order_metrics_spark] >> load_warehouse
    [load_warehouse, dbt_snapshot] >> dbt_run >> dbt_test >> generate_quality_report_task >> cleanup_old_checkpoints
