"""
spark_jobs/order_metrics.py : agrégats de commandes calculés avec Apache Spark.

Même résultat que build_daily_order_facts / build_product_performance (pandas,
dans transformers.py), mais avec Spark. Spark découpe les données en
« partitions » et les traite en parallèle ; sur un cluster, chaque machine
traite une partie des données. Ici on l'exécute en mode local[*] (tous les
cœurs de la machine), mais le même code tournerait tel quel sur un cluster :
seule l'option --master change.

Différence clé avec pandas : Spark est « paresseux » (lazy). Les groupBy/agg
ne font que construire un plan de calcul ; rien n'est calculé avant une
ACTION (ici l'écriture .write.parquet). Spark optimise alors le plan complet.

Lancement (c'est ce que fait la tâche Airflow compute_order_metrics_spark) :
    spark-submit spark_jobs/order_metrics.py --run-id <run_id>
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional, Tuple

# Permet `spark-submit spark_jobs/order_metrics.py` depuis n'importe quel dossier :
# on ajoute la racine du projet au chemin d'import de Python.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

logger = logging.getLogger(__name__)


def build_spark_session(app_name: str = "order_metrics") -> SparkSession:
    """Crée (ou réutilise) la session Spark."""
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))           # local[*] : tous les cœurs de la machine
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))  # petit volume
        .config("spark.sql.session.timeZone", "UTC")              # pas de décalage horaire sur les dates
        .config("spark.ui.enabled", "false")                      # pas d'interface web (inutile ici)
        .getOrCreate()
    )


def compute_daily_order_facts(orders: DataFrame) -> DataFrame:
    """Une ligne par (jour, client). bround = arrondi « bancaire », identique à pandas."""
    return (
        orders.groupBy("order_date", "customer_id")
        .agg(
            F.bround(F.sum("total_amount"), 2).alias("total_sales"),
            F.count("order_id").alias("order_count"),
            F.bround(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.countDistinct("order_id").alias("unique_orders"),
        )
        .orderBy("order_date", "customer_id")
    )


def compute_product_performance(orders: DataFrame, items: DataFrame) -> DataFrame:
    """Une ligne par (jour, produit) : unités vendues, chiffre d'affaires, nombre de commandes."""
    lines = items.join(orders.select("order_id", "order_date"), on="order_id", how="inner")
    return (
        lines.withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
        .groupBy("order_date", "product_id")
        .agg(
            F.sum("quantity").cast("long").alias("units_sold"),
            F.bround(F.sum("line_revenue"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("order_date", "product_id")
    )


def compute_order_metrics_with_spark(checkpoints: Any, spark: Optional[SparkSession] = None) -> Tuple[Path, Path]:
    """Lit les commandes validées (Parquet), calcule les agrégats et les écrit (Parquet)."""
    spark = spark or build_spark_session()
    orders = spark.read.parquet(str(checkpoints.frame_path("validated_orders")))
    items = spark.read.parquet(str(checkpoints.frame_path("validated_order_items")))

    daily_path = checkpoints.frame_path("fact_daily_orders")
    perf_path = checkpoints.frame_path("fact_product_performance")
    # coalesce(1) : un seul fichier en sortie, le volume agrégé est faible.
    # C'est l'écriture (une ACTION) qui déclenche réellement les calculs.
    compute_daily_order_facts(orders).coalesce(1).write.mode("overwrite").parquet(str(daily_path))
    compute_product_performance(orders, items).coalesce(1).write.mode("overwrite").parquet(str(perf_path))
    logger.info("Agrégats Spark écrits dans %s et %s", daily_path, perf_path)
    return daily_path, perf_path


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Calcul Spark des agrégats de commandes d'une exécution")
    parser.add_argument("--run-id", required=True, help="identifiant de l'exécution (run_id Airflow)")
    args = parser.parse_args(argv)

    from config import load_config
    from pipeline import DataPipeline
    from utils import setup_logging

    config = load_config()
    setup_logging(config.log_level)
    # On passe par l'étape du pipeline : le checkpoint « transform_orders » est
    # ainsi marqué terminé, exactement comme avec le moteur pandas.
    DataPipeline(config, run_id=args.run_id).run_stage("transform_orders", engine="spark")
    return 0


if __name__ == "__main__":
    sys.exit(main())
