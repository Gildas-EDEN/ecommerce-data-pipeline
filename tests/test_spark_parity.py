"""
Test bonus : Spark et pandas doivent produire exactement les mêmes agrégats.

Nécessite PySpark et Java : ignoré sur un PC sans PySpark, exécuté dans Docker :
    docker compose exec -w /opt/project airflow python -m pytest tests/test_spark_parity.py -v
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("pyspark", reason="PySpark non installé : lancer ce test dans le conteneur Docker")
if shutil.which("java") is None:
    pytest.skip("Java introuvable : Spark ne peut pas démarrer", allow_module_level=True)
pytestmark = pytest.mark.spark

from checkpoint import CheckpointManager  # noqa: E402
from spark_jobs.order_metrics import compute_order_metrics_with_spark  # noqa: E402
from transformers import build_daily_order_facts, build_product_performance, flatten_orders  # noqa: E402

ORDERS_FILE = Path(__file__).resolve().parents[1] / "data" / "api" / "orders.json"


@pytest.fixture(scope="module", autouse=True)
def stop_spark_at_the_end():
    """Arrête proprement la session Spark (la JVM) à la fin des tests."""
    # py4j (le pont Python <-> Java) écrit un message d'information à la fermeture,
    # après que pytest a fermé ses flux : on ne garde que ses avertissements.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    yield
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()


def normalize(df: pd.DataFrame, keys: list) -> pd.DataFrame:
    out = df.copy()
    out["order_date"] = out["order_date"].astype(str)
    for col in out.columns:
        if col not in keys and col != "order_date" and pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].astype(float)
    return out.sort_values(keys).reset_index(drop=True)


def test_spark_and_pandas_produce_the_same_facts(tmp_path) -> None:
    orders, items = flatten_orders(json.loads(ORDERS_FILE.read_text(encoding="utf-8"))["orders"])
    checkpoints = CheckpointManager(tmp_path, "parity")
    checkpoints.save_frame("validated_orders", orders)
    checkpoints.save_frame("validated_order_items", items)

    compute_order_metrics_with_spark(checkpoints)

    spark_daily = normalize(checkpoints.load_frame("fact_daily_orders"), ["order_date", "customer_id"])
    pandas_daily = normalize(build_daily_order_facts(orders), ["order_date", "customer_id"])
    pd.testing.assert_frame_equal(spark_daily, pandas_daily, check_dtype=False, atol=0.011)

    spark_perf = normalize(checkpoints.load_frame("fact_product_performance"), ["order_date", "product_id"])
    pandas_perf = normalize(build_product_performance(orders, items), ["order_date", "product_id"])
    pd.testing.assert_frame_equal(spark_perf, pandas_perf, check_dtype=False, atol=0.011)
