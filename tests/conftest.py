"""
conftest.py : outils partagés par tous les tests (« fixtures » pytest).

Les tests n'ont besoin NI de PostgreSQL, NI de l'API, NI d'Airflow : les
connexions sont remplacées par des « doublures » (mocks et faux objets) qui
renvoient des données maîtrisées. On teste ainsi la LOGIQUE du pipeline,
rapidement et sans infrastructure.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from config import DatabaseConfig, PipelineConfig, load_rules
from extractors import DatabaseExtractor
from loaders import DIMENSION_COLUMNS, LoadBatch
from pipeline import DataPipeline

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "data" / "samples_cours"
TODAY = date(2026, 10, 6)


def read_course_csv(path: Path) -> pd.DataFrame:
    """Les CSV du cours ont chaque ligne entre guillemets : on les « déballe » avant lecture."""
    lines = [
        next(csv.reader([line]))[0]
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    return pd.read_csv(io.StringIO("\n".join(lines)), dtype=str)


# =============================================================================
# Données d'exemple du cours
# =============================================================================
@pytest.fixture
def sample_customers() -> pd.DataFrame:
    """Les 5 clients de « Sample customer data for testing.csv »."""
    df = read_course_csv(SAMPLES / "Sample customer data for testing.csv")
    df["customer_id"] = df["customer_id"].astype("Int64")
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    return df


@pytest.fixture
def sample_orders() -> List[Dict[str, Any]]:
    """Les 2 commandes de « Sample_orders.json »."""
    return json.loads((SAMPLES / "Sample_orders.json").read_text(encoding="utf-8"))["orders"]


@pytest.fixture
def sample_products() -> pd.DataFrame:
    """Les 5 produits de « Product catalog data.csv »."""
    df = read_course_csv(SAMPLES / "Product catalog data.csv")
    df["price"] = df["price"].astype(float)
    return df


@pytest.fixture
def rules() -> Dict[str, Any]:
    return load_rules()


@pytest.fixture
def tmp_config(tmp_path: Path, rules: Dict[str, Any]) -> PipelineConfig:
    """Configuration de test : tous les fichiers sont écrits dans un dossier temporaire."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "products.csv").write_text(
        "product_id,name,category,price,supplier\nP001,Hiking Boots,Footwear,129.99,OutdoorGear Inc\n",
        encoding="utf-8",
    )
    return PipelineConfig(
        database=DatabaseConfig("localhost", 5432, "analytics", "test_user", "test_password"),
        api_endpoint="http://api.test/orders",
        api_key="test-key",
        input_path=str(input_dir),
        output_path=str(tmp_path / "output"),
        state_path=str(tmp_path / "state"),
        checkpoint_path=str(tmp_path / "checkpoints"),
        log_file="",
        rules=rules,
    )


# =============================================================================
# Doublures des sources et de l'entrepôt
# =============================================================================
class FakeSourceDB:
    """Fausse base source : applique le watermark comme la vraie requête SQL,
    puis le même contrôle de schéma et de types que le vrai DatabaseExtractor."""

    def __init__(self, customers: pd.DataFrame) -> None:
        self.customers = customers.copy()
        self.calls: List[Optional[str]] = []
        self._real = DatabaseExtractor(DatabaseConfig("fake", 5432, "fake", "fake", "fake"))

    def extract_customers(self, last_updated: Optional[str] = None) -> pd.DataFrame:
        self.calls.append(last_updated)
        df = self.customers
        if last_updated:
            df = df[pd.to_datetime(df["last_updated"]) > pd.Timestamp(last_updated)]
        return self._real._enforce_schema(df.reset_index(drop=True))


class FakeAPI:
    """Fausse API : filtre les commandes par date comme l'API simulée."""

    def __init__(self, orders: List[Dict[str, Any]]) -> None:
        self.orders = orders
        self.calls: List[Optional[str]] = []
        self.stats = {"requests": 0, "retries": 0, "pages": 0}

    def extract_orders(self, date_from: Optional[str] = None) -> List[Dict[str, Any]]:
        self.calls.append(date_from)
        return [order for order in self.orders if not date_from or order["order_date"] >= date_from]


class FakeFile:
    def __init__(self, products: pd.DataFrame) -> None:
        self.products = products
        self.calls = 0
        self.repaired = False

    def extract_products(self) -> pd.DataFrame:
        self.calls += 1
        return self.products.copy()


class InMemoryWarehouse:
    """Faux entrepôt : applique le SCD2 et les chargements en mémoire, comme PostgresLoader."""

    def __init__(self) -> None:
        self.dim = pd.DataFrame(columns=DIMENSION_COLUMNS)
        self.next_key = 1
        self.loads: List[Dict[str, Any]] = []

    def fetch_dimension(self, customer_ids: Optional[List[int]] = None) -> pd.DataFrame:
        if customer_ids is None:
            return self.dim.copy()
        return self.dim[self.dim["customer_id"].isin(list(customer_ids))].reset_index(drop=True).copy()

    def load(self, batch: LoadBatch, run_id: str) -> Dict[str, int]:
        for _, row in batch.customers_to_update.iterrows():
            mask = self.dim["dim_customer_key"] == row["dim_customer_key"]
            assert self.dim.loc[mask, "is_current"].all(), "fermeture d'une version déjà fermée"
            self.dim.loc[mask, "effective_end_date"] = row["effective_end_date"]
            self.dim.loc[mask, "is_current"] = False
        if not batch.customers_to_insert.empty:
            new_rows = batch.customers_to_insert.drop(columns=["change_type"]).copy()
            new_rows.insert(0, "dim_customer_key", range(self.next_key, self.next_key + len(new_rows)))
            self.next_key += len(new_rows)
            new_rows = new_rows[DIMENSION_COLUMNS]
            self.dim = new_rows if self.dim.empty else pd.concat([self.dim, new_rows], ignore_index=True)
        counts = {
            "dim_customers_inserted": len(batch.customers_to_insert),
            "dim_customers_closed": len(batch.customers_to_update),
            "raw_orders": len(batch.orders),
            "raw_products": len(batch.products),
            "fact_daily_orders": len(batch.daily_order_facts),
        }
        self.loads.append({"run_id": run_id, **counts})
        return counts


@pytest.fixture
def make_pipeline(tmp_config: PipelineConfig):
    """Fabrique de pipelines branchés sur les doublures."""

    def factory(run_id: str, db: Any, api: Any, files: Any, warehouse: Any, **kwargs: Any) -> DataPipeline:
        return DataPipeline(
            tmp_config, run_id=run_id, db_extractor=db, api_extractor=api,
            file_extractor=files, loader=warehouse, today=TODAY, **kwargs,
        )

    return factory
