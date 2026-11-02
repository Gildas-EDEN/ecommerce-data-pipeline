"""
Tests des extracteurs.
  Test Case 1 : extraction depuis une base « simulée » (mock) -> bon schéma, bon nombre de lignes, bons types
  Test Case 2 : API qui renvoie HTTP 500 -> relances (backoff exponentiel) puis exception claire
"""

from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from pathlib import Path
from typing import Any, List, Optional
from unittest.mock import MagicMock

import pandas as pd
import psycopg2
import pytest
import requests

from config import DatabaseConfig
from extractors import APIExtractor, DatabaseExtractor, ExtractionError, FileExtractor

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples_cours"
DB_CONFIG = DatabaseConfig("localhost", 5432, "analytics", "user", "pwd")
COLUMNS = ["customer_id", "name", "email", "registration_date", "status", "last_updated"]
ROWS = [
    (1001, "Alice Johnson", "alice.johnson@email.com", dt.date(2023, 1, 15), "active", dt.datetime(2024, 2, 1, 10)),
    (1002, "Bob Smith", "bob.smith@email.com", dt.date(2023, 2, 20), "active", dt.datetime(2024, 2, 1, 10)),
    (1003, "Carol Davis", "carol.davis@email.com", dt.date(2023, 3, 10), "inactive", dt.datetime(2024, 1, 28, 15, 30)),
    (1004, "David Wilson", "david.wilson@email.com", dt.date(2023, 4, 5), "active", dt.datetime(2024, 2, 1, 10)),
    (1005, "Eve Brown", "eve.brown@email.com", dt.date(2023, 5, 12), "active", dt.datetime(2024, 1, 30, 9, 15)),
]


def mock_connection(rows: List[tuple], columns: List[str], batch_size: int):
    """Fausse connexion psycopg2 : le curseur renvoie les lignes par lots de batch_size."""
    cursor = MagicMock()
    cursor.description = [(name,) for name in columns]
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)] + [[]]
    cursor.fetchmany.side_effect = batches
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    state = {"opened": 0, "closed": 0}

    @contextmanager
    def provider():
        state["opened"] += 1
        try:
            yield conn
        finally:
            state["closed"] += 1

    return provider, conn, cursor, state


# =============================================================================
# Test Case 1 : extraction de base de données
# =============================================================================
def test_tc1_database_extraction_schema_row_count_and_types() -> None:
    provider, conn, cursor, state = mock_connection(ROWS, COLUMNS, batch_size=2)
    extractor = DatabaseExtractor(DB_CONFIG, batch_size=2, connection_provider=provider)

    df = extractor.extract_customers()

    # Bon nombre de lignes et bon schéma
    assert len(df) == 5
    assert list(df.columns) == COLUMNS
    # Types cohérents d'une exécution à l'autre
    assert str(df["customer_id"].dtype) == "Int64"
    assert all(isinstance(value, dt.date) for value in df["registration_date"])
    assert pd.api.types.is_datetime64_any_dtype(df["last_updated"])
    # Gestion de la connexion : curseur serveur, lecture par lots, connexion rendue
    conn.cursor.assert_called_once_with(name="extract_customers")
    assert cursor.fetchmany.call_count == 4  # 3 lots (2 + 2 + 1) puis un lot vide
    assert state == {"opened": 1, "closed": 1}


def test_tc1_incremental_query_is_parameterized() -> None:
    """Le watermark est passé en paramètre (%s), jamais collé dans le SQL : pas d'injection possible."""
    provider, _, cursor, _ = mock_connection(ROWS[:1], COLUMNS, batch_size=10)
    extractor = DatabaseExtractor(DB_CONFIG, connection_provider=provider)

    extractor.extract_customers(last_updated="2024-01-31T00:00:00")

    query, params = cursor.execute.call_args.args
    assert "WHERE last_updated > %s" in query
    assert params == ["2024-01-31T00:00:00"]
    assert "2024-01-31" not in query


def test_tc1_empty_result_keeps_schema() -> None:
    provider, *_ = mock_connection([], COLUMNS, batch_size=10)
    df = DatabaseExtractor(DB_CONFIG, connection_provider=provider).extract_customers("2030-01-01")
    assert df.empty
    assert list(df.columns) == COLUMNS


def test_tc1_schema_change_missing_column_is_blocking() -> None:
    columns = [c for c in COLUMNS if c != "email"]
    rows = [tuple(v for v, c in zip(row, COLUMNS) if c != "email") for row in ROWS]
    provider, *_ = mock_connection(rows, columns, batch_size=10)
    with pytest.raises(ExtractionError, match="email"):
        DatabaseExtractor(DB_CONFIG, connection_provider=provider).extract_customers()


def test_tc1_schema_change_new_column_is_tolerated(caplog: pytest.LogCaptureFixture) -> None:
    rows = [row + ("FR",) for row in ROWS]
    provider, *_ = mock_connection(rows, COLUMNS + ["country"], batch_size=10)
    df = DatabaseExtractor(DB_CONFIG, connection_provider=provider).extract_customers()
    assert "country" in df.columns
    assert "Nouvelles colonnes" in caplog.text


def test_tc1_transient_database_error_is_retried() -> None:
    provider, *_ = mock_connection(ROWS, COLUMNS, batch_size=10)
    attempts = {"count": 0}

    @contextmanager
    def flaky_provider():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise psycopg2.OperationalError("connexion perdue")
        with provider() as conn:
            yield conn

    sleeps: List[float] = []
    extractor = DatabaseExtractor(DB_CONFIG, connection_provider=flaky_provider, sleep=sleeps.append)
    assert len(extractor.extract_customers()) == 5
    assert sleeps == [1.0]


# =============================================================================
# Test Case 2 : gestion des erreurs de l'API
# =============================================================================
def response(status: int, payload: Any = None, headers: Optional[dict] = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.json.return_value = payload
    return resp


def make_api(responses: List[Any], max_retries: int = 3) -> tuple:
    session = MagicMock()
    session.headers = {}
    session.get.side_effect = responses
    sleeps: List[float] = []
    api = APIExtractor(
        "http://api.test/orders", "secret", max_retries=max_retries, backoff_factor=1.0,
        rate_limit_per_sec=0, session=session, sleep=sleeps.append,
    )
    return api, session, sleeps


def test_tc2_http_500_triggers_retries_then_clear_exception() -> None:
    api, session, sleeps = make_api([response(500)] * 3)

    with pytest.raises(ExtractionError, match="après 3 tentatives"):
        api.extract_orders(date_from="2024-02-01")

    assert session.get.call_count == 3       # 1 essai + 2 relances
    assert sleeps == [1.0, 2.0]               # backoff exponentiel : 1 s puis 2 s
    assert api.stats["retries"] == 2


def test_tc2_api_recovers_after_transient_errors(sample_orders: list) -> None:
    ok = response(200, {"orders": sample_orders, "has_more": False, "next_page": None})
    api, session, sleeps = make_api([response(503), requests.ConnectionError("réseau"), ok])
    orders = api.extract_orders()
    assert len(orders) == 2
    assert session.get.call_count == 3
    assert sleeps == [1.0, 2.0]


def test_tc2_client_error_is_not_retried() -> None:
    api, session, sleeps = make_api([response(401)])
    with pytest.raises(ExtractionError, match="401"):
        api.extract_orders()
    assert session.get.call_count == 1 and sleeps == []


def test_tc2_rate_limit_respects_retry_after(sample_orders: list) -> None:
    ok = response(200, {"orders": sample_orders, "has_more": False})
    api, _, sleeps = make_api([response(429, headers={"Retry-After": "7"}), ok])
    assert len(api.extract_orders()) == 2
    assert sleeps == [7.0]


def test_api_pagination_and_authentication(sample_orders: list) -> None:
    page1 = response(200, {"orders": sample_orders[:1], "has_more": True, "next_page": 2})
    page2 = response(200, {"orders": sample_orders[1:], "has_more": False, "next_page": None})
    api, session, _ = make_api([page1, page2])

    orders = api.extract_orders(date_from="2024-02-01")

    assert [o["order_id"] for o in orders] == ["ORD-2024-001", "ORD-2024-002"]
    pages = [call.kwargs["params"]["page"] for call in session.get.call_args_list]
    assert pages == [1, 2]
    assert session.headers["Authorization"] == "Bearer secret"


def test_api_unexpected_payload_is_rejected() -> None:
    api, *_ = make_api([response(200, {"data": []})])
    with pytest.raises(ExtractionError, match="orders"):
        api.extract_orders()


# =============================================================================
# Fichier CSV du catalogue
# =============================================================================
def test_course_csv_malformed_format_is_repaired() -> None:
    extractor = FileExtractor(SAMPLES / "Product catalog data.csv")
    df = extractor.extract_products()
    assert extractor.repaired is True
    assert len(df) == 5
    assert list(df.columns) == ["product_id", "name", "category", "price", "supplier"]
    assert df.loc[df["product_id"] == "P001", "price"].iloc[0] == pytest.approx(129.99)


def test_csv_missing_column_is_blocking(tmp_path) -> None:
    path = tmp_path / "products.csv"
    path.write_text("product_id,name,price\nP001,Boots,10\n", encoding="utf-8")
    with pytest.raises(ExtractionError, match="Colonnes manquantes"):
        FileExtractor(path).extract_products()


def test_csv_bad_price_becomes_missing_value(tmp_path) -> None:
    path = tmp_path / "products.csv"
    path.write_text("product_id,name,category,price,supplier\nP001,Boots,Shoes,abc,ACME\n", encoding="utf-8")
    df = FileExtractor(path).extract_products()
    assert pd.isna(df.loc[0, "price"])  # la validation le signalera ensuite
