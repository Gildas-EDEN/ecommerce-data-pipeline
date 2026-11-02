"""
Tests de la qualité des données.
  Test Case 5 : valeurs manquantes dans des champs obligatoires -> échec de la validation
                avec un rapport d'erreurs détaillé, et arrêt du pipeline (porte qualité)
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict

import pandas as pd
import pytest

from transformers import FAIL, PASS, WARN, DataQualityError, DataValidator, flatten_orders, standardize_customers
from tests.conftest import FakeAPI, FakeFile, FakeSourceDB, InMemoryWarehouse


def customers_with_nulls() -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": pd.array([1001, 1002, None, 1004], dtype="Int64"),
        "name": ["Alice Johnson", None, "Carol Davis", "David Wilson"],
        "email": ["alice@email.com", "bob@email.com", "carol@email.com", None],
        "registration_date": [date(2023, 1, 15)] * 4,
        "status": ["active", "active", "inactive", "active"],
        "last_updated": pd.to_datetime(["2024-02-01"] * 4),
    })


# =============================================================================
# Test Case 5 : valeurs manquantes dans des champs obligatoires
# =============================================================================
def test_tc5_nulls_in_required_fields_fail_with_detailed_report(rules: Dict[str, Any]) -> None:
    validator = DataValidator(rules["validation"])
    result = validator.validate_data_quality(customers_with_nulls(), "customers")

    assert result["overall_status"] == FAIL
    assert result["decision"] == "stop"
    check = result["checks"]["required_not_null"]
    assert check["status"] == FAIL
    assert check["failed_rows"] == 3
    # Rapport détaillé : nombre de valeurs manquantes par colonne...
    assert check["details"] == {"customer_id": 1, "name": 1, "email": 1}
    # ... et exemples de lignes fautives (numéro de ligne + identifiant)
    assert {"row_number": 1, "customer_id": 1002} in check["sample"]
    assert result["invalid_records"] == 3


def test_tc5_quality_gate_stops_the_pipeline(make_pipeline, sample_orders, sample_products) -> None:
    """La validation échoue : le rapport est sauvegardé et AUCUNE étape suivante ne s'exécute."""
    warehouse = InMemoryWarehouse()
    pipeline = make_pipeline("run-tc5", FakeSourceDB(customers_with_nulls()), FakeAPI(sample_orders),
                             FakeFile(sample_products), warehouse)

    with pytest.raises(DataQualityError, match="customers"):
        pipeline.run()

    assert pipeline.checkpoints.is_done("extract_customers")
    assert pipeline.checkpoints.stage_info("validate")["status"] == "failed"
    assert not pipeline.checkpoints.stage_info("transform_customers")  # jamais démarrée
    assert warehouse.loads == []                                       # rien n'a été chargé
    report = pipeline.checkpoints.load_json("validation_results")
    assert report["tables"]["customers"]["overall_status"] == FAIL


# =============================================================================
# Autres contrôles
# =============================================================================
def test_course_sample_data_passes(rules, sample_customers, sample_orders, sample_products) -> None:
    validator = DataValidator(rules["validation"])
    orders, items = flatten_orders(sample_orders)
    customers = standardize_customers(sample_customers)
    customers["registration_date"] = pd.to_datetime(customers["registration_date"]).dt.date
    for name, df in {"customers": customers, "orders": orders, "order_items": items,
                     "products": sample_products}.items():
        assert validator.validate_data_quality(df, name)["overall_status"] in (PASS, WARN), name
    assert validator.check_order_totals(orders, items)["status"] == PASS


@pytest.mark.parametrize("column, value, check_name", [
    ("email", "pas-un-email", "email_format_email"),
    ("status", "vip", "allowed_values_status"),
    ("customer_id", 1001, "unique_customer_id"),
])
def test_business_rules_violations_fail(rules, column, value, check_name) -> None:
    df = customers_with_nulls().dropna().reset_index(drop=True)
    df.loc[1, column] = value
    result = DataValidator(rules["validation"]).validate_data_quality(df, "customers")
    assert result["checks"][check_name]["status"] == FAIL
    assert result["overall_status"] == FAIL


def test_negative_price_fails(rules, sample_products) -> None:
    sample_products.loc[0, "price"] = -5.0
    result = DataValidator(rules["validation"]).validate_data_quality(sample_products, "products")
    assert result["checks"]["positive_price"]["status"] == FAIL


def test_missing_required_column_is_structural_failure(rules, sample_products) -> None:
    lenient = {**rules["validation"], "products": {**rules["validation"]["products"], "max_invalid_ratio": 1.0}}
    result = DataValidator(lenient).validate_data_quality(sample_products.drop(columns=["supplier"]), "products")
    assert result["checks"]["schema"]["status"] == FAIL
    assert result["overall_status"] == FAIL  # même avec une tolérance maximale


def test_quarantine_below_tolerance(rules) -> None:
    lenient = {**rules["validation"], "customers": {**rules["validation"]["customers"], "max_invalid_ratio": 0.5}}
    validator = DataValidator(lenient)
    df = customers_with_nulls().iloc[:2].reset_index(drop=True)  # 1 ligne invalide sur 2 = 50 %
    result = validator.validate_data_quality(df, "customers")
    assert result["overall_status"] == WARN and result["decision"] == "quarantine"
    valid, rejected = validator.split_valid_invalid(df, "customers")
    assert list(valid["customer_id"]) == [1001] and list(rejected["customer_id"]) == [1002]


def test_order_total_mismatch_is_a_warning(rules, sample_orders) -> None:
    sample_orders[0]["total_amount"] = 160.97  # frais de port inclus
    orders, items = flatten_orders(sample_orders)
    validator = DataValidator(rules["validation"])
    validator.validate_data_quality(orders, "orders")
    check = validator.check_order_totals(orders, items)
    assert check["status"] == WARN
    assert check["details"]["orders_with_total_mismatch"] == 1
    assert validator.validation_results["orders"]["overall_status"] == WARN


def test_unknown_column_is_reported_as_schema_drift(rules, sample_products) -> None:
    sample_products["color"] = "red"
    result = DataValidator(rules["validation"]).validate_data_quality(sample_products, "products")
    assert result["checks"]["schema"]["status"] == WARN
    assert result["checks"]["schema"]["details"]["unknown_columns"] == ["color"]
    assert datetime.fromisoformat(result["validation_timestamp"])
