"""
scripts/benchmark.py : mesure des performances sur 100 000 enregistrements.

Exigence du cours : traiter jusqu'à 100 000 enregistrements en moins de 2 heures
sur une machine standard (4 cœurs, 8 Go de RAM). Ce script chronomètre les
transformations du pipeline, sans base de données ni API :
  1. nettoyage + validation de 100 000 clients
  2. SCD2 : 100 000 clients entrants comparés à une dimension de 100 000 versions,
     dont 10 % ont changé
  3. aplatissement de 100 000 commandes JSON + validation + agrégats

Usage : python scripts/benchmark.py [--rows 100000]
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from config import load_rules  # noqa: E402
from transformers import (  # noqa: E402
    END_OF_TIME,
    DataValidator,
    SCD2Processor,
    build_daily_order_facts,
    build_product_performance,
    flatten_orders,
    standardize_customers,
)

PRICES = {"P001": 129.99, "P002": 89.95, "P003": 24.99, "P004": 199.99, "P005": 45.00}


def make_customers(n: int) -> pd.DataFrame:
    ids = range(1, n + 1)
    return pd.DataFrame({
        "customer_id": pd.array(list(ids), dtype="Int64"),
        "name": [f"Client {i}" for i in ids],
        "email": [f"client{i}@email.com" for i in ids],
        "registration_date": [date(2023, 1, 1) + timedelta(days=i % 365) for i in ids],
        "status": ["inactive" if i % 7 == 0 else "active" for i in ids],
        "last_updated": pd.Timestamp("2024-02-01 10:00:00"),
    })


def make_orders(n: int, n_customers: int, rng: random.Random) -> list:
    orders = []
    for i in range(n):
        items = [{"product_id": pid, "quantity": rng.randint(1, 3), "unit_price": PRICES[pid]}
                 for pid in rng.sample(list(PRICES), k=rng.randint(1, 3))]
        orders.append({
            "order_id": f"ORD-{i:07d}",
            "customer_id": rng.randint(1, n_customers),
            "order_date": (date(2024, 1, 1) + timedelta(days=i % 60)).isoformat(),
            "total_amount": round(sum(it["quantity"] * it["unit_price"] for it in items), 2),
            "items": items,
        })
    return orders


def timed(label: str, results: list, func):
    start = time.perf_counter()
    value = func()
    results.append((label, time.perf_counter() - start))
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    n = parser.parse_args().rows
    rng = random.Random(7)
    rules = load_rules()
    results: list = []

    size = f"{n:_}".replace("_", " ")
    print(f"Génération des données de test ({size} clients, {size} commandes)...")
    customers = make_customers(n)
    dimension = customers.rename(columns={"registration_date": "effective_start_date"}).drop(columns=["last_updated"])
    dimension.insert(0, "dim_customer_key", range(1, n + 1))
    dimension["effective_end_date"] = END_OF_TIME
    dimension["is_current"] = True
    changed = customers.sample(frac=0.10, random_state=1).index
    customers.loc[changed, "email"] = customers.loc[changed, "email"].str.replace("@email.com", "@nouveau.com")
    customers.loc[changed, "last_updated"] = pd.Timestamp("2026-10-06 09:00:00")
    raw_orders = make_orders(n, n, rng)

    validator = DataValidator(rules["validation"])
    clean = timed("1. Nettoyage des clients", results, lambda: standardize_customers(customers))
    timed("1. Validation des clients", results, lambda: validator.validate_data_quality(clean, "customers"))
    to_insert, to_update = timed(
        "2. SCD2 (10 % de changements)", results,
        lambda: SCD2Processor().apply_scd2_logic(clean, dimension, effective_date=date(2026, 10, 6)),
    )
    orders, items = timed("3. Aplatissement des commandes JSON", results, lambda: flatten_orders(raw_orders))
    timed("3. Validation des commandes", results, lambda: (
        validator.validate_data_quality(orders, "orders"),
        validator.validate_data_quality(items, "order_items"),
        validator.check_order_totals(orders, items),
    ))
    timed("3. Agrégats (faits journaliers)", results, lambda: build_daily_order_facts(orders))
    timed("3. Agrégats (performance produits)", results, lambda: build_product_performance(orders, items))

    print()
    for label, seconds in results:
        print(f"  {label:<38} {seconds:7.2f} s")
    total = sum(seconds for _, seconds in results)
    print(f"  {'TOTAL':<38} {total:7.2f} s   (limite du cahier des charges : 7 200 s)")
    print(f"\nSCD2 : {len(to_insert)} version(s) à insérer, {len(to_update)} à fermer "
          f"(attendu : {len(changed)} et {len(changed)})")
    print(f"Commandes : {len(orders)} en-têtes, {len(items)} lignes")


if __name__ == "__main__":
    main()
