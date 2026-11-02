"""
scripts/generate_sample_data.py : génère les commandes servies par l'API simulée.

Le cours ne fournit que 2 commandes d'exemple (Sample_orders.json). Pour que les
agrégats et les tableaux de bord aient du sens, on génère environ 150 commandes
sur janvier-février 2024, avec les 5 clients et les 5 produits du cours.

  - les 2 commandes du cours sont reprises telles quelles ;
  - les prix unitaires sont ceux du catalogue (data/input/products.csv) ;
  - la génération est déterministe (graine fixe) : mêmes données à chaque fois ;
  - une commande inclut volontairement 5,99 de frais de port : son total ne
    correspond pas à la somme de ses lignes, la validation le signalera (WARN).

Usage : python scripts/generate_sample_data.py
"""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "api" / "orders.json"

CUSTOMER_IDS = [1001, 1002, 1003, 1004, 1005]
CUSTOMER_WEIGHTS = [5, 4, 1, 3, 3]  # Carol (1003, inactive) commande peu
PRODUCT_PRICES = {"P001": 129.99, "P002": 89.95, "P003": 24.99, "P004": 199.99, "P005": 45.00}

# Les deux commandes fournies par le cours (Sample_orders.json), reprises à l'identique.
COURSE_ORDERS = [
    {
        "order_id": "ORD-2024-001", "customer_id": 1001, "order_date": "2024-02-01", "total_amount": 154.98,
        "items": [
            {"product_id": "P001", "quantity": 1, "unit_price": 129.99},
            {"product_id": "P003", "quantity": 1, "unit_price": 24.99},
        ],
    },
    {
        "order_id": "ORD-2024-002", "customer_id": 1002, "order_date": "2024-02-01", "total_amount": 89.95,
        "items": [{"product_id": "P002", "quantity": 1, "unit_price": 89.95}],
    },
]


def generate(seed: int = 42) -> list:
    rng = random.Random(seed)
    orders = list(COURSE_ORDERS)
    number = len(COURSE_ORDERS)
    day = date(2024, 1, 2)
    while day <= date(2024, 2, 29):
        for _ in range(rng.randint(1, 4)):
            number += 1
            products = rng.sample(list(PRODUCT_PRICES), k=rng.randint(1, 3))
            items = [
                {"product_id": pid, "quantity": rng.randint(1, 3), "unit_price": PRODUCT_PRICES[pid]}
                for pid in products
            ]
            total = round(sum(item["quantity"] * item["unit_price"] for item in items), 2)
            orders.append({
                "order_id": f"ORD-2024-{number:03d}",
                "customer_id": rng.choices(CUSTOMER_IDS, weights=CUSTOMER_WEIGHTS)[0],
                "order_date": day.isoformat(),
                "total_amount": total,
                "items": items,
            })
        day += timedelta(days=1)

    # Anomalie volontaire (non bloquante) : frais de port inclus dans le total.
    orders[10]["total_amount"] = round(orders[10]["total_amount"] + 5.99, 2)
    return orders


def main() -> None:
    orders = generate()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps({"orders": orders}, indent=2), encoding="utf-8")
    print(f"{len(orders)} commandes écrites dans {OUTPUT}")


if __name__ == "__main__":
    main()
