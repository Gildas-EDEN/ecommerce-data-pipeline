"""
scripts/simulate_changes.py : simule une journée d'activité dans les systèmes sources.

Après une première exécution du pipeline, lance ce script puis relance le DAG :
tu verras l'extraction INCRÉMENTALE (seuls les changements sont extraits) et
l'historisation SCD2 (anciennes versions fermées, nouvelles versions créées).

Changements appliqués :
  1. Bob Smith (1002) change d'adresse e-mail        -> Test Case 4 : version fermée + nouvelle version
  2. Carol Davis (1003) redevient « active »         -> changement de statut historisé
  3. Trois nouveaux clients (1006, 1007, 1008)      -> Test Case 3 : trois nouvelles versions
  4. Quatre nouvelles commandes datées d'aujourd'hui -> ajoutées au fichier lu par l'API simulée

Usage (depuis le conteneur Airflow, qui a accès à la base) :
    docker compose exec airflow python /opt/project/scripts/simulate_changes.py
ou depuis ton PC (environnement Python du projet activé, fichier .env présent) :
    python scripts/simulate_changes.py
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402

from config import load_config  # noqa: E402

ORDERS_FILE = ROOT / "data" / "api" / "orders.json"

SQL_CHANGES = [
    ("Bob Smith (1002) change d'e-mail",
     "UPDATE customers SET email = 'bob.smith@newmail.com', last_updated = NOW() WHERE customer_id = 1002"),
    ("Carol Davis (1003) redevient active",
     "UPDATE customers SET status = 'active', last_updated = NOW() WHERE customer_id = 1003"),
    ("3 nouveaux clients (1006 à 1008)",
     """INSERT INTO customers (customer_id, name, email, registration_date, status, last_updated) VALUES
        (1006, 'Frank Miller', 'frank.miller@email.com', CURRENT_DATE, 'active', NOW()),
        (1007, 'Grace Lee', 'grace.lee@email.com', CURRENT_DATE, 'active', NOW()),
        (1008, 'Henry Taylor', 'henry.taylor@email.com', CURRENT_DATE, 'active', NOW())
        ON CONFLICT (customer_id) DO NOTHING"""),
]


def apply_customer_changes() -> None:
    config = load_config()
    with psycopg2.connect(**config.database.connection_kwargs()) as conn, conn.cursor() as cur:
        for label, statement in SQL_CHANGES:
            cur.execute(statement)
            print(f"  - {label} ({cur.rowcount} ligne(s))")


def append_todays_orders() -> None:
    payload = json.loads(ORDERS_FILE.read_text(encoding="utf-8"))
    today = date.today().isoformat()
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    new_orders = [
        {"order_id": f"ORD-{stamp}-1", "customer_id": 1002, "order_date": today, "total_amount": 129.99,
         "items": [{"product_id": "P001", "quantity": 1, "unit_price": 129.99}]},
        {"order_id": f"ORD-{stamp}-2", "customer_id": 1003, "order_date": today, "total_amount": 49.98,
         "items": [{"product_id": "P003", "quantity": 2, "unit_price": 24.99}]},
        {"order_id": f"ORD-{stamp}-3", "customer_id": 1006, "order_date": today, "total_amount": 244.99,
         "items": [{"product_id": "P004", "quantity": 1, "unit_price": 199.99},
                   {"product_id": "P005", "quantity": 1, "unit_price": 45.00}]},
        {"order_id": f"ORD-{stamp}-4", "customer_id": 1007, "order_date": today, "total_amount": 89.95,
         "items": [{"product_id": "P002", "quantity": 1, "unit_price": 89.95}]},
    ]
    payload["orders"].extend(new_orders)
    ORDERS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  - {len(new_orders)} commande(s) datée(s) du {today} ajoutée(s) à {ORDERS_FILE.name}")


def main() -> None:
    print("Changements dans la base clients :")
    apply_customer_changes()
    print("Nouvelles commandes dans l'API :")
    append_todays_orders()
    print("\nRelance maintenant le DAG (ou python pipeline.py) pour voir le traitement incrémental et le SCD2.")


if __name__ == "__main__":
    main()
