"""
Test Case 6 : traitement incrémental.
  Entrée  : un gros jeu de données dont seulement 10 % des lignes sont nouvelles ou modifiées
  Attendu : seul ce sous-ensemble est extrait, transformé et chargé
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from tests.conftest import FakeAPI, FakeFile, FakeSourceDB, InMemoryWarehouse

N_CUSTOMERS = 10_000


def big_customer_table() -> pd.DataFrame:
    ids = np.arange(1, N_CUSTOMERS + 1)
    return pd.DataFrame({
        "customer_id": pd.array(ids, dtype="Int64"),
        "name": [f"Client {i}" for i in ids],
        "email": [f"client{i}@email.com" for i in ids],
        "registration_date": [date(2023, 1, 1) + timedelta(days=int(i % 365)) for i in ids],
        "status": np.where(ids % 7 == 0, "inactive", "active"),
        "last_updated": pd.Timestamp("2024-01-01 08:00:00") + pd.to_timedelta(ids % 1000, unit="s"),
    })


def orders_for(day: str, start: int, count: int) -> list:
    return [
        {"order_id": f"ORD-{start + i}", "customer_id": 1 + i, "order_date": day, "total_amount": 10.0,
         "items": [{"product_id": "P001", "quantity": 1, "unit_price": 10.0}]}
        for i in range(count)
    ]


def test_tc6_only_new_or_changed_records_are_processed(make_pipeline, tmp_config, sample_products) -> None:
    source = big_customer_table()
    api = FakeAPI(orders_for("2024-01-01", 0, 50))
    files = FakeFile(sample_products)
    warehouse = InMemoryWarehouse()

    # --- 1re exécution : chargement complet (aucun watermark) -----------------------
    db = FakeSourceDB(source)
    first = make_pipeline("run-1", db, api, files, warehouse)
    first.run()
    assert db.calls == [None]                                   # extraction complète
    assert first.checkpoints.stage_metadata("extract_customers")["rows"] == N_CUSTOMERS
    assert len(warehouse.dim) == N_CUSTOMERS
    watermark = first.watermarks.get("customers")
    assert watermark == source["last_updated"].max().isoformat()

    # --- Activité : 10 % de lignes nouvelles ou modifiées -----------------------------
    changed_at = pd.Timestamp("2024-02-01 09:00:00")
    modified = source.sample(n=500, random_state=1).index
    source.loc[modified, "email"] = source.loc[modified, "email"].str.replace("@email.com", "@nouveau.com")
    source.loc[modified, "last_updated"] = changed_at
    new_ids = np.arange(N_CUSTOMERS + 1, N_CUSTOMERS + 501)
    newcomers = pd.DataFrame({
        "customer_id": pd.array(new_ids, dtype="Int64"),
        "name": [f"Nouveau {i}" for i in new_ids],
        "email": [f"nouveau{i}@email.com" for i in new_ids],
        "registration_date": [date(2024, 2, 1)] * 500,
        "status": "active",
        "last_updated": changed_at,
    })
    source = pd.concat([source, newcomers], ignore_index=True)
    api.orders += orders_for("2024-02-01", 1000, 20)

    # --- 2e exécution : seul le sous-ensemble modifié est traité ----------------------
    db = FakeSourceDB(source)
    second = make_pipeline("run-2", db, api, files, warehouse)
    second.run()

    assert db.calls == [watermark]                              # le watermark est bien transmis
    extract = second.checkpoints.stage_metadata("extract_customers")
    assert extract["mode"] == "incrémental"
    assert extract["rows"] == 1_000                             # 10 % de 10 000
    scd2 = second.checkpoints.stage_metadata("transform_customers")
    assert scd2["incoming_customers"] == 1_000
    assert scd2["changes"] == {"changed": 500, "new": 500}
    assert scd2["versions_to_close"] == 500
    assert warehouse.loads[-1]["dim_customers_inserted"] == 1_000
    # Historique complet : 10 000 + 500 nouveaux clients + 500 nouvelles versions
    assert len(warehouse.dim) == 11_000
    assert int(warehouse.dim["is_current"].sum()) == 10_500

    # Commandes : on repart du dernier jour déjà traité (inclus), pas de tout l'historique
    assert api.calls == [None, "2024-01-01"]
    assert second.checkpoints.stage_metadata("transform_orders")["days_recomputed"] == 2
    # Catalogue produits : fichier inchangé (même empreinte SHA-256) -> pas relu
    assert files.calls == 1
    assert second.checkpoints.stage_metadata("extract_products")["mode"] == "inchangé (ignoré)"


def test_watermark_advances_only_after_successful_load(make_pipeline, sample_customers, sample_orders, sample_products) -> None:
    class FailingWarehouse(InMemoryWarehouse):
        def load(self, batch, run_id):
            raise RuntimeError("base indisponible")

    pipeline = make_pipeline("run-fail", FakeSourceDB(sample_customers), FakeAPI(sample_orders),
                             FakeFile(sample_products), FailingWarehouse())
    try:
        pipeline.run()
    except RuntimeError:
        pass
    # Le chargement a échoué : les watermarks n'ont pas bougé, rien ne sera perdu.
    assert pipeline.watermarks.all() == {}


def test_rerun_with_same_data_is_idempotent(make_pipeline, sample_customers, sample_orders, sample_products) -> None:
    warehouse = InMemoryWarehouse()
    for run_id in ("run-a", "run-b"):
        # full_refresh : on renvoie volontairement les mêmes données une 2e fois
        make_pipeline(run_id, FakeSourceDB(sample_customers), FakeAPI(sample_orders),
                      FakeFile(sample_products), warehouse, full_refresh=True).run()
    assert warehouse.loads[0]["dim_customers_inserted"] == 5
    assert warehouse.loads[1]["dim_customers_inserted"] == 0   # aucune fausse nouvelle version
    assert len(warehouse.dim) == 5
