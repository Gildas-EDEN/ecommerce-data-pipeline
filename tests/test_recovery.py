"""
Test Case 7 : reprise après échec (checkpoints).
  Entrée  : le pipeline échoue APRÈS l'extraction, pendant la transformation
  Attendu : la relance repart de la transformation, SANS ré-extraire les données
"""

from __future__ import annotations

import pytest

from checkpoint import CheckpointManager
from pipeline import DataPipeline
from tests.conftest import FakeAPI, FakeFile, FakeSourceDB, InMemoryWarehouse


def test_tc7_restart_from_transformation_without_reextracting(
    make_pipeline, monkeypatch, sample_customers, sample_orders, sample_products
) -> None:
    db, api, files = FakeSourceDB(sample_customers), FakeAPI(sample_orders), FakeFile(sample_products)
    warehouse = InMemoryWarehouse()

    # Panne simulée : la transformation plante lors de la 1re tentative uniquement.
    original = DataPipeline._stage_transform_customers
    attempts = {"count": 0}

    def flaky_transform(self):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("panne simulée pendant la transformation")
        return original(self)

    monkeypatch.setattr(DataPipeline, "_stage_transform_customers", flaky_transform)

    # --- 1re tentative : échec ------------------------------------------------------
    first = make_pipeline("run-tc7", db, api, files, warehouse)
    with pytest.raises(RuntimeError, match="panne simulée"):
        first.run()
    cp = first.checkpoints
    assert all(cp.is_done(stage) for stage in ("extract_customers", "extract_orders", "extract_products", "validate"))
    assert cp.stage_info("transform_customers")["status"] == "failed"
    assert cp.run_info()["status"] == "failed"
    assert warehouse.loads == []

    # --- 2e tentative, même run_id : reprise ---------------------------------------
    second = make_pipeline("run-tc7", db, api, files, warehouse)
    second.run()

    assert db.calls == [None]         # base clients interrogée UNE seule fois
    assert api.calls == [None]        # API appelée UNE seule fois
    assert files.calls == 1           # fichier lu UNE seule fois
    assert attempts["count"] == 2     # la transformation, elle, a bien été rejouée
    assert len(warehouse.loads) == 1
    assert second.checkpoints.run_info()["status"] == "success"


def test_resume_finds_latest_incomplete_run(tmp_path) -> None:
    done = CheckpointManager(tmp_path, "cli_20261006T090000")
    done.set_run_status("success")
    failed = CheckpointManager(tmp_path, "cli_20261006T100000")
    failed.set_run_status("failed")
    airflow_run = CheckpointManager(tmp_path, "manual__2026-10-06T11:00:00+00:00")
    airflow_run.set_run_status("failed")
    # Sans filtre : la plus récente, quelle que soit son origine
    assert CheckpointManager.find_latest_incomplete(tmp_path) == airflow_run.run_id
    # --resume ne reprend que les exécutions lancées en ligne de commande
    assert CheckpointManager.find_latest_incomplete(tmp_path, prefix="cli_") == "cli_20261006T100000"


def test_from_stage_replays_later_stages(make_pipeline, sample_customers, sample_orders, sample_products) -> None:
    db, api, files = FakeSourceDB(sample_customers), FakeAPI(sample_orders), FakeFile(sample_products)
    warehouse = InMemoryWarehouse()
    pipeline = make_pipeline("run-replay", db, api, files, warehouse)
    pipeline.run()
    pipeline.run(from_stage="transform_orders")   # rejoue à partir des agrégats
    assert db.calls == [None]                     # pas de ré-extraction
    assert len(warehouse.loads) == 2
    # Le SCD2 a été recalculé avant le rechargement : aucune version en double.
    assert warehouse.loads[1]["dim_customers_inserted"] == 0
    assert len(warehouse.dim) == 5


def test_airflow_style_run_ids_are_safe_folder_names(tmp_path) -> None:
    cp = CheckpointManager(tmp_path, "manual__2026-10-06T10:00:00+00:00")
    assert cp.run_id == "manual__2026-10-06T10-00-00-00-00"
    assert cp.run_dir.exists()
