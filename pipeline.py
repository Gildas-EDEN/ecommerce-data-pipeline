"""
pipeline.py : orchestration du pipeline, utilisable seul ou depuis Airflow.

Étapes, dans l'ordre (chacune sauvegarde son résultat : checkpoint) :
  1. extract_customers  ┐
  2. extract_orders     ├ extraction des 3 sources, en parallèle
  3. extract_products   ┘
  4. validate             contrôles qualité + porte qualité (arrêt si FAIL)
  5. transform_customers  SCD2 de la dimension client
  6. transform_orders     agrégats (tables de faits) avec pandas ou Spark
  7. load                 écriture atomique dans PostgreSQL + avance des watermarks
  8. report               rapport d'exécution et de qualité (JSON + Markdown)

Utilisation en ligne de commande :
  python pipeline.py                        nouvelle exécution
  python pipeline.py --resume               reprend la dernière exécution inachevée
  python pipeline.py --run-id ID            reprend (ou crée) l'exécution ID
  python pipeline.py --run-id ID --from-stage transform_customers
                                            rejoue l'exécution ID à partir d'une étape
  python pipeline.py --engine spark         agrégats calculés avec Spark
  python pipeline.py --full-refresh         ignore les watermarks (rechargement complet)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd
import requests

from checkpoint import CheckpointManager, WatermarkStore
from config import PipelineConfig, load_config
from db import close_all_pools, ping
from extractors import APIExtractor, DatabaseExtractor, FileExtractor
from loaders import LoadBatch, PostgresLoader
from reporting import build_lineage, render_markdown_report
from transformers import (
    FAIL,
    DataQualityError,
    DataValidator,
    SCD2Processor,
    build_daily_order_facts,
    build_product_performance,
    flatten_orders,
    standardize_customers,
    standardize_products,
)
from utils import file_sha256, json_default, new_run_id, sanitize_run_id, setup_logging

logger = logging.getLogger("pipeline")

EXTRACT_STAGES = ("extract_customers", "extract_orders", "extract_products")
STAGES = EXTRACT_STAGES + ("validate", "transform_customers", "transform_orders", "load", "report")
# Préfixe des exécutions lancées en ligne de commande (celles d'Airflow commencent
# par « scheduled__ » ou « manual__ ») : --resume ne reprend que les premières.
CLI_PREFIX = "cli"


class DataPipeline:
    """Enchaîne les étapes et gère la reprise après erreur (checkpoints)."""

    def __init__(
        self,
        config: PipelineConfig,
        run_id: Optional[str] = None,
        *,
        full_refresh: bool = False,
        db_extractor: Any = None,
        api_extractor: Any = None,
        file_extractor: Any = None,
        loader: Any = None,
        today: Optional[date] = None,
    ) -> None:
        self.config = config
        self.run_id = sanitize_run_id(run_id or new_run_id(CLI_PREFIX))
        self.full_refresh = full_refresh
        self.checkpoints = CheckpointManager(config.checkpoint_dir, self.run_id)
        self.watermarks = WatermarkStore(config.state_dir / "watermarks.json")
        self.today = today or date.today()
        # Composants injectables : les tests passent ici de « faux » extracteurs / chargeurs.
        self._db_extractor = db_extractor
        self._api_extractor = api_extractor
        self._file_extractor = file_extractor
        self._loader = loader

    # -- composants, créés à la demande -------------------------------------------
    @property
    def db_extractor(self) -> DatabaseExtractor:
        if self._db_extractor is None:
            self._db_extractor = DatabaseExtractor(
                self.config.database, batch_size=self.config.batch_size, max_retries=self.config.db_max_retries
            )
        return self._db_extractor

    @property
    def api_extractor(self) -> APIExtractor:
        if self._api_extractor is None:
            cfg = self.config
            self._api_extractor = APIExtractor(
                cfg.api_endpoint, cfg.api_key, timeout=cfg.api_timeout, max_retries=cfg.api_max_retries,
                backoff_factor=cfg.api_backoff_factor, page_size=cfg.api_page_size,
                rate_limit_per_sec=cfg.api_rate_limit_per_sec, jitter_ratio=cfg.api_jitter_ratio,
            )
        return self._api_extractor

    @property
    def file_extractor(self) -> FileExtractor:
        if self._file_extractor is None:
            self._file_extractor = FileExtractor(self.config.products_file, chunk_size=self.config.batch_size)
        return self._file_extractor

    @property
    def loader(self) -> PostgresLoader:
        if self._loader is None:
            self._loader = PostgresLoader(self.config.database, batch_size=self.config.batch_size)
        return self._loader

    # -- exécution ----------------------------------------------------------------
    def run(self, from_stage: Optional[str] = None, engine: Optional[str] = None) -> Dict[str, Any]:
        """Exécute toutes les étapes ; celles déjà terminées (checkpoint) sont sautées."""
        if from_stage:
            if from_stage not in STAGES:
                raise ValueError(f"Étape inconnue : {from_stage}. Étapes possibles : {STAGES}")
            to_replay = list(STAGES[STAGES.index(from_stage):])
            if "load" in to_replay and "transform_customers" not in to_replay:
                # Le delta SCD2 dépend de l'état de la dimension au moment du calcul :
                # avant de recharger, on le recalcule (sinon on réinsérerait des versions).
                to_replay.append("transform_customers")
            self.checkpoints.invalidate(to_replay)
        logger.info("===== Exécution %s =====", self.run_id)
        try:
            # Les 3 extractions sont indépendantes : on les lance en parallèle (threads).
            # Si l'une échoue, les autres vont quand même au bout et sont sauvegardées.
            with ThreadPoolExecutor(max_workers=len(EXTRACT_STAGES), thread_name_prefix="extract") as pool:
                for _ in pool.map(self.run_stage, EXTRACT_STAGES):
                    pass
            for stage in STAGES[len(EXTRACT_STAGES):]:
                self.run_stage(stage, **({"engine": engine} if stage == "transform_orders" else {}))
        except Exception:
            self.checkpoints.set_run_status("failed")
            raise
        self.checkpoints.set_run_status("success")
        logger.info("===== Exécution %s terminée avec succès =====", self.run_id)
        return self.checkpoints.stage_metadata("report")

    def run_stage(self, stage: str, **kwargs: Any) -> Dict[str, Any]:
        """Exécute UNE étape. Utilisé par run() et par chaque tâche Airflow."""
        if stage not in STAGES:
            raise ValueError(f"Étape inconnue : {stage}. Étapes possibles : {STAGES}")
        if self.checkpoints.is_done(stage):
            logger.info("Étape %-20s : déjà terminée (checkpoint), on la saute", stage)
            return self.checkpoints.stage_metadata(stage)

        logger.info("Étape %-20s : démarrage", stage)
        if self.checkpoints.run_info().get("status") != "running":
            self.checkpoints.set_run_status("running")
        self.checkpoints.mark_running(stage)
        started = time.perf_counter()
        try:
            metadata = getattr(self, f"_stage_{stage}")(**kwargs)
        except Exception as exc:
            self.checkpoints.mark_failed(stage, f"{exc.__class__.__name__}: {exc}")
            self.checkpoints.set_run_status("failed")
            logger.error("Étape %-20s : ÉCHEC (%s)", stage, exc)
            raise
        metadata["duration_seconds"] = round(time.perf_counter() - started, 3)
        # Conversion en types JSON natifs : ces métadonnées partent aussi dans les XCom d'Airflow.
        metadata = json.loads(json.dumps(metadata, default=json_default))
        self.checkpoints.mark_done(stage, metadata)
        logger.info("Étape %-20s : terminée en %.2f s", stage, metadata["duration_seconds"])
        return metadata

    def _watermark(self, source: str) -> Optional[str]:
        return None if self.full_refresh else self.watermarks.get(source)

    # -- 1 à 3 : extraction -----------------------------------------------------------
    def _stage_extract_customers(self) -> Dict[str, Any]:
        watermark = self._watermark("customers")
        customers = self.db_extractor.extract_customers(last_updated=watermark)
        self.checkpoints.save_frame("raw_customers", customers)
        candidate = watermark
        if not customers.empty and customers["last_updated"].notna().any():
            candidate = customers["last_updated"].max().isoformat()
        db = self.config.database
        return {
            "rows": len(customers),
            "mode": "incrémental" if watermark else "complet",
            "watermark_used": watermark,
            "watermark_candidate": candidate,
            "source": f"postgresql://{db.host}:{db.port}/{db.database} (table public.customers)",
        }

    def _stage_extract_orders(self) -> Dict[str, Any]:
        watermark = self._watermark("orders")
        records = self.api_extractor.extract_orders(date_from=watermark)
        orders, items = flatten_orders(records)
        # Zone d'atterrissage : on garde la réponse brute de l'API (traçabilité, rejeu possible).
        self.checkpoints.save_json("raw_orders_payload", records)
        self.checkpoints.save_frame("raw_orders", orders)
        self.checkpoints.save_frame("raw_order_items", items)
        dates = [d for d in orders["order_date"] if d is not None]
        stats = getattr(self.api_extractor, "stats", {})
        return {
            "rows": len(orders),
            "order_items": len(items),
            "mode": "incrémental" if watermark else "complet",
            # On repart du dernier jour traité (inclus) : ce jour est retraité en entier,
            # ce qui garantit des agrégats journaliers complets (écriture idempotente).
            "watermark_used": watermark,
            "watermark_candidate": max(dates).isoformat() if dates else watermark,
            "api_requests": stats.get("requests"),
            "api_retries": stats.get("retries"),
            "source": self.config.api_endpoint,
        }

    def _stage_extract_products(self) -> Dict[str, Any]:
        path = self.config.products_file
        previous = self._watermark("products")
        checksum = file_sha256(path) if path.exists() else None
        if checksum is not None and checksum == previous:
            # Incrémental au niveau du fichier : catalogue identique depuis la dernière exécution réussie.
            logger.info("Catalogue produits inchangé (même empreinte SHA-256) : rien à retraiter")
            products = pd.DataFrame(columns=list(FileExtractor.REQUIRED_COLUMNS))
            mode = "inchangé (ignoré)"
        else:
            products = self.file_extractor.extract_products()
            mode = "incrémental (fichier modifié)" if previous else "complet"
        self.checkpoints.save_frame("raw_products", products)
        return {
            "rows": len(products),
            "mode": mode,
            "file": str(path),
            "sha256": checksum,
            "repaired_format": bool(getattr(self.file_extractor, "repaired", False)),
            "watermark_used": previous,
            "watermark_candidate": checksum,
        }

    # -- 4 : validation (porte qualité) -----------------------------------------------
    def _stage_validate(self) -> Dict[str, Any]:
        cp = self.checkpoints
        datasets = {
            "customers": standardize_customers(cp.load_frame("raw_customers")),
            "orders": cp.load_frame("raw_orders"),
            "order_items": cp.load_frame("raw_order_items"),
            "products": standardize_products(cp.load_frame("raw_products")),
        }
        validator = DataValidator(self.config.rules.get("validation", {}))
        for name, df in datasets.items():
            validator.validate_data_quality(df, name)
        tolerance = float(self.config.rules.get("order_totals", {}).get("tolerance", 0.01))
        validator.check_order_totals(datasets["orders"], datasets["order_items"], tolerance)
        report = validator.build_report()
        # Le rapport est sauvegardé AVANT la décision : en cas d'arrêt, on sait pourquoi.
        report_path = cp.save_json("validation_results", report)

        failed = [name for name, result in report["tables"].items() if result["overall_status"] == FAIL]
        if failed:
            raise DataQualityError(
                f"Porte qualité fermée : données refusées pour {failed}. Rapport détaillé : {report_path}", report
            )

        summary: Dict[str, Any] = {}
        for name, df in list(datasets.items()):
            valid, rejected = validator.split_valid_invalid(df, name)
            if not rejected.empty:
                cp.save_frame(f"rejected_{name}", rejected)
                logger.warning("%d ligne(s) de %s mise(s) en quarantaine", len(rejected), name)
            datasets[name] = valid
            summary[name] = {
                "status": report["tables"][name]["overall_status"],
                "rows": len(df),
                "quarantined": len(rejected),
            }
        # Cohérence : on écarte les lignes dont la commande a été mise en quarantaine.
        valid_orders = set(datasets["orders"]["order_id"].dropna())
        orphans = ~datasets["order_items"]["order_id"].isin(valid_orders)
        if orphans.any():
            logger.warning("%d ligne(s) de commande orpheline(s) écartée(s)", int(orphans.sum()))
            datasets["order_items"] = datasets["order_items"][~orphans].reset_index(drop=True)

        for name, df in datasets.items():
            cp.save_frame(f"validated_{name}", df)
        return {"overall_status": report["overall_status"], "tables": summary, "report": str(report_path)}

    # -- 5 : SCD2 de la dimension client -------------------------------------------------
    def _stage_transform_customers(self) -> Dict[str, Any]:
        cp = self.checkpoints
        customers = cp.load_frame("validated_customers")
        # Extraction complète = instantané de TOUTE la source : on peut détecter les suppressions.
        # Désactivé si des lignes sont en quarantaine (elles passeraient pour supprimées).
        full_snapshot = cp.stage_metadata("extract_customers").get("watermark_used") is None
        detect_deletes = full_snapshot and not customers.empty and not cp.has_frame("rejected_customers")

        if customers.empty:
            existing = pd.DataFrame()
        elif detect_deletes:
            existing = self.loader.fetch_dimension()  # toute la dimension
        else:
            # Incrémental : on ne relit que les versions des clients du lot.
            existing = self.loader.fetch_dimension(customers["customer_id"].dropna().astype(int).tolist())

        scd2 = self.config.rules.get("scd2", {})
        processor = SCD2Processor(
            business_key=scd2.get("business_key", "customer_id"),
            tracked_columns=scd2.get("tracked_columns", ["name", "email", "status"]),
        )
        to_insert, to_update = processor.apply_scd2_logic(
            customers, existing, effective_date=self.today, detect_deletes=detect_deletes
        )
        cp.save_frame("scd2_to_insert", to_insert)
        cp.save_frame("scd2_to_update", to_update)
        changes = to_insert["change_type"].value_counts().to_dict() if not to_insert.empty else {}
        deleted = int((to_update["change_type"] == "deleted").sum()) if not to_update.empty else 0
        if deleted:
            changes["deleted"] = deleted
        return {
            "incoming_customers": len(customers),
            "versions_to_insert": len(to_insert),
            "versions_to_close": len(to_update),
            "changes": changes,
            "delete_detection": detect_deletes,
        }

    # -- 6 : agrégats (pandas ou Spark) -----------------------------------------------------
    def _stage_transform_orders(self, engine: Optional[str] = None) -> Dict[str, Any]:
        cp = self.checkpoints
        engine = (engine or self.config.order_metrics_engine).lower()
        orders = cp.load_frame("validated_orders")
        items = cp.load_frame("validated_order_items")
        if engine == "spark" and not orders.empty:
            from spark_jobs.order_metrics import compute_order_metrics_with_spark  # import paresseux (lourd)

            compute_order_metrics_with_spark(cp)
            daily, perf = cp.load_frame("fact_daily_orders"), cp.load_frame("fact_product_performance")
        else:
            if engine == "spark":
                logger.info("Aucune commande à agréger : Spark n'est pas démarré")
            daily = build_daily_order_facts(orders)
            perf = build_product_performance(orders, items)
            cp.save_frame("fact_daily_orders", daily)
            cp.save_frame("fact_product_performance", perf)
        days = sorted({str(d) for d in daily["order_date"]}) if not daily.empty else []
        return {
            "engine": engine,
            "fact_daily_orders": len(daily),
            "fact_product_performance": len(perf),
            # Incrémental : seuls les jours présents dans le lot sont recalculés.
            "days_recomputed": len(days),
            "first_day": days[0] if days else None,
            "last_day": days[-1] if days else None,
        }

    # -- 7 : chargement ------------------------------------------------------------------------
    def _stage_load(self) -> Dict[str, Any]:
        cp = self.checkpoints
        batch = LoadBatch(
            products=cp.load_frame("validated_products"),
            orders=cp.load_frame("validated_orders"),
            order_items=cp.load_frame("validated_order_items"),
            customers_to_insert=cp.load_frame("scd2_to_insert"),
            customers_to_update=cp.load_frame("scd2_to_update"),
            daily_order_facts=cp.load_frame("fact_daily_orders"),
            product_performance=cp.load_frame("fact_product_performance"),
        )
        counts = self.loader.load(batch, run_id=self.run_id)

        # Les watermarks n'avancent qu'APRÈS le COMMIT : si le chargement échoue,
        # la prochaine exécution reprendra les mêmes données (rien n'est perdu).
        advanced: Dict[str, str] = {}
        for source in ("customers", "orders", "products"):
            candidate = cp.stage_metadata(f"extract_{source}").get("watermark_candidate")
            if candidate and candidate != self.watermarks.get(source):
                self.watermarks.set(source, candidate, self.run_id)
                advanced[source] = candidate
        return {"rows_written": counts, "watermarks_advanced": advanced}

    # -- 8 : rapport ---------------------------------------------------------------------------
    def _stage_report(self) -> Dict[str, Any]:
        report = self.build_execution_report()
        out_dir = self.config.output_dir / "reports" / self.run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "execution_report.json"
        md_path = out_dir / "quality_report.md"
        json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=json_default), encoding="utf-8")
        md_path.write_text(render_markdown_report(report), encoding="utf-8")
        self.checkpoints.set_run_status("success")
        logger.info("Rapports écrits : %s", out_dir)
        return {
            "report_json": str(json_path),
            "report_markdown": str(md_path),
            "data_quality": (report.get("data_quality") or {}).get("overall_status"),
        }

    def build_execution_report(self) -> Dict[str, Any]:
        stages = self.checkpoints.all_stages()
        meta = {stage: info.get("metadata", {}) for stage, info in stages.items()}
        durations = [m.get("duration_seconds") or 0 for m in meta.values()]
        engine = meta.get("transform_orders", {}).get("engine", self.config.order_metrics_engine)
        return {
            "run_id": self.run_id,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "started_at": self.checkpoints.run_info().get("created_at"),
            "stages": {
                stage: {
                    "status": stages.get(stage, {}).get("status", "non exécutée"),
                    "duration_seconds": meta.get(stage, {}).get("duration_seconds"),
                    "finished_at": stages.get(stage, {}).get("finished_at"),
                }
                for stage in STAGES
            },
            "total_duration_seconds": round(sum(durations), 3),
            "records": {
                "extracted": {
                    source: {
                        "rows": meta.get(f"extract_{source}", {}).get("rows"),
                        "mode": meta.get(f"extract_{source}", {}).get("mode"),
                        "watermark_used": meta.get(f"extract_{source}", {}).get("watermark_used"),
                    }
                    for source in ("customers", "orders", "products")
                },
                "written": meta.get("load", {}).get("rows_written", {}),
            },
            "scd2": meta.get("transform_customers", {}),
            "data_quality": self.checkpoints.load_json("validation_results") or {},
            "lineage": build_lineage(meta, engine),
            "watermarks": self.watermarks.all(),
        }


# =============================================================================
# Fonctions appelées par Airflow (une tâche = une fonction)
# =============================================================================
def run_stage(stage: str, run_id: str, **kwargs: Any) -> Dict[str, Any]:
    """Exécute une seule étape d'une exécution (le run_id Airflow relie les tâches entre elles)."""
    pipeline = DataPipeline(load_config(), run_id=run_id)
    try:
        return pipeline.run_stage(stage, **kwargs)
    finally:
        close_all_pools()


def check_data_sources(config: Optional[PipelineConfig] = None) -> Dict[str, str]:
    """Vérifie que la base et l'API répondent AVANT de lancer les extractions."""
    config = config or load_config()
    status: Dict[str, str] = {}
    unavailable = []
    try:
        ping(config.database)
        status["database"] = "ok"
    except Exception as exc:  # noqa: BLE001 - on veut un diagnostic, pas un plantage brutal
        status["database"] = f"injoignable : {exc}"
        unavailable.append("database")
    finally:
        close_all_pools()
    try:
        api = APIExtractor(config.api_endpoint, config.api_key, timeout=10, max_retries=1)
        status["api"] = "ok" if api.check_health() else "répond en erreur"
    except requests.RequestException as exc:
        status["api"] = f"injoignable : {exc}"
    if status["api"] != "ok":
        unavailable.append("api")
    status["products_file"] = "présent" if config.products_file.exists() else "absent (le FileSensor l'attendra)"
    logger.info("Vérification des sources : %s", status)
    if unavailable:
        raise RuntimeError(f"Sources indisponibles : {unavailable} ({status})")
    return status


def cleanup_old_checkpoints(config: Optional[PipelineConfig] = None) -> Dict[str, Any]:
    """Supprime les dossiers de checkpoints plus anciens que CHECKPOINT_RETENTION_DAYS."""
    config = config or load_config()
    removed = CheckpointManager.cleanup(config.checkpoint_dir, config.checkpoint_retention_days)
    return {"removed": removed, "retention_days": config.checkpoint_retention_days}


# =============================================================================
# Point d'entrée demandé par le code de départ du cours
# =============================================================================
def run_data_pipeline(
    run_id: Optional[str] = None,
    *,
    resume: bool = False,
    fresh: bool = False,
    full_refresh: bool = False,
    engine: Optional[str] = None,
    from_stage: Optional[str] = None,
    config: Optional[PipelineConfig] = None,
) -> Dict[str, Any]:
    """Exécute le pipeline complet : extraction, validation, transformations, chargement, rapport."""
    config = config or load_config()
    if resume and not run_id:
        run_id = CheckpointManager.find_latest_incomplete(config.checkpoint_dir, prefix=f"{CLI_PREFIX}_")
        if run_id:
            logger.info("Reprise de l'exécution inachevée %s", run_id)
        else:
            logger.info("Aucune exécution inachevée : démarrage d'une nouvelle exécution")
    pipeline = DataPipeline(config, run_id=run_id, full_refresh=full_refresh)
    if fresh:
        pipeline.checkpoints.reset()
    return pipeline.run(from_stage=from_stage, engine=engine)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline de données automatisé (clients, commandes, produits)")
    parser.add_argument("--run-id", help="identifiant d'exécution (pour reprendre une exécution précise)")
    parser.add_argument("--resume", action="store_true", help="reprend la dernière exécution inachevée")
    parser.add_argument("--fresh", action="store_true", help="ignore les checkpoints de cette exécution")
    parser.add_argument("--full-refresh", action="store_true", help="ignore les watermarks (rechargement complet)")
    parser.add_argument("--from-stage", choices=STAGES, help="rejoue à partir de cette étape")
    parser.add_argument("--engine", choices=("pandas", "spark"), help="moteur de calcul des agrégats")
    args = parser.parse_args(argv)

    config = load_config()
    setup_logging(config.log_level, config.log_file)
    try:
        result = run_data_pipeline(
            args.run_id, resume=args.resume, fresh=args.fresh, full_refresh=args.full_refresh,
            engine=args.engine, from_stage=args.from_stage, config=config,
        )
    except DataQualityError as exc:
        logger.error("Pipeline arrêté par la porte qualité : %s", exc)
        logger.error(
            "Les données extraites de cette exécution sont figées dans son checkpoint : relancer "
            "la même exécution échouerait à nouveau. Corrige les données à la source, puis lance "
            "une NOUVELLE exécution (python pipeline.py) : le watermark n'ayant pas avancé, "
            "les mêmes enregistrements seront ré-extraits."
        )
        return 2
    except Exception:
        logger.exception("Le pipeline a échoué. Corrige la cause puis relance avec : python pipeline.py --resume")
        return 1
    finally:
        close_all_pools()
    print(f"\nRapport qualité : {result.get('report_markdown')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
