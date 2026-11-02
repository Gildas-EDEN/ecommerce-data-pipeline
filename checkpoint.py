"""
checkpoint.py : points de reprise (checkpoints) et watermarks.

CHECKPOINT : après chaque étape réussie, son résultat est sauvegardé sur disque
(fichiers Parquet) et l'étape est notée « terminée ». Si le pipeline plante à
l'étape 5, la relance (même run_id) saute les étapes 1 à 4 et recharge leurs
résultats : on ne ré-extrait pas les sources, on repart de là où ça a cassé.

WATERMARK (« niveau des plus hautes eaux ») : pour chaque source, la plus grande
valeur déjà traitée avec succès (ex. max(last_updated) des clients). L'exécution
suivante n'extrait que ce qui est plus récent : c'est le traitement incrémental.

Organisation sur disque :
    data/checkpoints/<run_id>/run.json              statut global de l'exécution
    data/checkpoints/<run_id>/stages/<etape>.json   statut et métadonnées de chaque étape
    data/checkpoints/<run_id>/<jeu>.parquet         données produites par les étapes
    data/state/watermarks.json                      watermarks (partagés entre exécutions)

Un fichier par étape (et non un fichier commun) : sous Airflow, plusieurs tâches
tournent en même temps dans des processus différents ; chacune écrit son propre
fichier, sans risque d'écraser le travail d'une autre.

Toutes les écritures sont ATOMIQUES : on écrit un fichier temporaire puis on le
renomme (os.replace). Un plantage en pleine écriture ne laisse jamais un fichier
à moitié écrit.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from utils import json_default, sanitize_run_id

logger = logging.getLogger(__name__)

# Les extractions tournent dans plusieurs threads : ce verrou évite que deux threads
# lisent / remplacent le même petit fichier JSON au même instant.
_FILE_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _replace_with_retry(source: str, target: Path, attempts: int = 10) -> None:
    """os.replace avec quelques nouvelles tentatives.

    Sous Windows, un fichier ouvert par un autre programme (antivirus, synchronisation
    OneDrive...) ne peut pas être remplacé pendant un court instant : on réessaie.
    """
    for attempt in range(1, attempts + 1):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == attempts:
                raise
            time.sleep(0.05 * attempt)


def _atomic_write_text(path: Path, text: str) -> None:
    """Écrit dans un fichier temporaire puis le renomme : jamais de fichier à moitié écrit."""
    with _FILE_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
            _replace_with_retry(tmp_name, path)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise


def _read_json(path: Path, default: Any) -> Any:
    with _FILE_LOCK:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))


class CheckpointManager:
    """Sauvegarde l'avancement et les données intermédiaires d'une exécution."""

    def __init__(self, base_dir: Path, run_id: str) -> None:
        self.base_dir = Path(base_dir)
        self.run_id = sanitize_run_id(run_id)
        self.run_dir = self.base_dir / self.run_id
        self.stages_dir = self.run_dir / "stages"
        self.stages_dir.mkdir(parents=True, exist_ok=True)
        if not self._run_file().exists():
            self._write_run({"run_id": self.run_id, "status": "created", "created_at": _now()})

    # -- statut global de l'exécution -------------------------------------------
    def _run_file(self) -> Path:
        return self.run_dir / "run.json"

    def _write_run(self, info: Dict[str, Any]) -> None:
        info["updated_at"] = _now()
        _atomic_write_text(self._run_file(), json.dumps(info, indent=2, ensure_ascii=False))

    def run_info(self) -> Dict[str, Any]:
        return _read_json(self._run_file(), {})

    def set_run_status(self, status: str) -> None:
        info = self.run_info()
        info["status"] = status
        self._write_run(info)

    # -- statut des étapes ----------------------------------------------------------
    def _stage_file(self, stage: str) -> Path:
        return self.stages_dir / f"{stage}.json"

    def stage_info(self, stage: str) -> Dict[str, Any]:
        # Relu depuis le disque à chaque appel : une autre tâche Airflow a pu l'écrire.
        return _read_json(self._stage_file(stage), {})

    def all_stages(self) -> Dict[str, Dict[str, Any]]:
        return {path.stem: _read_json(path, {}) for path in sorted(self.stages_dir.glob("*.json"))}

    def is_done(self, stage: str) -> bool:
        return self.stage_info(stage).get("status") == "done"

    def stage_metadata(self, stage: str) -> Dict[str, Any]:
        return self.stage_info(stage).get("metadata", {})

    def mark_running(self, stage: str) -> None:
        self._write_stage(stage, {"status": "running", "started_at": _now()})

    def mark_done(self, stage: str, metadata: Dict[str, Any]) -> None:
        info = self.stage_info(stage)
        info.update({"status": "done", "finished_at": _now(), "metadata": metadata})
        info.pop("error", None)
        self._write_stage(stage, info)

    def mark_failed(self, stage: str, error: str) -> None:
        info = self.stage_info(stage)
        info.update({"status": "failed", "failed_at": _now(), "error": error})
        self._write_stage(stage, info)

    def invalidate(self, stages: Iterable[str]) -> None:
        """Oublie des étapes terminées (pour les rejouer avec --from-stage)."""
        for stage in stages:
            self._stage_file(stage).unlink(missing_ok=True)

    def _write_stage(self, stage: str, info: Dict[str, Any]) -> None:
        _atomic_write_text(self._stage_file(stage), json.dumps(info, indent=2, ensure_ascii=False, default=json_default))

    # -- données intermédiaires (Parquet) ------------------------------------------
    def frame_path(self, name: str) -> Path:
        return self.run_dir / f"{name}.parquet"

    def has_frame(self, name: str) -> bool:
        return self.frame_path(name).exists()

    def save_frame(self, name: str, df: pd.DataFrame) -> Path:
        """Sauvegarde un DataFrame en Parquet (format colonne, compressé, qui conserve les types)."""
        path = self.frame_path(name)
        tmp = path.with_name(f".{path.name}.tmp")
        # Horodatages en microsecondes : format lisible par Spark (pas les nanosecondes de pandas).
        df.to_parquet(tmp, index=False, engine="pyarrow", coerce_timestamps="us", allow_truncated_timestamps=True)
        if path.is_dir():  # ancienne sortie Spark (un dossier) à remplacer
            shutil.rmtree(path)
        _replace_with_retry(str(tmp), path)
        return path

    def load_frame(self, name: str) -> pd.DataFrame:
        path = self.frame_path(name)
        if not path.exists():
            raise FileNotFoundError(
                f"Checkpoint '{name}' introuvable pour l'exécution {self.run_id} : "
                "l'étape qui le produit n'a pas encore été exécutée"
            )
        return pd.read_parquet(path)  # fonctionne pour un fichier (pandas) comme pour un dossier (Spark)

    def save_json(self, name: str, payload: Any) -> Path:
        path = self.run_dir / f"{name}.json"
        _atomic_write_text(path, json.dumps(payload, indent=2, ensure_ascii=False, default=json_default))
        return path

    def has_json(self, name: str) -> bool:
        return (self.run_dir / f"{name}.json").exists()

    def load_json(self, name: str) -> Any:
        return _read_json(self.run_dir / f"{name}.json", None)

    def reset(self) -> None:
        """Repart de zéro pour cette exécution (option --fresh)."""
        shutil.rmtree(self.run_dir, ignore_errors=True)
        self.stages_dir.mkdir(parents=True, exist_ok=True)
        self._write_run({"run_id": self.run_id, "status": "created", "created_at": _now()})

    # -- outils sur l'ensemble des exécutions ---------------------------------------
    @staticmethod
    def find_latest_incomplete(base_dir: Path, prefix: Optional[str] = None) -> Optional[str]:
        """Exécution la plus récente qui ne s'est pas terminée avec succès (pour --resume).

        `prefix` restreint la recherche (ex. « cli_ » : seulement les exécutions lancées
        en ligne de commande ; celles d'Airflow se reprennent depuis son interface).
        """
        candidates = []
        for run_file in Path(base_dir).glob("*/run.json"):
            if prefix and not run_file.parent.name.startswith(prefix):
                continue
            info = _read_json(run_file, {})
            if info.get("status") != "success":
                candidates.append((run_file.stat().st_mtime, info.get("run_id", run_file.parent.name)))
        return max(candidates)[1] if candidates else None

    @staticmethod
    def cleanup(base_dir: Path, retention_days: int, now: Optional[datetime] = None) -> List[str]:
        """Supprime les checkpoints des exécutions plus anciennes que retention_days."""
        base = Path(base_dir)
        if not base.exists():
            return []
        limit = (now or datetime.now()) - timedelta(days=retention_days)
        removed = []
        for run_dir in base.iterdir():
            if not run_dir.is_dir():
                continue
            marker = run_dir / "run.json"
            last_activity = datetime.fromtimestamp((marker if marker.exists() else run_dir).stat().st_mtime)
            if last_activity < limit:
                shutil.rmtree(run_dir, ignore_errors=True)
                removed.append(run_dir.name)
        if removed:
            logger.info("%d dossier(s) de checkpoints supprimé(s) (plus de %d jours)", len(removed), retention_days)
        return removed


class WatermarkStore:
    """Mémorise, pour chaque source, la dernière valeur traitée avec succès."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def all(self) -> Dict[str, Dict[str, Any]]:
        return _read_json(self.path, {})

    def get(self, source: str) -> Optional[str]:
        return self.all().get(source, {}).get("value")

    def set(self, source: str, value: str, run_id: str) -> None:
        data = self.all()
        data[source] = {"value": value, "run_id": run_id, "updated_at": _now()}
        _atomic_write_text(self.path, json.dumps(data, indent=2, ensure_ascii=False))
        logger.info("Watermark '%s' avancé à %s", source, value)

    def reset(self, source: Optional[str] = None) -> None:
        """Efface un watermark (ou tous) : la prochaine extraction sera complète."""
        if source is None:
            self.path.unlink(missing_ok=True)
            return
        data = self.all()
        data.pop(source, None)
        _atomic_write_text(self.path, json.dumps(data, indent=2, ensure_ascii=False))
