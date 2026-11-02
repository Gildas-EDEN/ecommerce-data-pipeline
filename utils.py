"""
utils.py : petites fonctions transverses utilisées par tous les modules.

  - setup_logging : configuration des logs (console + fichier)
  - retry_call / compute_backoff : relances automatiques avec attente exponentielle
  - new_run_id / sanitize_run_id : identifiants d'exécution
  - to_date_series / to_int_series : conversions de types robustes
  - send_alert : notification de succès ou d'échec (log, fichier, webhook)
"""

from __future__ import annotations

import hashlib
import json
import logging
import logging.handlers
import os
import random
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar

import pandas as pd

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-12s | %(message)s"

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Configure les logs une seule fois, au lancement en ligne de commande.

    Sous Airflow, on ne l'appelle PAS : Airflow capture lui-même les logs de
    chaque tâche et les affiche dans son interface.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())
    for handler in list(root.handlers):  # évite les doublons si appelée deux fois
        root.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_file:
        from config import resolve_path  # import local : évite une dépendance circulaire

        path = resolve_path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Fichier tournant : 5 Mo max, 3 anciens fichiers conservés.
        file_handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Relances (retry) avec attente exponentielle
# ---------------------------------------------------------------------------
def compute_backoff(attempt: int, backoff_factor: float, jitter_ratio: float = 0.0) -> float:
    """Délai d'attente avant la tentative suivante : facteur x 2^(tentative-1).

    Avec un facteur de 1 : 1 s, 2 s, 4 s, 8 s... On laisse ainsi au service le
    temps de se rétablir au lieu de le bombarder. Le « jitter » ajoute un petit
    aléa pour que plusieurs clients ne relancent pas tous au même instant.
    """
    delay = backoff_factor * (2 ** (attempt - 1))
    if jitter_ratio > 0:
        delay += random.uniform(0, delay * jitter_ratio)
    return delay


def retry_call(
    func: Callable[[], T],
    *,
    retries: int,
    backoff_factor: float,
    retry_on: Tuple[Type[BaseException], ...],
    description: str,
    logger: logging.Logger,
    sleep: Callable[[float], None] = time.sleep,
    jitter_ratio: float = 0.0,
) -> T:
    """Exécute func() et la relance si elle lève une erreur TRANSITOIRE (retry_on).

    Les autres erreurs (bug, données invalides...) remontent immédiatement :
    relancer ne servirait à rien.
    """
    for attempt in range(1, retries + 1):
        try:
            return func()
        except retry_on as exc:
            if attempt == retries:
                logger.error("%s : échec définitif après %d tentative(s)", description, retries)
                raise
            delay = compute_backoff(attempt, backoff_factor, jitter_ratio)
            logger.warning(
                "%s : échec (tentative %d/%d) : %s. Nouvel essai dans %.1f s",
                description, attempt, retries, exc, delay,
            )
            sleep(delay)
    raise RuntimeError("retry_call : nombre de tentatives invalide")  # pragma: no cover


# ---------------------------------------------------------------------------
# Identifiants d'exécution
# ---------------------------------------------------------------------------
def new_run_id(prefix: str = "manual") -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%dT%H%M%S')}"


def file_sha256(path: Path) -> str:
    """Empreinte SHA-256 d'un fichier : change dès qu'un seul octet du fichier change."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sanitize_run_id(run_id: str) -> str:
    """Rend un run_id utilisable comme nom de dossier.

    Les run_id Airflow ressemblent à « manual__2026-10-06T10:00:00+00:00 » :
    les caractères « : » et « + » sont interdits dans un nom de dossier Windows.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "-", (run_id or "").strip())
    if not cleaned:
        raise ValueError("run_id vide")
    return cleaned


# ---------------------------------------------------------------------------
# Conversions de types robustes
# ---------------------------------------------------------------------------
def parse_datetime_series(series: pd.Series) -> pd.Series:
    """Convertit en datetime64 ; une valeur illisible devient NaT (manquante).

    On ne plante pas sur une date mal formée : elle devient « manquante » et
    c'est la validation qui décidera ensuite si c'est bloquant.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    return pd.to_datetime(series, errors="coerce", format="mixed")


def to_date_series(series: pd.Series) -> pd.Series:
    """Convertit en dates Python (datetime.date), None si absente ou invalide.

    On stocke les dates « pures » (sans heure) en datetime.date : elles sont
    écrites en type DATE dans Parquet et dans PostgreSQL, et Spark les lit sans
    problème de fuseau horaire.
    """
    parsed = parse_datetime_series(series)
    values = [None if pd.isna(ts) else ts.date() for ts in parsed]
    return pd.Series(values, index=series.index, dtype=object)


def to_int_series(series: pd.Series) -> pd.Series:
    """Convertit en entiers nullables (Int64). « 2.5 » ou « abc » deviennent manquants."""
    numeric = pd.to_numeric(series, errors="coerce")
    numeric = numeric.where(numeric.isna() | (numeric == numeric.round()))
    return numeric.astype("Int64")


def to_python(value: Any) -> Any:
    """Convertit une valeur pandas/numpy en type Python natif (pour psycopg2 et JSON)."""
    if value is None:
        return None
    if isinstance(value, (list, dict, tuple)):
        return value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, date)):
        return value.item()  # numpy.int64 -> int, numpy.float64 -> float
    return value


def json_default(value: Any) -> Any:
    """Sérialiseur JSON pour les dates et les types numpy."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    converted = to_python(value)
    if converted is value:
        return str(value)
    return converted


# ---------------------------------------------------------------------------
# Alertes
# ---------------------------------------------------------------------------
def send_alert(level: str, subject: str, details: Dict[str, Any]) -> Path:
    """Envoie une notification de succès / d'échec.

    1. Toujours : écrit un log et un fichier JSON dans data/output/alerts/.
    2. Si ALERT_WEBHOOK_URL est défini (Slack, Teams, Discord...) : envoie aussi
       un message HTTP. Une alerte qui échoue ne doit jamais faire échouer le
       pipeline : les erreurs d'envoi sont seulement journalisées.
    """
    from config import resolve_path  # import local : évite une dépendance circulaire

    logger = logging.getLogger("alerts")
    payload = {"level": level, "subject": subject, "sent_at": datetime.now().isoformat(), **details}
    log_method = logger.error if level == "error" else logger.info
    log_method("[ALERTE %s] %s | %s", level.upper(), subject, json.dumps(details, default=json_default))

    alerts_dir = resolve_path(os.getenv("OUTPUT_PATH", "./data/output")) / "alerts"
    alerts_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{datetime.now().strftime('%Y%m%dT%H%M%S%f')}_{level}.json"
    path = alerts_dir / file_name
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=json_default), encoding="utf-8")

    webhook = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if webhook:
        try:
            import requests

            requests.post(webhook, json={"text": f"[{level.upper()}] {subject}"}, timeout=10)
        except Exception as exc:  # noqa: BLE001 - une alerte ne doit jamais casser le pipeline
            logger.warning("Envoi du webhook d'alerte impossible : %s", exc)
    return path
