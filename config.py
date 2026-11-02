"""
config.py : gestion centralisée de la configuration du pipeline.

Deux sources de configuration, volontairement séparées :

1. Les PARAMÈTRES D'ENVIRONNEMENT (hôte de la base, mots de passe, URL de l'API,
   chemins...) changent d'une machine à l'autre (ton PC, Docker, la production).
   Ils sont lus dans les variables d'environnement, elles-mêmes éventuellement
   chargées depuis le fichier `.env`. Ce fichier contient des secrets : on ne le
   versionne jamais, on versionne seulement le modèle `.env.example`.

2. Les RÈGLES MÉTIER (colonnes obligatoires, colonnes historisées en SCD2,
   seuils de qualité...) sont les mêmes partout. Elles vivent dans
   `config/pipeline.yaml` et se modifient sans toucher au code Python.

Ordre de priorité pour une même variable :
    variable déjà définie (Docker, Airflow)  >  fichier .env  >  valeur par défaut
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from dotenv import load_dotenv

# Racine du projet : tous les chemins relatifs sont résolus à partir d'ici, et
# non depuis le répertoire courant (qui change selon qu'on lance le pipeline à
# la main, depuis pytest ou depuis une tâche Airflow).
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RULES_FILE = PROJECT_ROOT / "config" / "pipeline.yaml"
VALID_ENGINES = ("pandas", "spark")

PathLike = Union[str, Path]


class ConfigError(ValueError):
    """Configuration invalide ou incomplète (le message dit quoi corriger)."""


# ---------------------------------------------------------------------------
# Lecture typée des variables d'environnement
# ---------------------------------------------------------------------------
def _get_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else value.strip()


def _get_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} doit être un nombre entier (valeur reçue : {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} doit être supérieur ou égal à {minimum} (valeur reçue : {value})")
    return value


def _get_float(name: str, default: float, minimum: Optional[float] = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} doit être un nombre (valeur reçue : {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} doit être supérieur ou égal à {minimum} (valeur reçue : {value})")
    return value


def resolve_path(path: PathLike) -> Path:
    """Chemin relatif (ex. ./data/input) -> chemin absolu depuis la racine du projet."""
    candidate = Path(path)
    return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()


# ---------------------------------------------------------------------------
# Objets de configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DatabaseConfig:
    """Paramètres de connexion PostgreSQL."""

    host: str
    port: int
    database: str
    username: str
    # repr=False : le mot de passe n'apparaît jamais dans un print() ou un log.
    password: str = field(repr=False)

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        return cls(
            host=_get_str("DB_HOST", "localhost"),
            port=_get_int("DB_PORT", 5432, minimum=1),
            database=_get_str("DB_NAME", "analytics"),
            username=_get_str("DB_USER", "pipeline_user"),
            password=os.getenv("DB_PASSWORD", ""),
        )

    def connection_kwargs(self) -> Dict[str, Any]:
        """Paramètres attendus par psycopg2.connect()."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "connect_timeout": 10,  # on échoue vite si la base est injoignable
        }


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration complète du pipeline."""

    # --- Champs imposés par le code de départ du cours ---
    database: DatabaseConfig
    api_endpoint: str
    api_key: str = field(repr=False)
    input_path: str
    output_path: str
    batch_size: int = 1000
    # --- Ajouts : pilotage fin du pipeline ---
    state_path: str = "./data/state"
    checkpoint_path: str = "./data/checkpoints"
    log_level: str = "INFO"
    log_file: str = "./logs/pipeline.log"
    products_file_name: str = "products.csv"
    api_timeout: int = 30
    api_max_retries: int = 3
    api_backoff_factor: float = 1.0
    api_jitter_ratio: float = 0.1
    api_page_size: int = 100
    api_rate_limit_per_sec: float = 5.0
    db_max_retries: int = 3
    order_metrics_engine: str = "pandas"
    checkpoint_retention_days: int = 7
    rules: Dict[str, Any] = field(default_factory=dict, repr=False)

    # Raccourcis : chemins absolus prêts à l'emploi
    @property
    def input_dir(self) -> Path:
        return resolve_path(self.input_path)

    @property
    def output_dir(self) -> Path:
        return resolve_path(self.output_path)

    @property
    def state_dir(self) -> Path:
        return resolve_path(self.state_path)

    @property
    def checkpoint_dir(self) -> Path:
        return resolve_path(self.checkpoint_path)

    @property
    def products_file(self) -> Path:
        return self.input_dir / self.products_file_name


def load_rules(path: Optional[PathLike] = None) -> Dict[str, Any]:
    """Charge les règles métier depuis le fichier YAML."""
    rules_path = Path(path or os.getenv("PIPELINE_RULES_FILE") or DEFAULT_RULES_FILE)
    rules_path = resolve_path(rules_path)
    if not rules_path.exists():
        raise ConfigError(f"Fichier de règles introuvable : {rules_path}")
    with rules_path.open(encoding="utf-8") as handle:
        rules = yaml.safe_load(handle) or {}
    if not isinstance(rules, dict):
        raise ConfigError(f"{rules_path} doit contenir un dictionnaire YAML")
    return rules


def load_config(env_file: Optional[PathLike] = None) -> PipelineConfig:
    """Charge la configuration complète : variables d'environnement + règles YAML."""
    env_path = Path(env_file) if env_file else PROJECT_ROOT / ".env"
    # override=False : une variable déjà définie (par Docker ou Airflow) reste prioritaire.
    load_dotenv(env_path, override=False)

    engine = _get_str("ORDER_METRICS_ENGINE", "pandas").lower()
    if engine not in VALID_ENGINES:
        raise ConfigError(f"ORDER_METRICS_ENGINE doit valoir {VALID_ENGINES} (reçu : {engine!r})")

    log_level = _get_str("LOG_LEVEL", "INFO").upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError(f"LOG_LEVEL invalide : {log_level!r}")

    return PipelineConfig(
        database=DatabaseConfig.from_env(),
        api_endpoint=_get_str("API_ENDPOINT", "http://localhost:8000/orders"),
        api_key=os.getenv("API_KEY", ""),
        input_path=_get_str("INPUT_PATH", "./data/input"),
        output_path=_get_str("OUTPUT_PATH", "./data/output"),
        batch_size=_get_int("BATCH_SIZE", 1000, minimum=1),
        state_path=_get_str("STATE_PATH", "./data/state"),
        checkpoint_path=_get_str("CHECKPOINT_PATH", "./data/checkpoints"),
        log_level=log_level,
        log_file=os.getenv("LOG_FILE", "./logs/pipeline.log").strip(),
        products_file_name=_get_str("PRODUCTS_FILE_NAME", "products.csv"),
        api_timeout=_get_int("API_TIMEOUT", 30, minimum=1),
        api_max_retries=_get_int("API_MAX_RETRIES", 3, minimum=1),
        api_backoff_factor=_get_float("API_BACKOFF_FACTOR", 1.0, minimum=0.0),
        api_jitter_ratio=_get_float("API_JITTER_RATIO", 0.1, minimum=0.0),
        api_page_size=_get_int("API_PAGE_SIZE", 100, minimum=1),
        api_rate_limit_per_sec=_get_float("API_RATE_LIMIT_PER_SEC", 5.0, minimum=0.0),
        db_max_retries=_get_int("DB_MAX_RETRIES", 3, minimum=1),
        order_metrics_engine=engine,
        checkpoint_retention_days=_get_int("CHECKPOINT_RETENTION_DAYS", 7, minimum=1),
        rules=load_rules(),
    )
