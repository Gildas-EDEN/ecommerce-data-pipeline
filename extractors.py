"""
extractors.py : connecteurs vers les trois sources de données.

  - DatabaseExtractor : base clients PostgreSQL (extraction incrémentale, par lots)
  - APIExtractor      : API REST des commandes (authentification, pagination,
                        relances avec attente exponentielle, limitation de débit)
  - FileExtractor     : catalogue produits CSV (contrôle du schéma, réparation
                        du format mal formé fourni par le cours)

Chaque connecteur renvoie une structure standard (DataFrame pandas ou liste de
dictionnaires), journalise ce qu'il fait et lève une ExtractionError explicite
en cas d'échec définitif.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from pathlib import Path
from typing import Any, Callable, ContextManager, Dict, List, Optional, Tuple, Union

import pandas as pd
import psycopg2
import requests

from config import DatabaseConfig
from db import pooled_connection
from utils import compute_backoff, parse_datetime_series, retry_call, to_date_series, to_int_series

logger = logging.getLogger(__name__)

ConnectionProvider = Callable[[], ContextManager[Any]]


class ExtractionError(RuntimeError):
    """Échec définitif d'une extraction (après épuisement des relances)."""


# =============================================================================
# 1. Base de données clients (PostgreSQL)
# =============================================================================
class DatabaseExtractor:
    """Extrait les clients de la base opérationnelle PostgreSQL."""

    # Colonnes indispensables : si l'une disparaît de la source, on s'arrête
    # (changement de schéma « cassant »). Une colonne EN PLUS est tolérée.
    REQUIRED_COLUMNS: Tuple[str, ...] = (
        "customer_id", "name", "email", "registration_date", "status", "last_updated",
    )

    def __init__(
        self,
        config: DatabaseConfig,
        batch_size: int = 1000,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        connection_provider: Optional[ConnectionProvider] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        # Par défaut : connexion empruntée au pool partagé (connexions réutilisées).
        # Les tests injectent ici une fausse connexion (mock).
        self._connection_provider = connection_provider or (lambda: pooled_connection(config))
        self._sleep = sleep

    def extract_customers(self, last_updated: Optional[str] = None) -> pd.DataFrame:
        """Extrait les clients.

        Si `last_updated` (le « watermark ») est fourni, seuls les clients
        modifiés APRÈS cette date sont extraits : c'est l'extraction incrémentale.
        Sans watermark (premier passage), on extrait toute la table.
        """
        # Requête PARAMÉTRÉE : la valeur (%s) est envoyée séparément du texte SQL.
        # Impossible d'injecter du SQL, contrairement à un f-string
        # f"... WHERE last_updated > '{last_updated}'" (solution du cours).
        query = "SELECT * FROM customers"
        params: Optional[List[Any]] = None
        if last_updated:
            query += " WHERE last_updated > %s"
            params = [last_updated]
        query += " ORDER BY last_updated, customer_id"

        mode = f"incrémentale (last_updated > {last_updated})" if last_updated else "complète"
        logger.info("Extraction des clients : %s", mode)

        try:
            raw = retry_call(
                lambda: self._fetch_in_batches(query, params),
                retries=self.max_retries,
                backoff_factor=self.backoff_factor,
                # Erreurs transitoires (réseau, base redémarrée...) : on relance.
                retry_on=(psycopg2.OperationalError, psycopg2.InterfaceError),
                description="Extraction des clients",
                logger=logger,
                sleep=self._sleep,
            )
        except psycopg2.Error as exc:
            raise ExtractionError(f"Impossible d'extraire les clients depuis PostgreSQL : {exc}") from exc

        customers = self._enforce_schema(raw)
        logger.info("Clients extraits : %d ligne(s)", len(customers))
        return customers

    def _fetch_in_batches(self, query: str, params: Optional[List[Any]]) -> pd.DataFrame:
        """Lit le résultat par lots de `batch_size` lignes (mémoire maîtrisée)."""
        chunks: List[pd.DataFrame] = []
        columns: Optional[List[str]] = None
        with self._connection_provider() as conn:
            # Curseur « serveur » (curseur nommé) : PostgreSQL envoie les lignes
            # par paquets au lieu de tout charger en mémoire d'un seul coup.
            with conn.cursor(name="extract_customers") as cur:
                cur.itersize = self.batch_size
                cur.execute(query, params)
                while True:
                    rows = cur.fetchmany(self.batch_size)
                    if columns is None and cur.description:
                        columns = [desc[0] for desc in cur.description]
                    if not rows:
                        break
                    chunks.append(pd.DataFrame(rows, columns=columns))
                    logger.debug("Lot de %d client(s) lu", len(rows))
        if not chunks:
            return pd.DataFrame(columns=columns or list(self.REQUIRED_COLUMNS))
        return pd.concat(chunks, ignore_index=True)

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Vérifie les colonnes et impose des types cohérents d'une exécution à l'autre."""
        missing = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ExtractionError(
                f"Colonnes manquantes dans la table customers : {missing}. "
                "Le schéma de la source a changé : adapter l'extraction."
            )
        extra = [col for col in df.columns if col not in self.REQUIRED_COLUMNS]
        if extra:
            logger.warning(
                "Nouvelles colonnes détectées dans customers (conservées mais ignorées "
                "par les transformations) : %s", extra,
            )

        out = df.copy()
        out["customer_id"] = to_int_series(out["customer_id"])
        out["registration_date"] = to_date_series(out["registration_date"])
        out["last_updated"] = parse_datetime_series(out["last_updated"])
        return out


# =============================================================================
# 2. API REST des commandes
# =============================================================================
def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """L'en-tête HTTP Retry-After indique combien de secondes attendre."""
    try:
        return max(0.0, float(value)) if value is not None else None
    except ValueError:
        return None


class APIExtractor:
    """Extrait les commandes depuis l'API REST (format du fichier Sample_orders.json)."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        page_size: int = 100,
        rate_limit_per_sec: float = 5.0,
        jitter_ratio: float = 0.0,
        max_pages: int = 10_000,
        session: Optional[requests.Session] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.page_size = page_size
        self.rate_limit_per_sec = rate_limit_per_sec
        self.jitter_ratio = jitter_ratio
        self.max_pages = max_pages
        # Une Session réutilise la même connexion TCP entre les pages (plus rapide).
        self.session = session or requests.Session()
        # Authentification : la clé part dans un en-tête HTTP, jamais dans l'URL
        # (les URL finissent dans les logs des serveurs et des proxys).
        self.session.headers.update({"Authorization": f"Bearer {api_key}", "Accept": "application/json"})
        self._sleep = sleep
        self._last_request_at: Optional[float] = None
        self.stats: Dict[str, int] = {"requests": 0, "retries": 0, "pages": 0}

    def extract_orders(self, date_from: Optional[str] = None) -> List[Dict[str, Any]]:
        """Extrait toutes les pages de commandes passées à partir de `date_from` (incluse)."""
        orders: List[Dict[str, Any]] = []
        page: Optional[int] = 1
        logger.info("Extraction des commandes (date_from=%s)", date_from or "toutes")

        while page is not None:
            if self.stats["pages"] >= self.max_pages:
                raise ExtractionError(f"Plus de {self.max_pages} pages : pagination anormale, arrêt par sécurité")
            params: Dict[str, Any] = {"page": page, "page_size": self.page_size}
            if date_from:
                params["date_from"] = date_from

            payload = self._get_json(params)
            batch = payload.get("orders")
            if not isinstance(batch, list):
                raise ExtractionError("Réponse inattendue de l'API : la clé 'orders' est absente ou invalide")
            orders.extend(batch)
            self.stats["pages"] += 1
            logger.info("Page %d : %d commande(s)", page, len(batch))

            # Pagination : l'API indique s'il reste des pages (has_more / next_page).
            if payload.get("has_more") and batch:
                page = int(payload.get("next_page") or page + 1)
            else:
                page = None

        logger.info(
            "Commandes extraites : %d (%d page(s), %d relance(s))",
            len(orders), self.stats["pages"], self.stats["retries"],
        )
        return orders

    def check_health(self) -> bool:
        """Vérifie que l'API répond (utilisé avant de lancer le pipeline)."""
        base_url = self.endpoint.rsplit("/", 1)[0]
        response = self.session.get(f"{base_url}/health", timeout=10)
        return response.status_code == 200

    # -- mécanique interne ----------------------------------------------------
    def _throttle(self) -> None:
        """Limitation de débit côté client : au plus `rate_limit_per_sec` requêtes par seconde."""
        if self.rate_limit_per_sec <= 0:
            return
        min_interval = 1.0 / self.rate_limit_per_sec
        now = time.monotonic()
        if self._last_request_at is not None:
            wait = min_interval - (now - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = time.monotonic()

    def _get_json(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Appelle l'API avec relances automatiques sur les erreurs transitoires.

        - erreur réseau, délai dépassé, HTTP 5xx : erreur du serveur, on relance
        - HTTP 429 (trop de requêtes)            : on attend (Retry-After) puis on relance
        - autres HTTP 4xx (401, 404...)          : erreur de NOTRE côté, relancer ne sert à rien
        """
        last_error = ""
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            self.stats["requests"] += 1
            retry_after: Optional[float] = None
            try:
                response = self.session.get(self.endpoint, params=params, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"erreur réseau ({exc.__class__.__name__})"
            else:
                status = response.status_code
                if status == 200:
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        raise ExtractionError("Réponse de l'API illisible (JSON invalide)") from exc
                    if not isinstance(payload, dict):
                        raise ExtractionError("Réponse de l'API inattendue : un objet JSON était attendu")
                    return payload
                if status == 429:
                    last_error = "HTTP 429 (trop de requêtes)"
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                elif 500 <= status < 600:
                    last_error = f"HTTP {status} (erreur serveur)"
                else:
                    raise ExtractionError(
                        f"Erreur HTTP {status} sur {self.endpoint} : vérifier l'URL et la clé API "
                        "(erreur non transitoire, pas de relance)"
                    )

            if attempt == self.max_retries:
                break
            delay = retry_after if retry_after is not None else compute_backoff(
                attempt, self.backoff_factor, self.jitter_ratio
            )
            self.stats["retries"] += 1
            logger.warning(
                "Appel API en échec (%s), tentative %d/%d. Nouvel essai dans %.1f s",
                last_error, attempt, self.max_retries, delay,
            )
            self._sleep(delay)

        raise ExtractionError(
            f"API indisponible après {self.max_retries} tentatives (dernière erreur : {last_error})"
        )


# =============================================================================
# 3. Fichier CSV du catalogue produits
# =============================================================================
class FileExtractor:
    """Lit et contrôle le catalogue produits (CSV)."""

    REQUIRED_COLUMNS: Tuple[str, ...] = ("product_id", "name", "category", "price", "supplier")

    def __init__(self, file_path: Union[str, Path], chunk_size: int = 1000) -> None:
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.repaired = False  # True si le format mal formé a été réparé

    def extract_products(self) -> pd.DataFrame:
        """Lit le CSV par morceaux, contrôle le schéma et convertit les types."""
        if not self.file_path.exists():
            raise ExtractionError(f"Fichier produits introuvable : {self.file_path}")
        logger.info("Lecture du catalogue produits : %s", self.file_path)

        source = self._open_source()
        chunks: List[pd.DataFrame] = []
        try:
            # dtype=str : on lit tout en texte, puis on convertit nous-mêmes en
            # contrôlant les erreurs (une valeur « abc » dans price ne fait pas planter).
            for chunk in pd.read_csv(source, dtype=str, chunksize=self.chunk_size, skipinitialspace=True):
                chunks.append(chunk)
        except (pd.errors.ParserError, UnicodeDecodeError) as exc:
            raise ExtractionError(f"CSV illisible ({self.file_path.name}) : {exc}") from exc

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=list(self.REQUIRED_COLUMNS))
        df.columns = [str(col).strip().lower() for col in df.columns]

        missing = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ExtractionError(f"Colonnes manquantes dans {self.file_path.name} : {missing}")
        extra = [col for col in df.columns if col not in self.REQUIRED_COLUMNS]
        if extra:
            logger.warning("Nouvelles colonnes dans le catalogue (conservées) : %s", extra)

        for col in df.columns:
            if col != "price":
                df[col] = df[col].str.strip()
        price_text = df["price"].str.strip()
        df["price"] = pd.to_numeric(price_text, errors="coerce")
        bad_prices = int((df["price"].isna() & price_text.notna()).sum())
        if bad_prices:
            logger.warning("%d prix illisible(s) remplacé(s) par une valeur manquante", bad_prices)

        logger.info("Produits extraits : %d ligne(s)", len(df))
        return df

    def _open_source(self) -> Union[Path, io.StringIO]:
        """Détecte et répare le format mal formé du fichier fourni par le cours.

        Dans « Product catalog data.csv », chaque ligne entière est entourée de
        guillemets : "P001,Hiking Boots,Footwear,129.99,OutdoorGear Inc".
        Un lecteur CSV y voit UNE seule colonne. On retire ces guillemets
        englobants ligne par ligne avant de relire le fichier normalement.
        """
        with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
            lines = handle.read().splitlines()
        first = next((line for line in lines if line.strip()), "")
        first_fields = next(csv.reader([first]), [])
        if not (len(first_fields) == 1 and "," in first_fields[0]):
            return self.file_path  # format normal : pandas lit directement le fichier

        self.repaired = True
        logger.warning(
            "Format CSV mal formé détecté dans %s (lignes entières entre guillemets) : "
            "réparation automatique", self.file_path.name,
        )
        repaired: List[str] = []
        for line in lines:
            if not line.strip():
                continue
            fields = next(csv.reader([line]), [])
            repaired.append(fields[0] if len(fields) == 1 else line)
        return io.StringIO("\n".join(repaired))
