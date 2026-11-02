"""
db.py : connexions PostgreSQL réutilisables (pool de connexions).

Ouvrir une connexion PostgreSQL coûte cher (réseau, authentification,
création d'un processus côté serveur). Un POOL garde quelques connexions
ouvertes et les prête aux morceaux de code qui en ont besoin : on les
réutilise au lieu d'en recréer une à chaque requête.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Dict, Iterator, Tuple

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extensions import connection as PgConnection

from config import DatabaseConfig

logger = logging.getLogger(__name__)

_pools: Dict[Tuple[str, int, str, str], pg_pool.ThreadedConnectionPool] = {}
_pools_lock = threading.Lock()


def get_pool(config: DatabaseConfig, minconn: int = 1, maxconn: int = 5) -> pg_pool.ThreadedConnectionPool:
    """Renvoie le pool associé à une base (créé au premier appel)."""
    key = (config.host, config.port, config.database, config.username)
    with _pools_lock:
        existing = _pools.get(key)
        if existing is None or existing.closed:
            # ThreadedConnectionPool : utilisable depuis plusieurs threads à la fois
            # (nos extractions tournent en parallèle).
            existing = pg_pool.ThreadedConnectionPool(minconn, maxconn, **config.connection_kwargs())
            _pools[key] = existing
            logger.debug("Pool PostgreSQL créé pour %s:%s/%s", config.host, config.port, config.database)
        return existing


@contextmanager
def pooled_connection(config: DatabaseConfig) -> Iterator[PgConnection]:
    """Emprunte une connexion au pool et la rend automatiquement à la fin du bloc `with`."""
    connection_pool = get_pool(config)
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        # Une connexion rendue au pool ne doit jamais garder une transaction ouverte.
        if not conn.closed:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        connection_pool.putconn(conn, close=bool(conn.closed))


def ping(config: DatabaseConfig) -> None:
    """Vérifie que la base répond (lève une exception sinon)."""
    with pooled_connection(config) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()


def close_all_pools() -> None:
    """Ferme proprement toutes les connexions (fin d'exécution)."""
    with _pools_lock:
        for connection_pool in _pools.values():
            if not connection_pool.closed:
                connection_pool.closeall()
        _pools.clear()
