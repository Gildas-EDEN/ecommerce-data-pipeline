"""
loaders.py : chargement dans l'entrepôt de données PostgreSQL.

Règle d'or : un chargement est ATOMIQUE. Toutes les écritures d'une exécution
(tables brutes, dimension SCD2, tables de faits) se font dans UNE SEULE transaction :
  - tout réussit        -> COMMIT   : les nouvelles données apparaissent d'un coup ;
  - une écriture échoue -> ROLLBACK : la base revient exactement à son état
                                      d'avant, rien n'est écrit à moitié.

Les écritures sont aussi IDEMPOTENTES (UPSERT = INSERT ... ON CONFLICT DO UPDATE) :
rejouer deux fois le même chargement ne crée aucun doublon. Et elles sont
INCRÉMENTALES : seules les lignes de l'exécution sont écrites, on ne recharge
jamais une table entière.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, ContextManager, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

from config import DatabaseConfig
from db import pooled_connection
from utils import to_python

logger = logging.getLogger(__name__)

ConnectionProvider = Callable[[], ContextManager[Any]]

DIMENSION_COLUMNS = [
    "dim_customer_key", "customer_id", "name", "email", "status",
    "effective_start_date", "effective_end_date", "is_current",
]

# Tables chargées par UPSERT : (colonnes écrites, clé d'unicité, colonne horodatée à rafraîchir)
UPSERT_TABLES: Dict[str, Tuple[List[str], List[str], str]] = {
    "raw_products": (["product_id", "name", "category", "price", "supplier"], ["product_id"], "loaded_at"),
    "raw_orders": (["order_id", "customer_id", "order_date", "total_amount"], ["order_id"], "loaded_at"),
    "fact_daily_orders": (
        ["order_date", "customer_id", "total_sales", "order_count", "avg_order_value", "unique_orders"],
        ["order_date", "customer_id"],
        "updated_at",
    ),
    "fact_product_performance": (
        ["order_date", "product_id", "units_sold", "revenue", "order_count"],
        ["order_date", "product_id"],
        "updated_at",
    ),
}


class LoadError(RuntimeError):
    """Chargement impossible ou incohérent (la transaction est annulée)."""


def _empty() -> pd.DataFrame:
    return pd.DataFrame()


@dataclass
class LoadBatch:
    """Tout ce qu'une exécution doit écrire dans l'entrepôt."""

    products: pd.DataFrame = field(default_factory=_empty)
    orders: pd.DataFrame = field(default_factory=_empty)
    order_items: pd.DataFrame = field(default_factory=_empty)
    customers_to_insert: pd.DataFrame = field(default_factory=_empty)
    customers_to_update: pd.DataFrame = field(default_factory=_empty)
    daily_order_facts: pd.DataFrame = field(default_factory=_empty)
    product_performance: pd.DataFrame = field(default_factory=_empty)


def _to_rows(df: pd.DataFrame, columns: Sequence[str], extra: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    """DataFrame -> liste de tuples Python natifs (psycopg2 ne sait pas écrire les types numpy)."""
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise LoadError(f"Colonnes manquantes pour le chargement : {missing}")
    return [
        tuple(to_python(value) for value in row) + tuple(extra)
        for row in df[list(columns)].itertuples(index=False, name=None)
    ]


class PostgresLoader:
    """Écrit les résultats d'une exécution dans PostgreSQL, en une seule transaction."""

    def __init__(
        self,
        config: DatabaseConfig,
        batch_size: int = 1000,
        connection_provider: Optional[ConnectionProvider] = None,
    ) -> None:
        self.config = config
        self.batch_size = batch_size  # nombre de lignes envoyées par requête
        self._connection_provider = connection_provider or (lambda: pooled_connection(config))

    # -- lecture de la dimension (nécessaire au SCD2) --------------------------
    def fetch_dimension(self, customer_ids: Optional[Sequence[int]] = None) -> pd.DataFrame:
        """Lit les versions de la dimension client : toutes, ou celles des clients indiqués.

        En mode incrémental, on ne lit QUE les clients présents dans le lot :
        inutile de charger toute la dimension pour en comparer une petite partie.
        """
        query = f"SELECT {', '.join(DIMENSION_COLUMNS)} FROM dim_customers"
        params: Optional[Tuple[Any, ...]] = None
        if customer_ids is not None:
            ids = sorted({int(customer_id) for customer_id in customer_ids})
            if not ids:
                return pd.DataFrame(columns=DIMENSION_COLUMNS)
            query += " WHERE customer_id = ANY(%s)"
            params = (ids,)
        query += " ORDER BY customer_id, effective_start_date"
        with self._connection_provider() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=DIMENSION_COLUMNS)

    # -- chargement atomique ---------------------------------------------------
    def load(self, batch: LoadBatch, run_id: str) -> Dict[str, int]:
        """Écrit tout le lot dans une transaction unique (COMMIT ou ROLLBACK)."""
        counts: Dict[str, int] = {}
        with self._connection_provider() as conn:
            try:
                with conn.cursor() as cur:
                    counts["raw_products"] = self._upsert(cur, "raw_products", batch.products, run_id)
                    counts["raw_orders"] = self._upsert(cur, "raw_orders", batch.orders, run_id)
                    counts["raw_order_items"] = self._replace_order_items(cur, batch.orders, batch.order_items, run_id)
                    # SCD2 : d'abord FERMER les anciennes versions, ensuite INSÉRER les
                    # nouvelles (l'index « une seule version courante par client » l'exige).
                    counts["dim_customers_closed"] = self._close_versions(cur, batch.customers_to_update)
                    counts["dim_customers_inserted"] = self._insert_versions(cur, batch.customers_to_insert, run_id)
                    counts["fact_daily_orders"] = self._upsert(cur, "fact_daily_orders", batch.daily_order_facts, run_id)
                    counts["fact_product_performance"] = self._upsert(
                        cur, "fact_product_performance", batch.product_performance, run_id
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                logger.error("Chargement annulé (ROLLBACK) : l'entrepôt est revenu à son état précédent")
                raise
        logger.info("Chargement validé (COMMIT) : %s", counts)
        return counts

    def _upsert(self, cur: Any, table: str, df: pd.DataFrame, run_id: str) -> int:
        """INSERT ... ON CONFLICT DO UPDATE : insère les nouvelles lignes, met à jour les existantes."""
        columns, conflict_keys, timestamp_column = UPSERT_TABLES[table]
        if df is None or df.empty:
            return 0
        df = df.drop_duplicates(subset=conflict_keys, keep="last")
        insert_columns = [*columns, "run_id"]
        rows = _to_rows(df, columns, extra=(run_id,))
        assignments = [
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
            for col in insert_columns if col not in conflict_keys
        ]
        assignments.append(sql.SQL("{col} = NOW()").format(col=sql.Identifier(timestamp_column)))
        # Identifiants (noms de tables/colonnes) assemblés avec psycopg2.sql : jamais de f-string.
        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES %s ON CONFLICT ({keys}) DO UPDATE SET {updates}").format(
            table=sql.Identifier(table),
            columns=sql.SQL(", ").join(map(sql.Identifier, insert_columns)),
            keys=sql.SQL(", ").join(map(sql.Identifier, conflict_keys)),
            updates=sql.SQL(", ").join(assignments),
        )
        # execute_values envoie les lignes par paquets de batch_size (beaucoup plus rapide
        # qu'une requête par ligne).
        execute_values(cur, query, rows, page_size=self.batch_size)
        logger.info("%-26s : %d ligne(s) écrite(s) (upsert)", table, len(rows))
        return len(rows)

    def _replace_order_items(self, cur: Any, orders: pd.DataFrame, items: pd.DataFrame, run_id: str) -> int:
        """Remplace les lignes des commandes rechargées (une commande corrigée peut avoir perdu une ligne)."""
        if orders is None or orders.empty:
            return 0
        order_ids = sorted({str(order_id) for order_id in orders["order_id"].dropna()})
        cur.execute("DELETE FROM raw_order_items WHERE order_id = ANY(%s)", (order_ids,))
        if items is None or items.empty:
            return 0
        columns = ["order_id", "line_number", "product_id", "quantity", "unit_price"]
        rows = _to_rows(items, columns, extra=(run_id,))
        execute_values(
            cur,
            "INSERT INTO raw_order_items (order_id, line_number, product_id, quantity, unit_price, run_id) VALUES %s",
            rows,
            page_size=self.batch_size,
        )
        logger.info("%-26s : %d ligne(s) écrite(s)", "raw_order_items", len(rows))
        return len(rows)

    def _close_versions(self, cur: Any, to_update: pd.DataFrame) -> int:
        """Ferme les versions SCD2 (date de fin + is_current = FALSE)."""
        if to_update is None or to_update.empty:
            return 0
        rows = [
            (int(key), to_python(end_date))
            for key, end_date in zip(to_update["dim_customer_key"], to_update["effective_end_date"])
        ]
        closed = execute_values(
            cur,
            """
            UPDATE dim_customers AS d
               SET effective_end_date = v.end_date, is_current = FALSE, updated_at = NOW()
              FROM (VALUES %s) AS v (dim_customer_key, end_date)
             WHERE d.dim_customer_key = v.dim_customer_key
               AND d.is_current
            RETURNING d.dim_customer_key
            """,
            rows,
            template="(%s, %s::date)",
            page_size=self.batch_size,
            fetch=True,
        )
        # Contrôle de cohérence : chaque version à fermer doit encore être courante.
        if len(closed) != len(rows):
            raise LoadError(
                f"{len(rows) - len(closed)} version(s) n'étaient plus courantes : la dimension a changé "
                "pendant le traitement. Relancer l'exécution."
            )
        logger.info("%-26s : %d version(s) fermée(s)", "dim_customers", len(closed))
        return len(closed)

    def _insert_versions(self, cur: Any, to_insert: pd.DataFrame, run_id: str) -> int:
        """Insère les nouvelles versions SCD2."""
        if to_insert is None or to_insert.empty:
            return 0
        columns = ["customer_id", "name", "email", "status", "effective_start_date", "effective_end_date", "is_current"]
        rows = _to_rows(to_insert, columns, extra=(run_id,))
        execute_values(
            cur,
            "INSERT INTO dim_customers (customer_id, name, email, status, effective_start_date, "
            "effective_end_date, is_current, run_id) VALUES %s",
            rows,
            page_size=self.batch_size,
        )
        logger.info("%-26s : %d version(s) insérée(s)", "dim_customers", len(rows))
        return len(rows)
