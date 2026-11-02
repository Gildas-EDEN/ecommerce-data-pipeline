"""
transformers.py : logique de transformation et contrôles de qualité.

Les fonctions de ce module sont « pures » : elles ne dépendent que de leurs
paramètres et n'écrivent nulle part (ni base, ni fichier). Avantages : elles se
testent facilement et servent aussi bien en ligne de commande que sous Airflow.

Contenu :
  - standardize_customers / flatten_orders / standardize_products : nettoyage
  - build_daily_order_facts / build_product_performance : agrégats (moteur pandas)
  - SCD2Processor : historisation de la dimension client (SCD type 2)
  - DataValidator : contrôles de qualité et « porte qualité » (quality gate)
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

from utils import parse_datetime_series, to_date_series, to_int_series, to_python

logger = logging.getLogger(__name__)

END_OF_TIME = date(9999, 12, 31)
PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "total_amount"]
ITEM_COLUMNS = ["order_id", "line_number", "product_id", "quantity", "unit_price"]
DAILY_FACT_COLUMNS = ["order_date", "customer_id", "total_sales", "order_count", "avg_order_value", "unique_orders"]
PRODUCT_PERF_COLUMNS = ["order_date", "product_id", "units_sold", "revenue", "order_count"]


# =============================================================================
# 1. Nettoyage et standardisation
# =============================================================================
def _clean_text(series: pd.Series) -> pd.Series:
    """Supprime les espaces superflus ; une chaîne vide devient une valeur manquante."""
    cleaned = series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return cleaned.mask(cleaned.eq("").fillna(False))


def _keep_latest(df: pd.DataFrame, key: Union[str, List[str]], order_by: Optional[str] = None) -> pd.DataFrame:
    """Un même identifiant présent plusieurs fois : on ne garde que sa version la plus récente.

    Les lignes SANS identifiant sont conservées telles quelles : c'est la
    validation qui les signalera (drop_duplicates les fusionnerait entre elles
    et masquerait le problème).
    """
    if df.empty:
        return df.reset_index(drop=True)
    keys = [key] if isinstance(key, str) else list(key)
    has_key = df[keys].notna().all(axis=1)
    keyed = df[has_key]
    if order_by and order_by in df.columns:
        keyed = keyed.sort_values(order_by, kind="stable", na_position="first")
    deduped = keyed.drop_duplicates(subset=keys, keep="last")
    removed = len(keyed) - len(deduped)
    if removed:
        logger.warning("%d doublon(s) sur %s supprimé(s) (version la plus récente conservée)", removed, keys)
    parts = [part for part in (deduped, df[~has_key]) if not part.empty]
    return pd.concat(parts).sort_index().reset_index(drop=True)


def standardize_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les clients : espaces, casse des e-mails et des statuts, doublons."""
    out = df.copy()
    out["name"] = _clean_text(out["name"])
    out["email"] = _clean_text(out["email"]).str.lower()   # les e-mails ne sont pas sensibles à la casse
    out["status"] = _clean_text(out["status"]).str.lower()
    return _keep_latest(out, key="customer_id", order_by="last_updated")


def standardize_products(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie le catalogue produits et supprime les doublons de product_id."""
    out = df.copy()
    for column in ("product_id", "name", "category", "supplier"):
        out[column] = _clean_text(out[column])
    return _keep_latest(out, key="product_id")


def flatten_orders(records: Iterable[Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Aplatit le JSON imbriqué de l'API en deux tables relationnelles.

    Une commande JSON contient une liste « items » : on la sépare en
      - orders      : une ligne par commande (en-tête)
      - order_items : une ligne par produit commandé (numérotée line_number)
    """
    header_rows: List[Dict[str, Any]] = []
    item_rows: List[Dict[str, Any]] = []
    malformed = 0
    for record in records:
        if not isinstance(record, dict):
            malformed += 1
            continue
        header_rows.append({column: record.get(column) for column in ORDER_COLUMNS})
        items = record.get("items")
        if not isinstance(items, list):
            items = []
        for line_number, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                malformed += 1
                continue
            item_rows.append({
                "order_id": record.get("order_id"),
                "line_number": line_number,
                "product_id": item.get("product_id"),
                "quantity": item.get("quantity"),
                "unit_price": item.get("unit_price"),
            })
    if malformed:
        logger.warning("%d élément(s) JSON mal formé(s) ignoré(s)", malformed)

    orders = pd.DataFrame(header_rows, columns=ORDER_COLUMNS)
    items_df = pd.DataFrame(item_rows, columns=ITEM_COLUMNS)

    # Typage explicite : une valeur illisible devient « manquante » et sera
    # signalée par la validation (au lieu de faire planter le pipeline ici).
    orders["order_id"] = _clean_text(orders["order_id"])
    orders["customer_id"] = to_int_series(orders["customer_id"])
    orders["order_date"] = to_date_series(orders["order_date"])
    orders["total_amount"] = pd.to_numeric(orders["total_amount"], errors="coerce").astype("float64")
    items_df["order_id"] = _clean_text(items_df["order_id"])
    items_df["line_number"] = to_int_series(items_df["line_number"])
    items_df["product_id"] = _clean_text(items_df["product_id"])
    items_df["quantity"] = to_int_series(items_df["quantity"])
    items_df["unit_price"] = pd.to_numeric(items_df["unit_price"], errors="coerce").astype("float64")

    # Une même commande reçue deux fois (pages qui se chevauchent...) : on garde la dernière.
    orders = _keep_latest(orders, key="order_id")
    items_df = _keep_latest(items_df, key=["order_id", "line_number"])
    return orders, items_df


# =============================================================================
# 2. Agrégats (tables de faits) : version pandas
#    La version Spark équivalente est dans spark_jobs/order_metrics.py
# =============================================================================
def build_daily_order_facts(orders: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par (jour, client) : chiffre d'affaires, nombre et panier moyen."""
    if orders.empty:
        return pd.DataFrame(columns=DAILY_FACT_COLUMNS)
    facts = orders.groupby(["order_date", "customer_id"], as_index=False).agg(
        total_sales=("total_amount", "sum"),
        order_count=("order_id", "count"),
        avg_order_value=("total_amount", "mean"),
        unique_orders=("order_id", "nunique"),
    )
    facts["total_sales"] = facts["total_sales"].round(2)
    facts["avg_order_value"] = facts["avg_order_value"].round(2)
    return facts[DAILY_FACT_COLUMNS].sort_values(["order_date", "customer_id"]).reset_index(drop=True)


def build_product_performance(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par (jour, produit) : unités vendues, chiffre d'affaires, nombre de commandes."""
    if orders.empty or items.empty:
        return pd.DataFrame(columns=PRODUCT_PERF_COLUMNS)
    lines = items.merge(orders[["order_id", "order_date"]], on="order_id", how="inner")
    lines["line_revenue"] = lines["quantity"].astype("float64") * lines["unit_price"]
    perf = lines.groupby(["order_date", "product_id"], as_index=False).agg(
        units_sold=("quantity", "sum"),
        revenue=("line_revenue", "sum"),
        order_count=("order_id", "nunique"),
    )
    perf["revenue"] = perf["revenue"].round(2)
    perf["units_sold"] = perf["units_sold"].astype("int64")
    return perf[PRODUCT_PERF_COLUMNS].sort_values(["order_date", "product_id"]).reset_index(drop=True)


# =============================================================================
# 3. SCD2 : historisation de la dimension client
# =============================================================================
def _as_date(value: Any) -> Optional[date]:
    """Convertit une valeur quelconque (date, datetime, texte) en date, None si absente."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):  # couvre aussi pd.Timestamp
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


class SCD2Processor:
    """Slowly Changing Dimension de type 2 sur les clients.

    Principe : on ne modifie JAMAIS une information passée. Quand un attribut
    suivi change (nom, e-mail, statut), on « ferme » la version courante
    (date de fin renseignée, is_current = False) et on insère une NOUVELLE
    version. La table conserve ainsi tout l'historique de chaque client.

    Convention des dates : intervalle semi-ouvert [effective_start_date ; effective_end_date[
      - une version est valide le jour J si  start <= J < end ;
      - la version fermée se termine le jour même où la suivante commence :
        ni trou, ni chevauchement dans l'historique ;
      - une version encore valide a pour date de fin 9999-12-31.
    """

    def __init__(
        self,
        connection: Any = None,
        business_key: str = "customer_id",
        tracked_columns: Sequence[str] = ("name", "email", "status"),
        end_of_time: date = END_OF_TIME,
    ) -> None:
        # `connection` est conservé pour rester compatible avec le code de départ.
        # La logique reste « pure » : la dimension existante est passée en
        # paramètre, ce qui permet de la tester sans base de données.
        self.connection = connection
        self.business_key = business_key
        self.tracked_columns = list(tracked_columns)
        self.end_of_time = end_of_time

    def apply_scd2_logic(
        self,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
        effective_date: Optional[date] = None,
        detect_deletes: bool = False,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Compare les clients entrants avec la dimension existante.

        Paramètres
          new_data       : clients extraits de la source (déjà nettoyés)
          existing_data  : versions existantes de ces clients dans dim_customers
                           (courantes ou non ; DataFrame vide au premier chargement)
          effective_date : date de traitement (défaut : aujourd'hui), utilisée quand
                           la source ne fournit pas de date de modification
          detect_deletes : True uniquement si new_data est un instantané COMPLET :
                           un client courant absent de la source est alors « supprimé »

        Retour : (records_to_insert, records_to_update)
          records_to_insert : nouvelles versions (change_type = new / changed / reactivated)
          records_to_update : versions à fermer   (change_type = closed / deleted)
        """
        processing_date = _as_date(effective_date) or date.today()
        key = self.business_key
        incoming = self._prepare_incoming(new_data, processing_date)
        existing = self._prepare_existing(existing_data)
        current = existing[existing["is_current"]]

        # Date de fin de la dernière version connue de chaque client (pour les réactivations).
        last_end = existing.groupby(key)["effective_end_date"].max()

        merged = incoming.merge(
            current[[key, "dim_customer_key", "_row_hash", "effective_start_date"]].rename(
                columns={"_row_hash": "_current_hash", "effective_start_date": "_current_start"}
            ),
            on=key,
            how="left",
        )
        has_current = merged["dim_customer_key"].notna()
        has_history = merged[key].isin(last_end.index)

        # Les 4 cas possibles pour un client entrant :
        is_changed = has_current & (merged["_row_hash"] != merged["_current_hash"])  # attribut modifié
        is_reactivated = ~has_current & has_history     # avait été fermé (supprimé), revient
        is_new = ~has_current & ~has_history            # jamais vu
        # (has_current et même empreinte : rien n'a changé, on ne fait rien)

        start = pd.Series(None, index=merged.index, dtype=object)
        # Nouveau client : sa 1re version commence à sa date d'inscription
        # (hypothèse du chargement initial : aucun historique antérieur connu).
        start[is_new] = [
            reg if reg is not None else chg
            for reg, chg in zip(merged.loc[is_new, "registration_date"], merged.loc[is_new, "_change_date"])
        ]
        # Client modifié : la nouvelle version commence à la date du changement,
        # jamais avant le début de la version courante.
        start[is_changed] = [
            max(chg, _as_date(cur))
            for chg, cur in zip(merged.loc[is_changed, "_change_date"], merged.loc[is_changed, "_current_start"])
        ]
        # Client réactivé : sa nouvelle version commence après la fin de la précédente.
        start[is_reactivated] = [
            max(chg, _as_date(last_end[k]))
            for chg, k in zip(merged.loc[is_reactivated, "_change_date"], merged.loc[is_reactivated, key])
        ]

        to_insert_mask = is_new | is_changed | is_reactivated
        inserts = merged.loc[to_insert_mask, [key, *self.tracked_columns]].copy()
        inserts["effective_start_date"] = start[to_insert_mask]
        inserts["effective_end_date"] = self.end_of_time
        inserts["is_current"] = True
        inserts["change_type"] = np.select(
            [is_new[to_insert_mask], is_changed[to_insert_mask], is_reactivated[to_insert_mask]],
            ["new", "changed", "reactivated"],
            default="new",
        )
        records_to_insert = inserts.reset_index(drop=True)

        update_parts: List[pd.DataFrame] = []
        if is_changed.any():
            closes = merged[is_changed]
            update_parts.append(pd.DataFrame({
                "dim_customer_key": closes["dim_customer_key"].astype("int64").to_numpy(),
                key: closes[key].to_numpy(),
                # l'ancienne version se termine le jour où la nouvelle commence
                "effective_end_date": start[is_changed].to_numpy(),
                "is_current": False,
                "change_type": "closed",
            }))
        if detect_deletes and not current.empty:
            gone = current[~current[key].isin(incoming[key])]
            if not gone.empty:
                update_parts.append(pd.DataFrame({
                    "dim_customer_key": gone["dim_customer_key"].astype("int64").to_numpy(),
                    key: gone[key].to_numpy(),
                    "effective_end_date": [max(processing_date, _as_date(s)) for s in gone["effective_start_date"]],
                    "is_current": False,
                    "change_type": "deleted",
                }))
        update_columns = ["dim_customer_key", key, "effective_end_date", "is_current", "change_type"]
        records_to_update = (
            pd.concat(update_parts, ignore_index=True) if update_parts else pd.DataFrame(columns=update_columns)
        )

        summary = records_to_insert["change_type"].value_counts().to_dict()
        logger.info(
            "SCD2 : %d client(s) reçu(s) -> %d nouveau(x), %d modifié(s), %d réactivé(s), %d inchangé(s), %d supprimé(s)",
            len(incoming), summary.get("new", 0), summary.get("changed", 0), summary.get("reactivated", 0),
            int((has_current & ~is_changed).sum()),
            int((records_to_update["change_type"] == "deleted").sum()) if not records_to_update.empty else 0,
        )
        return records_to_insert, records_to_update

    # -- préparation des entrées ---------------------------------------------
    def compute_row_hash(self, df: pd.DataFrame) -> pd.Series:
        """Empreinte MD5 des colonnes suivies : deux lignes identiques ont la même empreinte.

        Comparer une empreinte plutôt que colonne par colonne évite le piège des
        valeurs manquantes (en pandas NaN != NaN, ce qui créerait de faux changements).
        """
        if df.empty:
            return pd.Series(index=df.index, dtype=object)
        parts = [df[column].astype("string").fillna("").str.strip() for column in self.tracked_columns]
        joined = parts[0] if len(parts) == 1 else parts[0].str.cat(parts[1:], sep="\x1f")
        return joined.map(lambda text: hashlib.md5(str(text).encode("utf-8")).hexdigest()).astype(object)

    def _prepare_incoming(self, new_data: pd.DataFrame, processing_date: date) -> pd.DataFrame:
        key = self.business_key
        missing = [col for col in (key, *self.tracked_columns) if col not in new_data.columns]
        if missing:
            raise ValueError(f"Colonnes manquantes pour le SCD2 : {missing}")
        df = new_data.copy()
        df[key] = to_int_series(df[key])
        df = df[df[key].notna()]

        # Date du changement : date de modification dans la source (last_updated)
        # si elle est connue, sinon date de traitement.
        if "last_updated" in df.columns:
            changed_at = parse_datetime_series(df["last_updated"])
            df = df.assign(_changed_at=changed_at).sort_values("_changed_at", kind="stable", na_position="first")
            df["_change_date"] = [processing_date if pd.isna(ts) else ts.date() for ts in df["_changed_at"]]
        else:
            df["_change_date"] = [processing_date] * len(df)
        if "registration_date" in df.columns:
            df["registration_date"] = to_date_series(df["registration_date"])
        else:
            df["registration_date"] = pd.Series([None] * len(df), index=df.index, dtype=object)

        # Si un client apparaît plusieurs fois, seule sa version la plus récente compte.
        df = df.drop_duplicates(subset=[key], keep="last")
        df["_row_hash"] = self.compute_row_hash(df)
        return df.reset_index(drop=True)

    def _prepare_existing(self, existing_data: Optional[pd.DataFrame]) -> pd.DataFrame:
        key = self.business_key
        columns = ["dim_customer_key", key, *self.tracked_columns,
                   "effective_start_date", "effective_end_date", "is_current"]
        if existing_data is None or existing_data.empty:
            empty = pd.DataFrame({col: pd.Series(dtype=object) for col in columns})
            empty[key] = pd.Series(dtype="Int64")
            empty["dim_customer_key"] = pd.Series(dtype="Int64")
            empty["is_current"] = pd.Series(dtype=bool)
            empty["_row_hash"] = pd.Series(dtype=object)
            return empty
        missing = [col for col in columns if col not in existing_data.columns]
        if missing:
            raise ValueError(f"Colonnes manquantes dans la dimension existante : {missing}")
        df = existing_data.copy()
        df[key] = to_int_series(df[key])
        df["dim_customer_key"] = to_int_series(df["dim_customer_key"])
        df["effective_start_date"] = to_date_series(df["effective_start_date"])
        df["effective_end_date"] = to_date_series(df["effective_end_date"])
        df["is_current"] = df["is_current"].fillna(False).astype(bool)
        df["_row_hash"] = self.compute_row_hash(df)
        return df


# =============================================================================
# 4. Qualité des données
# =============================================================================
STRUCTURAL_CHECKS = ("schema", "data_types")  # erreurs qui empêchent toute quarantaine


class DataQualityError(RuntimeError):
    """Les données ne passent pas la porte qualité : le pipeline s'arrête."""

    def __init__(self, message: str, report: Dict[str, Any]) -> None:
        super().__init__(message)
        self.report = report


def _bool(mask: pd.Series) -> pd.Series:
    """Masque booléen sans valeur manquante (NA -> False)."""
    return mask.fillna(False).astype(bool)


def _matches_type(series: pd.Series, expected: str) -> bool:
    """Vérifie qu'une colonne a bien le type attendu."""
    non_null = series.dropna()
    if non_null.empty:
        return True
    if expected == "integer":
        if pd.api.types.is_integer_dtype(series):
            return True
        return pd.api.types.is_numeric_dtype(series) and bool((non_null == non_null.round()).all())
    if expected == "number":
        return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)
    if expected == "datetime":
        return pd.api.types.is_datetime64_any_dtype(series)
    if expected == "date":
        return pd.api.types.is_datetime64_any_dtype(series) or all(isinstance(v, date) for v in non_null)
    if expected == "string":
        return pd.api.types.is_string_dtype(series) or all(isinstance(v, str) for v in non_null)
    raise ValueError(f"Type inconnu dans les règles de validation : {expected!r}")


class DataValidator:
    """Contrôles de qualité, table par table, pilotés par config/pipeline.yaml.

    Chaque contrôle renvoie PASS (ok), WARN (à surveiller, non bloquant) ou
    FAIL (problème). Décision finale pour une table :
      - erreur de structure (colonne manquante, mauvais type)        -> FAIL : arrêt
      - lignes invalides au-delà du seuil max_invalid_ratio           -> FAIL : arrêt
      - lignes invalides sous le seuil                                -> WARN : quarantaine
      - aucun problème bloquant                                       -> PASS ou WARN : on continue
    """

    def __init__(self, rules: Optional[Dict[str, Any]] = None, sample_size: int = 5) -> None:
        self.rules = rules or {}
        self.sample_size = sample_size
        self.validation_results: Dict[str, Dict[str, Any]] = {}
        self._invalid_masks: Dict[str, pd.Series] = {}

    def validate_data_quality(self, data: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Applique toutes les règles d'une table et renvoie un rapport détaillé."""
        rules = self.rules.get(table_name, {})
        checks: Dict[str, Dict[str, Any]] = {}
        invalid = pd.Series(False, index=data.index)
        key_cols = self._key_columns(rules, data)

        # 1. Schéma : colonnes obligatoires présentes ? colonnes inattendues ?
        required = list(rules.get("required", []))
        known = set(required) | set(rules.get("optional", []))
        missing_cols = [col for col in required if col not in data.columns]
        unknown_cols = [col for col in data.columns if known and col not in known]
        checks["schema"] = {
            "status": FAIL if missing_cols else (WARN if unknown_cols else PASS),
            "details": {"missing_columns": missing_cols, "unknown_columns": unknown_cols},
        }

        # 2. Volume : une extraction incrémentale vide est normale mais on la signale.
        checks["row_count"] = {"status": WARN if data.empty else PASS, "details": f"{len(data)} ligne(s)"}

        # 3. Valeurs manquantes dans les colonnes OBLIGATOIRES (contrôle ligne par ligne)
        present_required = [col for col in required if col in data.columns]
        if present_required:
            null_mask = data[present_required].isna()
            row_mask = null_mask.any(axis=1)
            details = {col: int(count) for col, count in null_mask.sum().items() if count}
            checks["required_not_null"] = self._row_check(row_mask, data, key_cols, details)
            invalid |= _bool(row_mask)

        # 4. Valeurs manquantes dans les autres colonnes (information seulement)
        other_cols = [col for col in data.columns if col not in required]
        other_nulls = {col: int(n) for col, n in data[other_cols].isna().sum().items() if n} if other_cols else {}
        checks["optional_nulls"] = {"status": WARN if other_nulls else PASS, "details": other_nulls}

        # 5. Types de données
        type_issues = [
            f"{col} : type attendu {expected}, obtenu {data[col].dtype}"
            for col, expected in rules.get("types", {}).items()
            if col in data.columns and not _matches_type(data[col], expected)
        ]
        checks["data_types"] = {"status": FAIL if type_issues else PASS, "details": type_issues}

        # 6. Unicité des identifiants
        for spec in rules.get("unique", []):
            cols = [spec] if isinstance(spec, str) else list(spec)
            if not all(col in data.columns for col in cols):
                continue
            dup_mask = data.duplicated(subset=cols, keep=False) & data[cols].notna().all(axis=1)
            checks["unique_" + "_".join(cols)] = self._row_check(
                dup_mask, data, key_cols, f"{int(dup_mask.sum())} ligne(s) en doublon sur {cols}"
            )
            invalid |= _bool(dup_mask)

        # 7. Format des e-mails
        for col in rules.get("email", []):
            if col not in data.columns:
                continue
            matches = _bool(data[col].astype("string").str.match(EMAIL_PATTERN))
            bad = data[col].notna() & ~matches
            checks[f"email_format_{col}"] = self._row_check(bad, data, key_cols, f"{int(bad.sum())} e-mail(s) invalide(s)")
            invalid |= _bool(bad)

        # 8. Valeurs autorisées (liste fermée)
        for col, allowed in rules.get("allowed_values", {}).items():
            if col not in data.columns:
                continue
            bad = data[col].notna() & ~_bool(data[col].isin(allowed))
            checks[f"allowed_values_{col}"] = self._row_check(
                bad, data, key_cols, f"{int(bad.sum())} valeur(s) hors de {allowed}"
            )
            invalid |= _bool(bad)

        # 9. Bornes numériques (règles métier : un prix est > 0, un montant >= 0)
        for rule_name, predicate in (("positive", lambda s: s <= 0), ("non_negative", lambda s: s < 0)):
            for col in rules.get(rule_name, []):
                if col not in data.columns:
                    continue
                numeric = pd.to_numeric(data[col], errors="coerce")
                bad = _bool(predicate(numeric))
                checks[f"{rule_name}_{col}"] = self._row_check(
                    bad, data, key_cols, f"{int(bad.sum())} valeur(s) hors bornes"
                )
                invalid |= bad

        # Décision finale pour la table
        invalid = _bool(invalid)
        invalid_count = int(invalid.sum())
        total = len(data)
        ratio = invalid_count / total if total else 0.0
        max_ratio = float(rules.get("max_invalid_ratio", 0.0))
        structural_failure = any(checks[name]["status"] == FAIL for name in STRUCTURAL_CHECKS if name in checks)

        if structural_failure or (invalid_count and ratio > max_ratio):
            overall, decision = FAIL, "stop"
        elif invalid_count:
            overall, decision = WARN, "quarantine"
        else:
            overall = WARN if any(check["status"] == WARN for check in checks.values()) else PASS
            decision = "continue"

        result = {
            "table_name": table_name,
            "total_records": total,
            "invalid_records": invalid_count,
            "invalid_ratio": round(ratio, 4),
            "max_invalid_ratio": max_ratio,
            "validation_timestamp": datetime.now().isoformat(timespec="seconds"),
            "checks": checks,
            "overall_status": overall,
            "decision": decision,
        }
        self.validation_results[table_name] = result
        self._invalid_masks[table_name] = invalid

        log = logger.error if overall == FAIL else (logger.warning if overall == WARN else logger.info)
        log("Qualité %-12s : %s (%d ligne(s), %d invalide(s), décision : %s)",
            table_name, overall, total, invalid_count, decision)
        return result

    def check_order_totals(self, orders: pd.DataFrame, items: pd.DataFrame, tolerance: float = 0.01) -> Dict[str, Any]:
        """Règle métier : total de la commande = somme(quantité x prix unitaire) de ses lignes."""
        if orders.empty:
            check: Dict[str, Any] = {"status": PASS, "details": "aucune commande"}
        else:
            line_totals = (items["quantity"].astype("float64") * items["unit_price"]).groupby(items["order_id"]).sum()
            comparison = orders.set_index("order_id")[["total_amount"]].join(
                line_totals.rename("items_total"), how="left"
            )
            no_items = comparison["items_total"].isna()
            mismatched = _bool((comparison["total_amount"] - comparison["items_total"]).abs() > tolerance)
            check = {
                "status": WARN if (mismatched.any() or no_items.any()) else PASS,
                "details": {
                    "orders_without_items": int(no_items.sum()),
                    "orders_with_total_mismatch": int(mismatched.sum()),
                    "tolerance": tolerance,
                },
            }
            if mismatched.any():
                sample = comparison[mismatched].head(self.sample_size).reset_index()
                check["sample"] = [
                    {k: to_python(v) for k, v in row.items()} for row in sample.to_dict("records")
                ]
        if "orders" in self.validation_results:
            result = self.validation_results["orders"]
            result["checks"]["order_total_consistency"] = check
            if check["status"] == WARN and result["overall_status"] == PASS:
                result["overall_status"] = WARN
        return check

    def split_valid_invalid(self, data: pd.DataFrame, table_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Sépare les lignes valides des lignes à mettre en quarantaine."""
        mask = self._invalid_masks.get(table_name)
        if mask is None:
            raise KeyError(f"La table {table_name} n'a pas encore été validée")
        mask = mask.reindex(data.index, fill_value=False)
        return data[~mask].reset_index(drop=True), data[mask].reset_index(drop=True)

    def build_report(self) -> Dict[str, Any]:
        """Rapport global : le pire statut de toutes les tables."""
        statuses = [result["overall_status"] for result in self.validation_results.values()]
        overall = FAIL if FAIL in statuses else (WARN if WARN in statuses else PASS)
        return {
            "overall_status": overall,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "tables": self.validation_results,
        }

    # -- outils internes -------------------------------------------------------
    @staticmethod
    def _key_columns(rules: Dict[str, Any], data: pd.DataFrame) -> List[str]:
        """Colonnes qui identifient une ligne (pour citer des exemples dans le rapport)."""
        for spec in rules.get("unique", []):
            cols = [spec] if isinstance(spec, str) else list(spec)
            if all(col in data.columns for col in cols):
                return cols
        required = [col for col in rules.get("required", []) if col in data.columns]
        return required[:1]

    def _row_check(self, mask: pd.Series, data: pd.DataFrame, key_cols: List[str], details: Any) -> Dict[str, Any]:
        mask = _bool(mask)
        count = int(mask.sum())
        check: Dict[str, Any] = {"status": FAIL if count else PASS, "failed_rows": count, "details": details}
        if count:
            rows = data.loc[mask]
            # Exemples de lignes fautives : numéro de ligne + identifiant (rapport « détaillé »)
            check["sample"] = [
                {"row_number": int(position), **{col: to_python(rows.at[position, col]) for col in key_cols}}
                for position in rows.index[: self.sample_size]
            ]
        return check
