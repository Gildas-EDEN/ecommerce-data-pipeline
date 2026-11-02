"""
Tests de l'historisation SCD2.
  Test Case 3 : 3 nouveaux clients -> 3 nouvelles versions avec les bonnes dates d'effet
  Test Case 4 : un client change d'e-mail -> ancienne version fermée, nouvelle version créée
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from transformers import END_OF_TIME, SCD2Processor, standardize_customers

TODAY = date(2026, 10, 6)


def existing_dimension() -> pd.DataFrame:
    """Dimension déjà chargée : Alice et Bob, une version courante chacun."""
    return pd.DataFrame([
        {"dim_customer_key": 1, "customer_id": 1001, "name": "Alice Johnson", "email": "alice.johnson@email.com",
         "status": "active", "effective_start_date": date(2023, 1, 15), "effective_end_date": END_OF_TIME,
         "is_current": True},
        {"dim_customer_key": 2, "customer_id": 1002, "name": "Bob Smith", "email": "bob.smith@email.com",
         "status": "active", "effective_start_date": date(2023, 2, 20), "effective_end_date": END_OF_TIME,
         "is_current": True},
    ])


def customers(rows: list) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["customer_id", "name", "email", "registration_date", "status", "last_updated"])


# =============================================================================
# Test Case 3 : nouveaux clients
# =============================================================================
def test_tc3_three_new_customers_create_three_versions() -> None:
    incoming = customers([
        (1006, "Frank Miller", "frank@email.com", "2026-10-01", "active", datetime(2026, 10, 1, 9)),
        (1007, "Grace Lee", "grace@email.com", "2026-10-02", "active", datetime(2026, 10, 2, 9)),
        (1008, "Henry Taylor", "henry@email.com", "2026-10-03", "active", datetime(2026, 10, 3, 9)),
    ])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY)

    assert len(to_insert) == 3 and to_update.empty
    assert set(to_insert["customer_id"]) == {1006, 1007, 1008}
    assert (to_insert["change_type"] == "new").all()
    assert to_insert["is_current"].all()
    assert (to_insert["effective_end_date"] == END_OF_TIME).all()
    # Date d'effet d'un nouveau client = sa date d'inscription
    starts = dict(zip(to_insert["customer_id"], to_insert["effective_start_date"]))
    assert starts == {1006: date(2026, 10, 1), 1007: date(2026, 10, 2), 1008: date(2026, 10, 3)}


def test_tc3_first_load_with_empty_dimension(sample_customers: pd.DataFrame) -> None:
    """Premier chargement (dimension vide), appel identique au code de départ du cours."""
    to_insert, to_update = SCD2Processor(None).apply_scd2_logic(sample_customers, pd.DataFrame())
    assert len(to_insert) == 5 and to_update.empty
    alice = to_insert[to_insert["customer_id"] == 1001].iloc[0]
    assert alice["effective_start_date"] == date(2023, 1, 15)


# =============================================================================
# Test Case 4 : client modifié
# =============================================================================
def test_tc4_changed_email_closes_old_version_and_creates_new_one() -> None:
    incoming = customers([
        (1002, "Bob Smith", "bob.smith@newmail.com", "2023-02-20", "active", datetime(2026, 10, 6, 8, 30)),
    ])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY)

    # L'ancienne version est fermée (« end-dated »)...
    assert len(to_update) == 1
    closed = to_update.iloc[0]
    assert closed["dim_customer_key"] == 2
    assert closed["effective_end_date"] == TODAY
    assert not closed["is_current"]
    # ... et une nouvelle version commence le jour même du changement.
    assert len(to_insert) == 1
    new = to_insert.iloc[0]
    assert new["email"] == "bob.smith@newmail.com"
    assert new["effective_start_date"] == TODAY
    assert new["effective_end_date"] == END_OF_TIME
    assert new["is_current"] and new["change_type"] == "changed"
    # Historique continu : la fin de l'ancienne = le début de la nouvelle (ni trou, ni chevauchement)
    assert closed["effective_end_date"] == new["effective_start_date"]


def test_unchanged_customer_produces_nothing() -> None:
    incoming = customers([
        (1001, "Alice Johnson", "alice.johnson@email.com", "2023-01-15", "active", datetime(2026, 10, 6)),
    ])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY)
    assert to_insert.empty and to_update.empty


def test_cleaning_avoids_false_changes() -> None:
    """Espaces et majuscules ne doivent pas créer de fausse nouvelle version."""
    raw = customers([
        (1001, "  Alice   Johnson ", "ALICE.Johnson@Email.com ", "2023-01-15", "Active", datetime(2026, 10, 6)),
    ])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(standardize_customers(raw), existing_dimension())
    assert to_insert.empty and to_update.empty


def test_missing_values_do_not_create_false_changes() -> None:
    """Piège classique : en pandas NaN != NaN. L'empreinte (hash) évite ce faux changement."""
    dim = existing_dimension()
    dim.loc[0, "status"] = None
    incoming = customers([(1001, "Alice Johnson", "alice.johnson@email.com", "2023-01-15", None, datetime(2026, 10, 6))])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, dim, effective_date=TODAY)
    assert to_insert.empty and to_update.empty


def test_duplicates_in_batch_keep_latest_version() -> None:
    incoming = customers([
        (1002, "Bob Smith", "bob.v1@email.com", "2023-02-20", "active", datetime(2026, 10, 5)),
        (1002, "Bob Smith", "bob.v2@email.com", "2023-02-20", "active", datetime(2026, 10, 6)),
    ])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY)
    assert len(to_insert) == 1 and to_insert.iloc[0]["email"] == "bob.v2@email.com"
    assert len(to_update) == 1


def test_reactivated_customer_starts_after_previous_version() -> None:
    """Un client supprimé (aucune version courante) qui revient : nouvelle version, sans chevauchement."""
    dim = existing_dimension()
    dim.loc[1, ["is_current", "effective_end_date"]] = [False, date(2026, 9, 1)]
    incoming = customers([(1002, "Bob Smith", "bob.smith@email.com", "2023-02-20", "active", datetime(2026, 8, 1))])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, dim, effective_date=TODAY)
    assert to_insert.iloc[0]["change_type"] == "reactivated"
    assert to_insert.iloc[0]["effective_start_date"] == date(2026, 9, 1)  # pas avant la fin de l'ancienne
    assert to_update.empty


def test_hard_delete_detected_only_on_full_snapshot() -> None:
    incoming = customers([(1001, "Alice Johnson", "alice.johnson@email.com", "2023-01-15", "active", datetime(2024, 2, 1))])
    processor = SCD2Processor()
    _, without_detection = processor.apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY)
    _, with_detection = processor.apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY, detect_deletes=True)
    assert without_detection.empty
    assert list(with_detection["customer_id"]) == [1002]
    assert with_detection.iloc[0]["change_type"] == "deleted"


def test_change_date_never_before_current_version_start() -> None:
    """Donnée corrigée avec une date ancienne : la nouvelle version ne peut pas commencer avant l'actuelle."""
    incoming = customers([(1002, "Robert Smith", "bob.smith@email.com", "2023-02-20", "active", datetime(2022, 1, 1))])
    to_insert, to_update = SCD2Processor().apply_scd2_logic(incoming, existing_dimension(), effective_date=TODAY)
    assert to_insert.iloc[0]["effective_start_date"] == date(2023, 2, 20)
    assert to_update.iloc[0]["effective_end_date"] == date(2023, 2, 20)
