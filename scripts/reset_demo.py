"""
scripts/reset_demo.py : remet la démonstration à zéro.

  - supprime les fichiers produits par les exécutions (checkpoints, watermarks,
    rapports, alertes, logs) ;
  - régénère les commandes servies par l'API simulée (efface celles ajoutées
    par simulate_changes.py) ;
  - avec --docker : supprime aussi les conteneurs ET la base PostgreSQL
    (docker compose down -v). Au prochain « docker compose up -d », la base
    est recréée avec les 5 clients du cours.

Usage : python scripts/reset_demo.py [--docker]
"""

from __future__ import annotations

import argparse
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from generate_sample_data import main as generate_orders  # noqa: E402

GENERATED = ["data/checkpoints", "data/state", "data/output", "logs"]


def _make_writable_and_retry(func, path, _exc) -> None:
    """Fichier en lecture seule : on lève l'attribut puis on recommence."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def remove_tree(path: Path, attempts: int = 10) -> None:
    """Supprime un dossier en réessayant : sous Windows, OneDrive ou l'antivirus
    peuvent garder un dossier ouvert quelques instants après sa création."""
    for attempt in range(1, attempts + 1):
        try:
            if sys.version_info >= (3, 12):
                shutil.rmtree(path, onexc=_make_writable_and_retry)
            else:
                shutil.rmtree(path, onerror=_make_writable_and_retry)
            return
        except PermissionError:
            if attempt == attempts:
                raise
            time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Remet la démonstration à zéro")
    parser.add_argument("--docker", action="store_true", help="supprime aussi les conteneurs et la base (down -v)")
    args = parser.parse_args()

    if args.docker:
        print("Arrêt des conteneurs et suppression de la base (docker compose down -v)...")
        subprocess.run(["docker", "compose", "down", "-v"], cwd=ROOT, check=True)

    for relative in GENERATED:
        path = ROOT / relative
        if path.exists():
            remove_tree(path)
            print(f"Supprimé : {relative}/")
    generate_orders()
    print("Démonstration remise à zéro.")


if __name__ == "__main__":
    main()
