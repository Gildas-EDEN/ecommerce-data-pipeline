"""
scripts/build_submission.py : crée l'archive .zip demandée par le cours.

« Upload your complete pipeline code as a .zip file containing all Python modules
(config.py, extractors.py, transformers.py, pipeline.py, airflow_dag.py),
configuration files, and a README.md with setup instructions. »

L'archive contient tout le projet SAUF les secrets (.env) et les fichiers produits
à l'exécution (checkpoints, rapports, watermarks, logs, caches).

Usage : python scripts/build_submission.py
"""

from __future__ import annotations

import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "soumission" / "pipeline_spark_dbt_airflow.zip"

EXCLUDED_DIRS = {
    "data/checkpoints", "data/state", "data/output", "logs", "soumission",
    "dbt_project/target", "dbt_project/logs", "dbt_project/dbt_packages",
}
EXCLUDED_NAMES = {".env", "__pycache__", ".pytest_cache", ".venv", "spark-warehouse"}


def is_excluded(path: Path) -> bool:
    relative = path.relative_to(ROOT).as_posix()
    if any(part in EXCLUDED_NAMES for part in path.relative_to(ROOT).parts):
        return True
    return any(relative == d or relative.startswith(d + "/") for d in EXCLUDED_DIRS)


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(p for p in ROOT.rglob("*") if p.is_file() and not is_excluded(p))
    with zipfile.ZipFile(OUTPUT, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, Path(ROOT.name) / path.relative_to(ROOT))
    print(f"{len(files)} fichiers archivés dans {OUTPUT} ({OUTPUT.stat().st_size / 1024:.0f} Ko)")


if __name__ == "__main__":
    main()
