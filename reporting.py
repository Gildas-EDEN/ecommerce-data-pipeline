"""
reporting.py : rapport d'exécution et de qualité des données.

Deux fichiers par exécution, dans data/output/reports/<run_id>/ :
  - execution_report.json : version complète, exploitable par un outil de monitoring
  - quality_report.md     : version résumée, lisible par un humain
"""

from __future__ import annotations

import json
from typing import Any, Dict, List


STATUS_LABELS = {"done": "terminée", "failed": "échec", "running": "en cours"}


def build_lineage(meta: Dict[str, Dict[str, Any]], engine: str) -> List[Dict[str, Any]]:
    """Lignage des données : d'où vient chaque table cible et quelles transformations elle a subies."""
    customers = meta.get("extract_customers", {})
    orders = meta.get("extract_orders", {})
    products = meta.get("extract_products", {})
    written = meta.get("load", {}).get("rows_written", {})
    return [
        {
            "target": "dim_customers",
            "sources": [customers.get("source", "public.customers")],
            "transformations": [
                f"DatabaseExtractor ({customers.get('mode', '?')})", "standardize_customers",
                "DataValidator", "SCD2Processor",
            ],
            "rows_written": written.get("dim_customers_inserted", 0) + written.get("dim_customers_closed", 0),
        },
        {
            "target": "raw_orders + raw_order_items",
            "sources": [orders.get("source", "API des commandes")],
            "transformations": [
                f"APIExtractor ({orders.get('mode', '?')}, pagination, relances)", "flatten_orders", "DataValidator",
            ],
            "rows_written": written.get("raw_orders", 0) + written.get("raw_order_items", 0),
        },
        {
            "target": "raw_products",
            "sources": [f"{products.get('file', 'products.csv')} (sha256 {str(products.get('sha256', ''))[:12]}...)"],
            "transformations": ["FileExtractor", "standardize_products", "DataValidator"],
            "rows_written": written.get("raw_products", 0),
        },
        {
            "target": "fact_daily_orders",
            "sources": ["commandes validées de l'exécution"],
            "transformations": [f"build_daily_order_facts (moteur {engine})"],
            "rows_written": written.get("fact_daily_orders", 0),
        },
        {
            "target": "fact_product_performance",
            "sources": ["commandes et lignes validées de l'exécution"],
            "transformations": [f"build_product_performance (moteur {engine})"],
            "rows_written": written.get("fact_product_performance", 0),
        },
    ]


def _cell(value: Any) -> str:
    """Texte d'une cellule de tableau Markdown."""
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return text.replace("|", "\\|").replace("\n", " ")


def render_markdown_report(report: Dict[str, Any]) -> str:
    """Met en forme le rapport d'exécution en Markdown."""
    quality = report.get("data_quality") or {}
    lines = [
        f"# Rapport d'exécution : {report['run_id']}",
        "",
        f"- Généré le : {report['generated_at']}",
        f"- Statut qualité global : **{quality.get('overall_status', 'non disponible')}**",
        f"- Durée cumulée des étapes : {report['total_duration_seconds']} s",
        "",
        "## Étapes",
        "",
        "| Étape | Statut | Durée (s) |",
        "|---|---|---|",
    ]
    for stage, info in report["stages"].items():
        status = info.get("status")
        label = "en cours (ce rapport)" if stage == "report" else STATUS_LABELS.get(status, status)
        lines.append(f"| {stage} | {label} | {info.get('duration_seconds') or ''} |")

    lines += ["", "## Extraction", "", "| Source | Mode | Lignes extraites | Watermark utilisé |", "|---|---|---|---|"]
    for source, info in report["records"]["extracted"].items():
        lines.append(f"| {source} | {info.get('mode')} | {info.get('rows')} | {_cell(info.get('watermark_used') or 'aucun')} |")

    lines += ["", "## Lignes écrites dans l'entrepôt", "", "| Table | Lignes |", "|---|---|"]
    for table, count in report["records"]["written"].items():
        lines.append(f"| {table} | {count} |")

    lines += ["", "## Qualité des données", ""]
    for table, result in quality.get("tables", {}).items():
        lines += [
            f"### {table} : {result['overall_status']} (décision : {result.get('decision')})",
            "",
            f"{result['total_records']} ligne(s) contrôlée(s), {result['invalid_records']} invalide(s).",
            "",
            "| Contrôle | Statut | Détails |",
            "|---|---|---|",
        ]
        for name, check in result["checks"].items():
            lines.append(f"| {name} | {check['status']} | {_cell(check.get('details'))} |")
        lines.append("")

    lines += ["## Lignage des données", "", "| Table cible | Source(s) | Transformations | Lignes écrites |", "|---|---|---|---|"]
    for entry in report["lineage"]:
        lines.append(
            f"| {entry['target']} | {_cell(', '.join(entry['sources']))} | "
            f"{_cell(' -> '.join(entry['transformations']))} | {entry['rows_written']} |"
        )

    lines += ["", "## Watermarks (prochaine extraction incrémentale)", "", "| Source | Valeur | Exécution |", "|---|---|---|"]
    for source, info in report["watermarks"].items():
        lines.append(f"| {source} | {_cell(info.get('value'))} | {info.get('run_id')} |")
    return "\n".join(lines) + "\n"
