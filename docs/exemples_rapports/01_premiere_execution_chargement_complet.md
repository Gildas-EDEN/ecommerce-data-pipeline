# Rapport d'exécution : scheduled__2026-10-06T02-00-00-00-00

- Généré le : 2026-10-06T12:15:58
- Statut qualité global : **WARN**
- Durée cumulée des étapes : 31.597 s

## Étapes

| Étape | Statut | Durée (s) |
|---|---|---|
| extract_customers | terminée | 1.183 |
| extract_orders | terminée | 1.234 |
| extract_products | terminée | 0.575 |
| validate | terminée | 1.017 |
| transform_customers | terminée | 1.162 |
| transform_orders | terminée | 25.585 |
| load | terminée | 0.841 |
| report | en cours (ce rapport) |  |

## Extraction

| Source | Mode | Lignes extraites | Watermark utilisé |
|---|---|---|---|
| customers | complet | 5 | aucun |
| orders | complet | 144 | aucun |
| products | complet | 5 | aucun |

## Lignes écrites dans l'entrepôt

| Table | Lignes |
|---|---|
| raw_products | 5 |
| raw_orders | 144 |
| raw_order_items | 287 |
| dim_customers_closed | 0 |
| dim_customers_inserted | 5 |
| fact_daily_orders | 119 |
| fact_product_performance | 196 |

## Qualité des données

### customers : PASS (décision : continue)

5 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | PASS | 5 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_customer_id | PASS | 0 ligne(s) en doublon sur ['customer_id'] |
| email_format_email | PASS | 0 e-mail(s) invalide(s) |
| allowed_values_status | PASS | 0 valeur(s) hors de ['active', 'inactive'] |

### orders : WARN (décision : continue)

144 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | PASS | 144 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_order_id | PASS | 0 ligne(s) en doublon sur ['order_id'] |
| non_negative_total_amount | PASS | 0 valeur(s) hors bornes |
| order_total_consistency | WARN | {"orders_without_items": 0, "orders_with_total_mismatch": 1, "tolerance": 0.01} |

### order_items : PASS (décision : continue)

287 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | PASS | 287 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_order_id_line_number | PASS | 0 ligne(s) en doublon sur ['order_id', 'line_number'] |
| positive_quantity | PASS | 0 valeur(s) hors bornes |
| non_negative_unit_price | PASS | 0 valeur(s) hors bornes |

### products : PASS (décision : continue)

5 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | PASS | 5 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_product_id | PASS | 0 ligne(s) en doublon sur ['product_id'] |
| positive_price | PASS | 0 valeur(s) hors bornes |

## Lignage des données

| Table cible | Source(s) | Transformations | Lignes écrites |
|---|---|---|---|
| dim_customers | postgresql://postgres:5432/analytics (table public.customers) | DatabaseExtractor (complet) -> standardize_customers -> DataValidator -> SCD2Processor | 5 |
| raw_orders + raw_order_items | http://mock-api:8000/orders | APIExtractor (complet, pagination, relances) -> flatten_orders -> DataValidator | 431 |
| raw_products | /opt/project/data/input/products.csv (sha256 75ac228ab815...) | FileExtractor -> standardize_products -> DataValidator | 5 |
| fact_daily_orders | commandes validées de l'exécution | build_daily_order_facts (moteur spark) | 119 |
| fact_product_performance | commandes et lignes validées de l'exécution | build_product_performance (moteur spark) | 196 |

## Watermarks (prochaine extraction incrémentale)

| Source | Valeur | Exécution |
|---|---|---|
| customers | 2024-02-01T10:00:00 | scheduled__2026-10-06T02-00-00-00-00 |
| orders | 2024-02-29 | scheduled__2026-10-06T02-00-00-00-00 |
| products | 75ac228ab8150c1cd06cf0b9e25bd71b239a798133f49c54c42f35493a3234ca | scheduled__2026-10-06T02-00-00-00-00 |
