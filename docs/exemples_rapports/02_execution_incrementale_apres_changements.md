# Rapport d'exécution : manual__2026-10-06T13-16-19.132100-00-00

- Généré le : 2026-10-06T13:18:00
- Statut qualité global : **WARN**
- Durée cumulée des étapes : 32.44 s

## Étapes

| Étape | Statut | Durée (s) |
|---|---|---|
| extract_customers | terminée | 0.183 |
| extract_orders | terminée | 0.269 |
| extract_products | terminée | 0.134 |
| validate | terminée | 0.77 |
| transform_customers | terminée | 0.558 |
| transform_orders | terminée | 29.899 |
| load | terminée | 0.627 |
| report | en cours (ce rapport) |  |

## Extraction

| Source | Mode | Lignes extraites | Watermark utilisé |
|---|---|---|---|
| customers | incrémental | 5 | 2024-02-01T10:00:00 |
| orders | incrémental | 7 | 2024-02-29 |
| products | inchangé (ignoré) | 0 | 75ac228ab8150c1cd06cf0b9e25bd71b239a798133f49c54c42f35493a3234ca |

## Lignes écrites dans l'entrepôt

| Table | Lignes |
|---|---|
| raw_products | 0 |
| raw_orders | 7 |
| raw_order_items | 11 |
| dim_customers_closed | 2 |
| dim_customers_inserted | 5 |
| fact_daily_orders | 7 |
| fact_product_performance | 9 |

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

### orders : PASS (décision : continue)

7 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | PASS | 7 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_order_id | PASS | 0 ligne(s) en doublon sur ['order_id'] |
| non_negative_total_amount | PASS | 0 valeur(s) hors bornes |
| order_total_consistency | PASS | {"orders_without_items": 0, "orders_with_total_mismatch": 0, "tolerance": 0.01} |

### order_items : PASS (décision : continue)

11 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | PASS | 11 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_order_id_line_number | PASS | 0 ligne(s) en doublon sur ['order_id', 'line_number'] |
| positive_quantity | PASS | 0 valeur(s) hors bornes |
| non_negative_unit_price | PASS | 0 valeur(s) hors bornes |

### products : WARN (décision : continue)

0 ligne(s) contrôlée(s), 0 invalide(s).

| Contrôle | Statut | Détails |
|---|---|---|
| schema | PASS | {"missing_columns": [], "unknown_columns": []} |
| row_count | WARN | 0 ligne(s) |
| required_not_null | PASS | {} |
| optional_nulls | PASS | {} |
| data_types | PASS | [] |
| unique_product_id | PASS | 0 ligne(s) en doublon sur ['product_id'] |
| positive_price | PASS | 0 valeur(s) hors bornes |

## Lignage des données

| Table cible | Source(s) | Transformations | Lignes écrites |
|---|---|---|---|
| dim_customers | postgresql://postgres:5432/analytics (table public.customers) | DatabaseExtractor (incrémental) -> standardize_customers -> DataValidator -> SCD2Processor | 7 |
| raw_orders + raw_order_items | http://mock-api:8000/orders | APIExtractor (incrémental, pagination, relances) -> flatten_orders -> DataValidator | 18 |
| raw_products | /opt/project/data/input/products.csv (sha256 75ac228ab815...) | FileExtractor -> standardize_products -> DataValidator | 0 |
| fact_daily_orders | commandes validées de l'exécution | build_daily_order_facts (moteur spark) | 7 |
| fact_product_performance | commandes et lignes validées de l'exécution | build_product_performance (moteur spark) | 9 |

## Watermarks (prochaine extraction incrémentale)

| Source | Valeur | Exécution |
|---|---|---|
| customers | 2026-10-06T13:14:07.369404 | manual__2026-10-06T13-16-19.132100-00-00 |
| orders | 2026-10-06 | manual__2026-10-06T13-16-19.132100-00-00 |
| products | 75ac228ab8150c1cd06cf0b9e25bd71b239a798133f49c54c42f35493a3234ca | scheduled__2026-10-06T02-00-00-00-00 |
