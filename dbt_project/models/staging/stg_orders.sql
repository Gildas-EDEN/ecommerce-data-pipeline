-- Commandes : une ligne par commande, colonnes renommées et typées.
select
    order_id,
    customer_id,
    order_date,
    total_amount::numeric(12, 2) as total_amount,
    run_id as loaded_by_run_id,
    loaded_at
from {{ source('warehouse', 'raw_orders') }}
