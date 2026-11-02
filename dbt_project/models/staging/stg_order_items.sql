-- Lignes de commande, avec le montant de chaque ligne.
select
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price,
    (quantity * unit_price)::numeric(12, 2) as line_amount
from {{ source('warehouse', 'raw_order_items') }}
