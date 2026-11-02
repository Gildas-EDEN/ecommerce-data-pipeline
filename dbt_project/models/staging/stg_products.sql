-- Catalogue produits.
select
    product_id,
    name as product_name,
    category,
    price as current_price,
    supplier
from {{ source('warehouse', 'raw_products') }}
