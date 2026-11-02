-- Versions successives des clients (dimension SCD2 construite par le pipeline Python).
-- Intervalle de validité semi-ouvert : une version est valide le jour J si
--   effective_start_date <= J < effective_end_date
select
    dim_customer_key,
    customer_id,
    name as customer_name,
    email,
    status,
    effective_start_date,
    effective_end_date,
    is_current
from {{ source('warehouse', 'dim_customers') }}
