-- Test SCD2 : les périodes de validité d'un même client ne se chevauchent jamais.
-- Deux intervalles [a_debut ; a_fin[ et [b_debut ; b_fin[ se chevauchent si
--   a_debut < b_fin  ET  b_debut < a_fin
select
    a.customer_id,
    a.dim_customer_key,
    b.dim_customer_key as overlapping_key
from {{ ref('stg_customer_versions') }} as a
join {{ ref('stg_customer_versions') }} as b
    on a.customer_id = b.customer_id
   and a.dim_customer_key < b.dim_customer_key
   and a.effective_start_date < b.effective_end_date
   and b.effective_start_date < a.effective_end_date
