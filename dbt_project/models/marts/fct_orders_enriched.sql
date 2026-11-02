-- Une ligne par commande, enrichie avec le statut du client AU MOMENT de la commande.
--
-- Modèle INCRÉMENTAL : au premier passage, dbt construit toute la table ; ensuite il
-- ne traite que les commandes chargées (ou rechargées) depuis son dernier passage,
-- et remplace les anciennes lignes de ces commandes grâce à la clé unique order_id.
-- Pour tout reconstruire : dbt run --full-refresh
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    -- {{ this }} désigne la table déjà construite : on ne prend que le nouveau
    where loaded_at > (select coalesce(max(loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

items as (
    select
        order_id,
        count(*) as line_count,
        sum(quantity) as units
    from {{ ref('stg_order_items') }}
    group by order_id
),

customer_versions as (
    select * from {{ ref('stg_customer_versions') }}
)

select
    o.order_id,
    o.order_date,
    o.customer_id,
    c.customer_name,
    -- Jointure « point-in-time » : on prend la version du client valide à la date
    -- de la commande, pas sa version actuelle. C'est tout l'intérêt du SCD2.
    coalesce(c.status, 'unknown') as customer_segment_at_order,
    o.total_amount,
    coalesce(i.line_count, 0) as line_count,
    coalesce(i.units, 0) as units,
    o.loaded_at
from orders as o
left join items as i
    on i.order_id = o.order_id
left join customer_versions as c
    on c.customer_id = o.customer_id
   and o.order_date >= c.effective_start_date
   and o.order_date <  c.effective_end_date
