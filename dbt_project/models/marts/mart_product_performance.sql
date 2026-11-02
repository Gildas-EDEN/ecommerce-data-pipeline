-- Synthèse de la performance des produits : ventes totales, part du chiffre
-- d'affaires, classement et tendance (30 derniers jours vs 30 jours précédents).
with perf as (
    select * from {{ source('warehouse', 'fact_product_performance') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

bounds as (
    -- la tendance est calculée par rapport à la dernière date de vente connue
    select max(order_date) as last_date from perf
),

totals as (
    select
        p.product_id,
        sum(p.units_sold) as units_sold,
        sum(p.revenue) as revenue,
        sum(p.order_count) as orders,
        min(p.order_date) as first_sale_date,
        max(p.order_date) as last_sale_date,
        sum(case when p.order_date > b.last_date - 30 then p.revenue else 0 end) as revenue_last_30d,
        sum(case when p.order_date <= b.last_date - 30
                  and p.order_date > b.last_date - 60 then p.revenue else 0 end) as revenue_prev_30d
    from perf as p
    cross join bounds as b
    group by p.product_id
)

select
    pr.product_id,
    pr.product_name,
    pr.category,
    pr.supplier,
    pr.current_price,
    coalesce(t.units_sold, 0) as units_sold,
    coalesce(t.revenue, 0) as revenue,
    coalesce(t.orders, 0) as orders,
    round(100.0 * coalesce(t.revenue, 0) / nullif(sum(coalesce(t.revenue, 0)) over (), 0), 2) as revenue_share_pct,
    rank() over (order by coalesce(t.revenue, 0) desc) as revenue_rank,
    t.first_sale_date,
    t.last_sale_date,
    coalesce(t.revenue_last_30d, 0) as revenue_last_30d,
    coalesce(t.revenue_prev_30d, 0) as revenue_prev_30d,
    round(100.0 * (t.revenue_last_30d - t.revenue_prev_30d) / nullif(t.revenue_prev_30d, 0), 1) as trend_pct
from products as pr
left join totals as t
    on t.product_id = pr.product_id
