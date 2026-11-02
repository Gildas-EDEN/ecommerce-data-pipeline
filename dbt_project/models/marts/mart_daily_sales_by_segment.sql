-- Ventes quotidiennes par segment client (statut du client à la date de la commande).
-- Répond à l'exigence « Daily Order Facts : métriques par date et segment client ».
select
    order_date,
    customer_segment_at_order as customer_segment,
    count(*) as orders,
    count(distinct customer_id) as customers,
    sum(total_amount) as revenue,
    round(avg(total_amount), 2) as avg_order_value
from {{ ref('fct_orders_enriched') }}
group by order_date, customer_segment_at_order
