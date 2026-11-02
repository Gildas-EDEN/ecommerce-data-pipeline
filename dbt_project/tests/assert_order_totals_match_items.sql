-- Règle métier : le total d'une commande doit égaler la somme de ses lignes.
-- Sévérité « warn » : un écart peut être légitime (frais de port, remise), il est
-- signalé sans bloquer. Les données de démo contiennent volontairement un cas.
{{ config(severity='warn') }}

select
    o.order_id,
    o.total_amount,
    sum(i.line_amount) as items_amount
from {{ ref('stg_orders') }} as o
join {{ ref('stg_order_items') }} as i
    on i.order_id = o.order_id
group by o.order_id, o.total_amount
having abs(o.total_amount - sum(i.line_amount)) > 0.01
