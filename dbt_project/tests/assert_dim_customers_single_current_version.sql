-- Test SCD2 : un client ne peut jamais avoir plus d'UNE version courante.
-- Un test dbt « singulier » est une requête qui doit renvoyer ZÉRO ligne :
-- chaque ligne renvoyée est une anomalie.
select
    customer_id,
    count(*) as current_versions
from {{ ref('stg_customer_versions') }}
where is_current
group by customer_id
having count(*) > 1
