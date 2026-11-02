-- Rapprochement source / entrepôt : chaque client de la base source doit avoir
-- une version courante dans la dimension. Sévérité « warn » : un client modifié
-- après le dernier passage du pipeline apparaîtra au passage suivant.
{{ config(severity='warn') }}

select s.customer_id
from {{ source('operational', 'customers') }} as s
left join {{ ref('stg_customer_versions') }} as d
    on d.customer_id = s.customer_id
   and d.is_current
where d.customer_id is null
