{{config(order_by=('id'))}}


with ( select avg(val) from {{ ref('clean_source_data') }} ) as global_avg_ec
select id, max(val) as p_max, avg(val) / global_avg_ec as scaling_factor
from {{ ref('clean_source_data') }}
group by id
order by id
