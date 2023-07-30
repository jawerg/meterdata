{{config(order_by=('id'))}}


with ( select avg(ec) from {{ ref('clean_and_partitioned_source_data') }} ) as global_avg_ec
select id, max(ec) as p_max, avg(ec) / global_avg_ec as scaling_factor
from {{ ref('clean_and_partitioned_source_data') }}
group by id
order by id
