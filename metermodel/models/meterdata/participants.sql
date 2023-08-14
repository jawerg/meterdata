{{config(order_by=('id'))}}


with ( select avg(val) from {{ source('meterdata', 'meter_halfhourly_dataset') }} ) as global_avg_ec
select id, max(val) as p_max, avg(val) / global_avg_ec as scaling_factor
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
group by id
order by id
