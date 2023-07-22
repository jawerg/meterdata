{{config(order_by = ('id'))}}


with (
    select avg(`energy(kWh/hh)`)
    from {{ source ('meterdata', 'meter_halfhourly_dataset') }} ) as global_avg_ec
select LCLid                                 as id,
       avg(`energy(kWh/hh)`) / global_avg_ec as scaling_factor
from {{ source ('meterdata', 'meter_halfhourly_dataset') }}
group by LCLid
order by 1
