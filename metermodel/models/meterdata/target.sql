{{config(order_by = ('id', 'ts'))}}


select LCLid as id, tstp as ts, `energy(kWh/hh)` as ec
from {{ source('meterdata', 'meter_halfhourly_dataset') }}

union all

select id, ts, ec
from {{ ref('interpolation_long_intervals') }}

union all

select id, ts, ec
from {{ ref('interpolation_short_intervals') }}
