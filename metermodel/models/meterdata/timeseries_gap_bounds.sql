{{config(order_by = ('id', 'ts'))}}


select
  LCLid                         as id,
  tstp                          as ts,
  neighbor(tstp, 1)             as next_ts,
  `energy(kWh/hh)`              as ec,
  neighbor(`energy(kWh/hh)`, 1) as next_ec
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
where next_ts - tstp > 60 * 60 -- fill gaps larger than one hours
  and neighbor(LCLid, 1) = LCLid
order by id, ts
