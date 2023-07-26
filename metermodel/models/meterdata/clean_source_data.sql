{{ config(materialized='view') }}

select
    LCLid            as id,
    tstp             as ts,
    `energy(kWh/hh)` as ec
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
where minute(tstp) in (0, 30)
