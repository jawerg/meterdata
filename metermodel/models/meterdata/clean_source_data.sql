{{config(materialized='view')}}

select
    id            as id,
    ts             as ts,
    `val` as val
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
where minute(ts) in (0, 30)
