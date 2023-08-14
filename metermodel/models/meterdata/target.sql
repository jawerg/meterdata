{{
    config(
        materialized='incremental',
        order_by=('id', 'ts'),
        unique_key='id, ts',
        incremental_strategy='append'
    )
}}

select id, ts, val
from {{ source('meterdata', 'meter_halfhourly_dataset') }}

union all

select id, ts, val
from {{ ref('interpolation_long_intervals') }}
