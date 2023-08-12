{{config(order_by=('id', 'gap_id', 'ts'))}}

with
interpolation_long_interval_bounds as (
    select id, gap_id, ts, val
    from {{ ref('boundaries') }}

    union distinct

    select id, gap_id, next_ts, next_ec
    from {{ ref('boundaries') }}
)

select id, gap_id, ts, val
from interpolation_long_interval_bounds
order by id, gap_id, ts
