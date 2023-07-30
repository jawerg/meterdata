{{config(order_by=('id', 'gap_id', 'ts'))}}

with interpolation_long_intervals_and_bounds as (
    select id, gap_id, ts, ec
    from {{ ref('interpolation_long_intervals') }}

    union all

    select id, gap_id, ts, ec
    from {{ ref('interpolation_long_interval_bounds') }}
)

select id, gap_id, ts, ec
from interpolation_long_intervals_and_bounds
order by id, gap_id, ts
