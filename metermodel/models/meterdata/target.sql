{{config(order_by=('id', 'ts'))}}

with
data_union as (
    select id, ts, val
    from {{ ref('clean_source_data') }}

    union all

    select id, ts, val
    from {{ ref('interpolation_long_intervals') }}
)

select id, ts, val
from data_union
order by id, ts
