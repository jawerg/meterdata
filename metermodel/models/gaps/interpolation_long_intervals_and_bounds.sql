{{config(order_by = ('id', 'gap_id', 'ts'))}}

select id, gap_id, ts, ec
from {{ ref('interpolation_long_intervals') }}

union all

select id, gap_id, ts, ec
from {{ ref('interpolation_long_interval_bounds') }}
