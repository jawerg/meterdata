{{config(order_by = ('id', 'ts'))}}


select id, ts, ec
from {{ ref('clean_source_data') }}

union all

select id, ts, ec
from {{ ref('interpolation_long_intervals') }}
