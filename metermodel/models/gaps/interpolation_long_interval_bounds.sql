{{config(order_by = ('id', 'gap_id', 'ts'))}}

select id, gap_id, ts, ec
from {{ ref('boundaries') }}

union distinct

select id, gap_id, next_ts, next_ec
from {{ ref('boundaries') }}
