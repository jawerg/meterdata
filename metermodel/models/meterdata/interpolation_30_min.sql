{{config(order_by = ('id', 'ts'))}}


select
    id,
    ts + interval 30 minute as ts,
    (ec + neighbor(ec, 1)) / 2 as ec
from {{ ref('gap_closer') }}
where id = neighbor(id, 1)
order by id, ts
