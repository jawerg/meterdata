select id, ts, neighbor(ts, 1) as next_ts, neighbor(ts, 1) - ts as diff
from {{ ref('target') }}
where id = neighbor(id, 1)
  and neighbor(ts, 1) - ts > 60 * 60
order by id, ts
