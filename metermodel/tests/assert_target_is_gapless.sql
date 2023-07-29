select id
from {{ ref('target') }}
where id = neighbor(id, 1)
  and neighbor(ts, 1) - ts != 30 * 60
