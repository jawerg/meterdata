import time

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns
from clickhouse_driver import Client

sns.set_style("darkgrid")

PG_SELECT_QUERY = """
select id, ts, val
from raw.meter_halfhourly_dataset
where id = ( select id from raw.device_ids where device_id = '{id}')
"""

CH_SELECT_QUERY = """
select id, ts, val
from meterdata.target
where id = '{id}'
"""

PG_INTERP_QUERY = """
with meterdata as (
    select id, ts, val
    from raw.meter_halfhourly_dataset
    where id = ( select id from raw.device_ids where device_id = '{id}')

    union all

    select id, ts + interval '30 minute' as ts, (val + lead(val) over (partition by id order by ts)) / 2 as val
    from (
        select
            id, ts, val,
            lead(ts) over (partition by id order by ts) as next_ts,
            lead(val) over (partition by id order by ts) as next_val
        from raw.meter_halfhourly_dataset
        where id = ( select id from raw.device_ids where device_id = '{id}')
    ) as subquery
    where next_ts - ts = interval '1 hour' and next_val is not null
)
select * from meterdata order by id, ts
"""

CH_INTERP_QUERY = """
with meterdata as (
    select id, ts, val
    from meterdata.target
    where id = '{id}'

    union all

    select id, ts + interval 30 minute as ts, (val + neighbor(val, 1)) / 2 as val
    from meterdata.target
    where id = '{id}'
      and neighbor(ts, 1) - ts = 60 * 60
      and neighbor(val, 1) is not null
)
select * from meterdata order by id, ts
"""

if __name__ == '__main__':
    # Note: ChatGPT generated :D

    # Connect to PostgreSQL
    dbname, user, password, host, port = "meterdata", "postgres", "postgres", "localhost", "5432"
    pg_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    pg_cursor = pg_conn.cursor()

    # Connect to ClickHouse
    ch_client = Client(host='localhost')

    # Fetch all available IDs
    pg_cursor.execute("select distinct device_id from raw.device_ids")
    device_ids = [row[0] for row in pg_cursor.fetchall()]

    # Lists to store results
    pg_select_results = []
    ch_select_results = []
    pg_interp_results = []
    ch_interp_results = []

    for device_id in device_ids:
        # Execute PostgreSQL queries
        start = time.perf_counter()
        pg_cursor.execute(PG_SELECT_QUERY.format(id=device_id))
        _ = pg_cursor.fetchall()
        pg_select_results.append(1000 * (time.perf_counter() - start))

        start = time.perf_counter()
        pg_cursor.execute(PG_INTERP_QUERY.format(id=device_id))
        _ = pg_cursor.fetchall()
        pg_interp_results.append(1000 * (time.perf_counter() - start))

        # Execute ClickHouse queries
        start = time.perf_counter()
        _ = ch_client.execute(CH_SELECT_QUERY.format(id=device_id))
        ch_select_results.append(1000 * (time.perf_counter() - start))

        start = time.perf_counter()
        _ = ch_client.execute(CH_INTERP_QUERY.format(id=device_id))
        ch_interp_results.append(1000 * (time.perf_counter() - start))

    # Close connections
    pg_cursor.close()
    pg_conn.close()

    # Convert results to DataFrames
    pg_select_df = pd.DataFrame(pg_select_results, columns=['query time'])
    ch_select_df = pd.DataFrame(ch_select_results, columns=['query time'])

    pg_interp_df = pd.DataFrame(pg_interp_results, columns=['query time'])
    ch_interp_df = pd.DataFrame(ch_interp_results, columns=['query time'])

    # Plot kernel density estimates using seaborn
    fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(15, 5))

    sns.kdeplot(pg_select_df['query time'], label='PostgreSQL', ax=ax[0])
    sns.kdeplot(ch_select_df['query time'], label='ClickHouse', ax=ax[0])
    ax[0].legend()
    ax[0].set_title(f"KDE of Simple Select Query Times (N={len(device_ids)})")
    ax[0].set_xlim(0, 100)

    sns.kdeplot(pg_interp_df['query time'], label='PostgreSQL', ax=ax[1])
    sns.kdeplot(ch_interp_df['query time'], label='ClickHouse', ax=ax[1])
    ax[1].legend()
    ax[1].set_title(f"KDE of Interpolation Query Times (N={len(device_ids)})")
    ax[0].set_xlim(0, 100)

    plt.savefig(
        "figures/query-kde.png",
        dpi=600,
        bbox_inches='tight',
        transparent=True,
        pad_inches=0
    )
    plt.show()
