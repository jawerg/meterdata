---
marp: true
theme: default
---

# Agenda

- showcase of open smart meter data import and interpolation.
- usage of OLAP database (clickhouse) and dbt for query handling and auto-docs.
- comparison of ballparks for disk-usage and query speed

## Disclaimer

- I've neither used clickhouse or dbt professionally.
- Thus, the way it's done won't be best practice with high probability.
- But I guess that was the idea, just try to see what's in there...
- Thus, some calculations may be off

So, let's go 🙃

---

# Data used here

A quick sample of the data, such that labels and usage aren't too misterious:

| LCLid     | tstp                | energy(kWh/hh) |
| --------- | ------------------- |--------------- |
| MAC000002 | 2013-06-03 13:30:00 |          0.096 |
| MAC000002 | 2013-06-03 14:00:00 |          0.129 |
| MAC000246 | 2013-06-03 13:30:00 |          0.022 |
| MAC000246 | 2013-06-03 14:00:00 |          0.044 |
| MAC003223 | 2013-06-03 13:30:00 |          0.232 |
| MAC003223 | 2013-06-03 14:00:00 |          0.127 |

---

# Clickhouse 101

clickhouse...

- was created for real-time analytics of large datasets.
- has its own SQL dialect and has some fine-grained controls when designing tables (e.g. engines and order by).
- is a column-orientated warehouse, which allows to compress the stored data.
  - allows for fast aggregation of averages, standard deviations, etc.
- uses sparse indexes on sorted tables.
  - thus minimal primary key size, despite large table sizes.

---

## Clickhouse DDL

Imagine we've got some data about [smart meters in London from kaggle](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london), which already rests in the following clickhouse table:

```sql
create or replace table meterdata_raw.meter_halfhourly_dataset (
    id  String,
    ts  DateTime('UTC'),
    val Float32
)
engine = MergeTree
primary key (id, ts)
order by (id, ts);
```

Let's talk later about how the data got there and in what time.

---

## Clickhouse Compression

Maybe, it becomes tangible here how compression works using column-storage:

- A single `id` appears repeatedly, so the data structure will only store something like "id x repeats 1000 times here". Thus, there is only little cost of using a String with the external id.
- At the same time the `ts` has some regularity, such that there's no need to store the unix timestamp itself, but rather the difference between two neighboring rows ([Delta Encoding](https://altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse))
- Keep in mind, that compression not only reduces storage cost, but additionally reduces the cost of scanning data, as there is simply less to scan.

---

# dbt 101

dbt is...

- an abbreviation for _data build tool_ and reflects only the T of ETL (Extract, Transform, Load) patterns in a DWH context.
- is an open source solution to programatically design data transformations in a data warehouse.
- uses jinja templates which allows to re-use components (macros) and implements simple control flows (if-else, for loops, etc).
- includes a testing framework.
- auto-generates documentation of pipelines, tests, tables and columns.
- simplyfies writing data pipelines, as it reduces the input needed to a simple query and handles all the orchestration around it.

---

## Clickhouse + dbt: Defining a schema

dbt must learn about tables that are not managed by it, thus sources are described in yaml notation, which are used to auto-gen docs later.

```yml
version: 2

sources:
  - name: meterdata
    database: meterdata_raw  # note that clickhouse has no notion of a schema
    tables:
      - name: meter_halfhourly_dataset
        columns:
          - name: id
            description: External ID of smartmeter
            tests:
              - not_null
          ...  # save some slide-space
```

---

## Clickhouse + dbt: First Model

A model is only a query that results in a table or view and expressed in SQL (here, clickhouse dialect). Let's kick of with a simple candidate here, that shows to the previously defined table will be referenced.

```sql
{{config(order_by=('id'))}}

with ( select avg(val) from {{ source('meterdata', 'meter_halfhourly_dataset') }} ) as global_avg_ec
select
    id,
    max(val)                 as p_max,
    avg(val) / global_avg_ec as scaling_factor
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
group by id
order by id
```

Note that all jinja stuff `{{ }}` is later build (thus build tool) by dbt. The configs are clickhouse specific. Here, a MergeTree is used. If no primary key is given, the order by clause applies.

---

## Clickhouse + dbt: Second Model

Another quite simple example, which will however help us to illustrate how tests work in the next step:

```sql
{{config(order_by=('dt', 'ts'))}}

with years as (
    select distinct year(ts) as year
    from {{ source('meterdata', 'meter_halfhourly_dataset') }}
)
select
    makeDate(years.year, month(ts), day(ts) ) as dt,
    makeDateTime(years.year, month(ts), day(ts), hour(ts), 0, 0, 'UTC') as ts,
    avg(val) as val
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
     cross join years
group by dt, ts
order by dt, ts
```

---

## Clickhouse + dbt: First Test

Oftentimes, tests in SQL are defined, such that the expected result is empty. This allows to test for unexpected data. Here, I want the standard load profile to be in one hour intervals from the first to last timestamp.

Clickhouse's neighbor keyword: Similar to Postgres' `lead(ts) over (partition by id order by ts)`. Recall that the order by clause is set in the table definition, which enables this to be fast ⚡️

```sql
select ts
from {{ ref('standard_load_profile') }}
where ts - neighbor(ts, 1) > 60 * 60
  and year(neighbor(ts, 1)) != 1970 -- last row would be succeeded by 1970-01-01 00:00:00
```

Core take-away: dbt-defined tables are referenced using the `source` and `ref` syntax, which allows dbt to build a DAG.

---

## Loading Data into Clickhouse Pt.1

Use duckdb to generate one Parquet file from csv regex, with filter large row groups (batches):

```sql
copy (
    select
        "LCLid" as id,
        "tstp" as ts,
        "energy(kWh/hh)" as val
    from read_csv(
            'data/raw/halfhourly_dataset/halfhourly_dataset/block_*.csv',
            delim = ',',
            header = true,
            nullstr = 'Null',
            columns ={ 'LCLid': 'VARCHAR', 'tstp': 'TIMESTAMP', 'energy(kWh/hh)': 'FLOAT' }
        )
    where "energy(kWh/hh)" is not null
    )
    to 'data/ready/halfhourly_dataset.parquet' (
        format 'parquet',
        compression 'zstd',
        row_group_size 2000000
);
```

---

## Loading Data into Clickhouse Pt.2.1

In the end, the insert will look very familiar:

```py
def insert_data(data):
    with Client("localhost") as client:
        client.execute(
            query="insert into meterdata_raw.meter_halfhourly_dataset (id, ts, val) values",
            params=data,
            types_check=True,
        )
```

Note that there is nothing clickhouse specific. But maybe, the parallelization works much better. However, I didn't try this strategy on Postgres yet (not true anymore 🤭). All quality assurance (expect not null requirement) is shifted to within the DWH.

---

## Loading Data into Clickhouse Pt.2.2

Each row group is designed to be of around 2 Mrows here. I've added the time and percentange of each step.

```py
def insert_row_group(args):
    file_path, row_group_index = args

    # read (0.4%, 0.03s)
    parquet_file = pq.ParquetFile(file_path)  # Open the Parquet file within the worker process
    table = parquet_file.read_row_group(row_group_index)
    table = table.rename_columns(["id", "ts", "val"])

    # transpose (44.8%, 3.43s)
    data = list(zip(*[tuple(row) for row in table.to_pydict().values()]))  # 😕

    # insert (54.8%, 4.19s)
    insert_data(data=data)
```

Still waiting for [ADBC](https://arrow.apache.org/docs/format/ADBC.html), which should make the second step redundant...

---

## Loading Data into Clickhouse Pt.2.3

The magic is in the row groups, and allows the single file to be read in parallel from multiple processes without blocking 🤯

```py
def insert_file_content(table_name: str):
    file_path = f"data/ready/{table_name}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    with Pool() as pool:
        pool.map(
            insert_row_group,
            [(file_path, i) for i in range(parquet_file.num_row_groups)],
        )
```

Running this on the M1 with 8 cores, takes 90 seconds in total for 84 row groups of 2 Mrows. Each row group takes approximately 7.65 seconds. So roughly 168.000.000 rows in total.

---

# Timeseries interpolation PoC

My goal was to completely rely on (clickhouse's) native SQL syntax and avoid any stored procedure.
The following steps have been taken to do so:
- Create a row for each timeseries gap larger than 60 minutes with start and end timestamp.
- Split each of those rows by day to allow to join against the SLP.
- Join standard load profile and scale by participants `p_max`.
- Build union of raw data and interpolated data.

Any interpolation below 1 hour can be done later when querying the data, such that we don't need to store simple linear interpolation steps. Snippet follows.

---

## Timeseries interpolation: Gap Finding

I'll leave the `{{config=...}}`` stuff out here.

```sql
select
    id,
    rowNumberInAllBlocks() as gap_id, -- generates row ID
    ts,
    neighbor(ts, 1) as next_ts,
    val,
    neighbor(val, 1) as next_val
from {{ ref('clean_source_data') }}
where next_ts - ts > 30 * 60 -- fill gaps larger than 30 mins
  and neighbor(id, 1) = id
order by id, ts
```

Why 30 min: 11:30 - 12:30 is only an hour, but 11:00 is missing. Before we've filtered

---

## Timeseries interpolation: Split Gaps by Day

Main Takeaway: Clickhouse has some useful built-in functions:

```sql
select
    id,
    gap_id,
    arrayJoin(
        arrayDistinct(
                arrayMap(
                    x -> toDate(x, 'UTC'),
                    range(toUInt32(ts), toUInt32(next_ts), 30 * 60)
                )
        )
    ) as dt,
    case
        when dt == ts::Date
            then greatest(ts, dt::Timestamp('UTC') + interval 0 hour)
        else makeDateTime(year(dt), month(dt), day(dt), 0, 0, 0, 'UTC')
    end as start_ts,
    case
        when dt != next_ts::Date
            then  makeDateTime(year(dt), month(dt), day(dt), 23, 0, 0, 'UTC')
        else least(next_ts, dt::Timestamp('UTC') + interval 23 hour + interval 30 minute)
    end as end_ts
from {{ ref('boundaries') }}
order by id, gap_id, dt, start_ts
```

---

## Timeseries interpolation: Interpolate with SLP

```sql
select
    bbd.id as id,
    bbd.gap_id as gap_id,
    slp.ts as ts,
    slp.val * p.scaling_factor as val
from {{ ref('boundaries_by_date') }} as bbd
inner join {{ ref('standard_load_profile') }} as slp on bbd.dt = slp.dt
inner join {{ ref('participants') }} as p on bbd.id = p.id
where slp.ts >= bbd.start_ts
  and slp.ts <= bbd.end_ts
  and (id, ts) not in (
    select id, ts
    from {{ ref('interpolation_long_interval_bounds') }}
)
order by id, gap_id, ts
```

where the `interpolation_long_interval_bounds` are just the timestamp bounds of a gap, which becomes necessary due to split by day, which was necessary to construct a memory sparing inner join.

---

## Timeseries interpolation: dbt run

So, let's run our model and see how long it takes.

```
(.meterdata) ➜  metermodel git:(main) ✗ dbt run
09:57:35  Found 7 models, 6 tests, 0 snapshots, 0 analyses, 319 macros, 0 operations, 0 seed files, 2 sources, 0 exposures, 0 metrics
09:57:35
09:57:35  Concurrency: 1 threads (target='dev')
09:57:35
09:57:35  1 of 7 START sql view model meterdata.clean_source_data ........................ [RUN]
09:57:36  1 of 7 OK created sql view model meterdata.clean_source_data ................... [OK in 0.06s]
09:57:36  2 of 7 START sql table model meterdata_gaps.boundaries ......................... [RUN]
09:57:36  2 of 7 OK created sql table model meterdata_gaps.boundaries .................... [OK in 0.74s]
09:57:36  3 of 7 START sql table model meterdata.participants ............................ [RUN]
09:57:37  3 of 7 OK created sql table model meterdata.participants ....................... [OK in 0.83s]
09:57:37  4 of 7 START sql table model meterdata_slp.standard_load_profile ............... [RUN]
09:57:41  4 of 7 OK created sql table model meterdata_slp.standard_load_profile .......... [OK in 3.81s]
09:57:41  5 of 7 START sql table model meterdata_gaps.boundaries_by_date ................. [RUN]
09:57:41  5 of 7 OK created sql table model meterdata_gaps.boundaries_by_date ............ [OK in 0.03s]
09:57:41  6 of 7 START sql table model meterdata_gaps.interpolation_long_intervals ....... [RUN]
09:57:42  6 of 7 OK created sql table model meterdata_gaps.interpolation_long_intervals .. [OK in 0.66s]
09:57:42  7 of 7 START sql table model meterdata.target .................................. [RUN]
09:57:53  7 of 7 OK created sql table model meterdata.target ............................. [OK in 11.13s]
09:57:53
09:57:53  Finished running 1 view model, 6 table models in 0 hours 0 minutes and 17.69 seconds (17.69s).
09:57:53
09:57:53  Completed successfully
09:57:53
09:57:53  Done. PASS=7 WARN=0 ERROR=0 SKIP=0 TOTAL=7
```

---

## Interpolation: Linear interpolation

If intervals of less than 60 minutes are interpolated linearly, this can be done while fetching the data instead of blowing up the table:

```sql
with meterdata as (
    select id, ts, val
    from meterdata.target
    where id = 'MAC005558'

    union all

    select id, ts + interval 30 minute as ts, (val + neighbor(val, 1)) / 2 as val
    from meterdata.target
    where id = 'MAC005558'
      and neighbor(ts, 1) - ts = 60 * 60
      and neighbor(val, 1) is not null
)
select * from meterdata order by id, ts
```

This query returned in 60ms. So, let's summarize timing stuff.

---

# Timing

Let's summarize where time was spend reading thos 167 Mrows, corresponding to 7.3 GB csv file size and 1.1GB in clickhouse tables size. **If** this would scale linearly, we could process 1TB of data in less than 4.5 hours.

| Step                     | time taken     | postgres |
| ---                      | ---            | ---      |
| csv2parquet (duckdb)     |     6.35s      |          |
| parallel load2clickhouse |    88.79s      | 398.91s  |
| interpolation (dbt)      |    17.69s      |          |
| **Total**                | **112.83s**    |          |

---

## Timing and Disk Usage: Postgres

I Couldn't resist the urge to run the same for postgres and calculate the time spend shares:
- read (0,1%, 0,03s)
- transpose (8.6%, 2.99s)
- insert (91.3%, 31.82s) vs (54.8%, 4.19s) for clickhouse.

Note, that the postgres table has no index, so we can't query the data yet... Nevertheless, it **takes 7.5 times as long**.
Creating a table with a bigserial and replacing the id string, takes another 23s + 18m 33s. Maybe, I did something wrong here 😅

Nevertheless, Storage needs in Postgres:
- Base Table: 9.41
- Indexed Table: 8.15 GB + 5.09 GB Index

So, **compression** saves us a **factor of 12** compared to the 1.1GB.
