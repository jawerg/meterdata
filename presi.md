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

---

# Clickhouse 101

clickhouse...

- has its own SQL dialect and has some fine-grained controls when designing tables (e.g. engines and order by)
- is a column-orientated warehouse, which allows to compress the stored data.
  - allows for fast aggregation of means and standard deviations (as used for SE)
- uses sparse indexes on sorted tables
  - thus minimal primary key size, despite large table sizes.
  - note: Postgres Index became larger than memory.

---

## Clickhouse DDL

Imagine we've got some data about [smart meters in London from kaggle](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london), which already rests in the following clickhouse table:

```sql
create or replace table meter_halfhourly_dataset (
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
- Keep in mind, that compression not only reduces storage cost, but additionally reduces the cost of scanning data.

The latter also hints to why large batches of inserted data are more efficient in Clickhouse: Finding patterns to compress becomes more efficient if there is more "training data".

---

# dbt 101

dbt is...

- is an open source solution to programatically design data transformations in a data warehouse.
- uses jinja templates which allows to re-use components and implement simple control flows (if-else, for loops, etc).
- includes a testing framework.
- auto-generates documentation of pipelines, tests, tables and columns.
- simplyfies writing data pipelines, as it reduces the input needed to a simple query and handles all the orchestration around it.


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

A model is only a query that results in a table or view and expressed in SQL (here, clickhouse dialect). Let's kick of with a simple candidate here:

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
    avg(ec) as ec
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

Core take-away: dbt-defined tables are referenced using the `ref` syntax, which allows dbt to build a DAG.

- Note: Show `dbt docs generate` + `dbt docs serve`