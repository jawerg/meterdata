---
marp: true
theme: default
---

# dbt 101

dbt is...

- is an open source solution to programatically design data transformations in a data warehouse.    
- uses jinja templates which allows to re-use components and implement simple control flows (if-else, for loops, etc).
- includes a testing framework.
- auto-generates documentation of pipelines, tests, tables and columns.



--- 

# Clickhouse 101

clickhouse...

- has its own SQL dialect and has some fine-grained controls when designing tables (e.g. engines and order by)
- is a column-orientated warehouse, which allows to compress the stored data.
- uses sparse indexes

---

## Clickhouse DDL

Imagine we've got some data about smart meters in London from kaggle, which already rests in the following clickhouse table:

```sql
create or replace table meter_halfhourly_dataset (
    "LCLid"          String,
    "tstp"           DateTime('UTC'),
    "energy(kWh/hh)" Float32
)
engine = MergeTree
primary key ("LCLid", "tstp")
order by ("LCLid", "tstp");
```

--- 

## Clickhouse Compression

Maybe, it becomes tangible here how compression works using column-storage: 

- A single `LCLid` appears repeatedly, so the data structure will only store something like "id x repeats 1000 times here". Thus, there is only little cost of using a String with the external id. 
- At the same time the `tstp` has some regularity, such that there's no need to store the unix timestamp itself, but rather the difference between two neighboring rows, which will oftentimes follow some pattern. As most of those will be 15, 30 or 60 minutes, some Huffman coding-like compression should be able to reduce the used disk space.

The latter also hints to why large batches of inserted data are more efficient in Clickhouse: Finding patterns to compress becomes more efficient if there is more "training data".


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
          - name: LCLid
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

with ( select avg(`energy(kWh/hh)`) from {{ source('meterdata', 'meter_halfhourly_dataset') }} ) as global_avg_ec
select 
    LCLid                                 as id,
    max(`energy(kWh/hh)`)                 as p_max,
    avg(`energy(kWh/hh)`) / global_avg_ec as scaling_factor
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
group by id
order by id
```

Note that all jinja stuff `{{ }}` is later build (thus build tool) by dbt. The configs are clickhouse specific. Here, a MergeTree is used. If no primary key is given, the order by clause applies.

---

## Clickhouse + dbt: Second Model 

Another quite simple example, which will however help us to illustrate how tests work:

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
