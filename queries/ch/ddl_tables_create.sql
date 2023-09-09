create or replace table meterdata_raw.meter_halfhourly_dataset
(
    id  String,
    ts  DateTime('UTC') CODEC(Delta, ZSTD),
    val Float32
)
    engine = MergeTree
        primary key (id, ts)
        order by (id, ts)
;

create or replace table meterdata.timeseries
(
    id  String,
    ts  DateTime('UTC') CODEC(Delta, ZSTD),
    val Float32,
    loaded_at DateTime('UTC') CODEC(ZSTD(16)),
    is_synthetic Boolean
)
    engine = ReplacingMergeTree()
        primary key (id, ts)
        order by (id, ts)
;

OPTIMIZE TABLE meterdata.timeseries FINAL DEDUPLICATE;
