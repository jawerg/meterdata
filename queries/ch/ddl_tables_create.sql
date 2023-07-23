create or replace table meter_daily_dataset
(
    "LCLid"         String,
    "day"           date,
    "energy_median" Float32,
    "energy_mean"   Float32,
    "energy_max"    Float32,
    "energy_count"  Float32,
    "energy_std"    Float32,
    "energy_sum"    Float32,
    "energy_min"    Float32
)
    engine = MergeTree
        primary key ("LCLid", "day")
        order by ("LCLid", "day");

create or replace table meter_halfhourly_dataset
(
    "LCLid"          String,
    "tstp"           DateTime('UTC'),
    "energy(kWh/hh)" Float32
)
    engine = MergeTree
        primary key ("LCLid", "tstp")
        order by ("LCLid", "tstp")
;
