create schema core;
create table core.devices (
    id uuid primary key,
    name varchar(255) not null
);

create table core.measurements (
    device_id uuid references core.devices(id),
    ts timestamp without time zone not null,
    val float4 not null,
    primary key (device_id, ts)
);
