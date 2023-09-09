import hashlib
import time
from multiprocessing import Pool
from uuid import UUID

import polars as pl
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

from meterdata.loader.common import (
    write_execution_time,
    get_row_group,
)

PG_DEVICE_TABLE = "core.devices"
PG_MEASUREMENTS_TABLE = "core.measurements"

# i've copied some stuff for the container from the pg docker page...
PG_CREDENTIALS = {
    "host": "localhost",
    "database": "meterdata",
    "user": "postgres",
    "password": "mysecretpassword",
}


def pg_truncate_tables():
    start = time.perf_counter()

    with psycopg2.connect(**PG_CREDENTIALS) as conn:
        cur = conn.cursor()
        cur.execute(f"truncate table core.devices cascade")
        conn.commit()

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pg_truncate_tables.__name__,
    )


def pg_insert_devices(devices: dict[str, str]):
    start = time.perf_counter()

    with psycopg2.connect(**PG_CREDENTIALS) as conn:
        cur = conn.cursor()
        qry = "insert into core.devices(id, name) values %s on conflict do nothing"
        execute_values(cur, qry, devices)
        conn.commit()

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pg_insert_devices.__name__,
    )


def pg_insert_measurements(
    measurements: dict[str, str, float], year: int, row_group: int
):
    start = time.perf_counter()

    with psycopg2.connect(**PG_CREDENTIALS) as conn:
        cur = conn.cursor()
        qry = "insert into core.measurements(device_id, ts, val) values %s"
        execute_values(cur, qry, measurements)
        conn.commit()

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pg_insert_measurements.__name__,
        n_rows=len(measurements),
        year=year,
        row_group=row_group,
    )


def pg_get_device_names(table: pa.Table):
    df = pl.from_arrow(table).select("id").unique().to_pandas()
    df["name"] = df["id"]
    df["id"] = df["name"].apply(
        lambda x: str(UUID(hashlib.md5(x.encode()).hexdigest()))
    )
    return df.to_numpy().tolist()


def pg_set_device_id(table: pa.Table):
    df = pl.from_arrow(table).to_pandas()
    df["device_id"] = df["id"].apply(
        lambda x: str(UUID(hashlib.md5(x.encode()).hexdigest()))
    )
    return df[["device_id", "ts", "val"]].to_numpy().tolist()


def pg_insert_row_group(args):
    start = time.perf_counter()

    # load the table filter by specific year.
    file_path, year, row_group = args
    table = get_row_group(file_path=file_path, row_group=row_group)

    # derive and load devices from table.
    devices = pg_get_device_names(table=table)
    pg_insert_devices(devices=devices)

    # derive and load measurements from table.
    measurements = pg_set_device_id(table=table)
    pg_insert_measurements(measurements=measurements, year=year, row_group=row_group)

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pg_insert_row_group.__name__,
    )


def pg_insert_file_content(table_name: str, year: int):
    start = time.perf_counter()

    file_path = f"data/ready/{table_name}_{year}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    with Pool(4) as pool:
        pool.map(
            pg_insert_row_group,
            [(file_path, year, i) for i in range(parquet_file.num_row_groups)],
        )

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pg_insert_file_content.__name__,
    )
