import time
from multiprocessing import Pool

from clickhouse_driver import Client
from pyarrow import parquet as pq

from meterdata.loader.common import (
    write_execution_time,
    get_row_group,
    pyarrow_table_to_pylist,
)

CH_BASE_TABLE = "meterdata_raw.meter_halfhourly_dataset"


def ch_truncate_table(table: str):
    start = time.perf_counter()

    with Client("localhost") as client:
        client.execute(query=f"truncate table {table}")

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=ch_truncate_table.__name__,
    )


def ch_insert_data(data: list[str, str, float], year: int, row_group: int):
    start = time.perf_counter()

    with Client("localhost") as client:
        client.execute(
            query="insert into meterdata_raw.meter_halfhourly_dataset (id, ts, val) values",
            params=data,
            types_check=True,
        )

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=ch_insert_data.__name__,
        n_rows=len(data),
        year=year,
        row_group=row_group,
    )


def ch_insert_row_group(args):
    start = time.perf_counter()

    file_path, year, row_group = args
    table = get_row_group(file_path=file_path, row_group=row_group)
    data = pyarrow_table_to_pylist(table=table)
    ch_insert_data(data=data, year=year, row_group=row_group)

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=ch_insert_row_group.__name__,
    )


def ch_insert_file_content(table_name: str, year: int):
    start = time.perf_counter()

    file_path = f"data/ready/{table_name}_{year}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    with Pool(4) as pool:
        pool.map(
            ch_insert_row_group,
            [(file_path, year, i) for i in range(parquet_file.num_row_groups)],
        )

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=ch_insert_file_content.__name__,
    )
