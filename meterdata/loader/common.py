import time
from pathlib import Path

import duckdb
import pyarrow as pa
from pyarrow import compute as pc, parquet as pq


def create_empty_exec_time_file():
    with open("exec_time.csv", "w") as f:
        f.write("function,exec_time\n")


def write_execution_time(exec_time: float, function_name: str):
    """looks like decorators are not working easy with multiprocessing"""
    with open("exec_time.csv", "a") as f:
        f.write(f"{function_name},{exec_time}\n")


def derive_parquet_data_from_csv_with_ddb_query(table_name: str):
    start = time.perf_counter()

    with duckdb.connect(database=":memory:") as conn:
        query_file_path = Path("queries") / "ddb" / f"{table_name}.sql"
        with open(query_file_path) as qry_file:
            conn.execute(qry_file.read())

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=derive_parquet_data_from_csv_with_ddb_query.__name__,
    )


def filter_table_by_year(table: pa.Table, year: int):
    start = time.perf_counter()

    year_array = pc.year(table.column("ts"))
    mask = pc.equal(year_array, pa.scalar(year))
    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=filter_table_by_year.__name__,
    )
    return table.filter(mask)


def get_data_from_row_group(file_path, year, row_group_index):
    start = time.perf_counter()

    parquet_file = pq.ParquetFile(file_path)  # Open file within the worker process!
    table = parquet_file.read_row_group(row_group_index)
    table = filter_table_by_year(table=table, year=year)
    data = pyarrow_table_to_pydict(table=table)

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=get_data_from_row_group.__name__,
    )
    return data


def pyarrow_table_to_pydict(table: pa.Table):
    start = time.perf_counter()
    data = list(zip(*[tuple(row) for row in table.to_pydict().values()]))

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pyarrow_table_to_pydict.__name__,
    )
    return data
