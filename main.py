import subprocess
import time

from meterdata.loader.clickhouse import (
    ch_get_row_count,
    ch_truncate_table,
    ch_insert_file_content,
    CH_BASE_TABLE,
)
from meterdata.loader.common import (
    derive_parquet_data_from_csv_with_ddb_query,
    create_empty_exec_time_file,
    write_execution_time,
)

if __name__ == "__main__":
    create_empty_exec_time_file()
    derive_parquet_data_from_csv_with_ddb_query(table_name=f"halfhourly_dataset")

    ch_truncate_table(table=CH_BASE_TABLE)
    for y in [2011, 2012, 2013, 2014]:
        ch_truncate_table(table=CH_BASE_TABLE)
        row_count = ch_get_row_count(table=CH_BASE_TABLE)
        ch_insert_file_content(table_name="halfhourly_dataset", year=y)

        start = time.perf_counter()
        subprocess.run("cd metermodel; dbt run", shell=True, text=True)
        write_execution_time(
            exec_time=time.perf_counter() - start,
            function_name="dbt_run",
        )
