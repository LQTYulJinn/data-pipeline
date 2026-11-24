import argparse
from pathlib import Path
import duckdb

TABLES = ['fact_transactions', 'dim_card', 'dim_merchant', 'dim_date']


def export(database: Path, output: Path):
    output.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database), read_only=True)
    for table in TABLES:
        query = f'(select * from {table} order by 1)'
        csv_path = (output / f'{table}.csv').as_posix().replace("'", "''")
        parquet_path = (output / f'{table}.parquet').as_posix().replace("'", "''")
        connection.execute(f"copy {query} to '{csv_path}' (header, delimiter ',')")
        connection.execute(f"copy {query} to '{parquet_path}' (format parquet)")
    connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    export(args.database, args.output)
