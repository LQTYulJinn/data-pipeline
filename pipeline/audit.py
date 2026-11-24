"""Write a reproducible cross-layer count and storage audit for one run."""
import argparse
import json
from pathlib import Path

import duckdb


def sizes(directory):
    files = list(directory.glob('*.parquet'))
    size = sum(path.stat().st_size for path in files)
    return {'files': len(files), 'bytes': size, 'MiB': round(size / 1048576, 3)}


def main(run_dir):
    db = duckdb.connect(str(run_dir / 'warehouse.duckdb'), read_only=True)
    scalar = lambda sql: db.execute(sql).fetchone()[0]
    audit = {
        'quality': json.loads((run_dir / 'quality.json').read_text()),
        'bronze_rows': scalar(f"select count(*) from read_parquet('{(run_dir / 'bronze' / '*.parquet').as_posix()}')"),
        'silver_rows': scalar('select count(*) from silver_transactions'),
        'fact_rows': scalar('select count(*) from fact_transactions'),
        'fact_source_rows': dict(db.execute('select source_file, count(*) from fact_transactions group by 1').fetchall()),
        'test_rows_with_prior_card_history': scalar("select count(*) from fact_transactions where source_file = 'fraudTest.csv' and prior_txn_count > 0"),
        'cards': scalar('select count(*) from dim_card'),
        'merchants': scalar('select count(*) from dim_merchant'),
        'dates': scalar('select count(*) from dim_date'),
        'min_event_time': str(scalar('select min(event_time) from fact_transactions')),
        'max_event_time': str(scalar('select max(event_time) from fact_transactions')),
        'bronze_parquet': sizes(run_dir / 'bronze'),
        'gold_parquet': sizes(run_dir / 'gold'),
        'gold_files': {path.name: path.stat().st_size for path in sorted((run_dir / 'gold').glob('*'))},
    }
    db.close()
    (run_dir / 'audit.json').write_text(json.dumps(audit, indent=2))
    print(json.dumps(audit, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-dir', type=Path, required=True)
    args = parser.parse_args()
    main(args.run_dir.resolve())
