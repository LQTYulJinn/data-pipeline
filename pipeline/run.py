"""Local Sparkov subset pipeline. Raw identifiers never enter published outputs."""
import argparse
import csv
import json
import os
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

FIELDS = ['trans_date_trans_time','cc_num','merchant','category','amt','trans_num','is_fraud']


def subset(source: Path, dest: Path, limit: int):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with source.open(newline='', encoding='utf-8') as src, dest.open('w', newline='', encoding='utf-8') as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=FIELDS)
        writer.writeheader()
        for i, row in enumerate(reader):
            if i >= limit:
                break
            writer.writerow({key: row[key] for key in FIELDS})


def run(input_files: list[Path], output: Path, run_id: str, limit: int = 0):
    start = time.perf_counter()
    run_dir = output / run_id
    staging = output / ('.' + run_id + '.staging')
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    sources = []
    for i, input_file in enumerate(input_files):
        source = input_file
        if limit:
            source = staging / f'input_subset_{i}.csv'
            subset(input_file, source, limit)
        sources.append(source)
    t_ingest = time.perf_counter()
    parallelism = int(os.environ.get('SPARK_LOCAL_THREADS', '4'))
    shuffle_partitions = int(os.environ.get('SPARK_SHUFFLE_PARTITIONS', '8'))
    driver_memory = os.environ.get('SPARK_DRIVER_MEMORY', '4g')
    spark = (SparkSession.builder.master(f'local[{parallelism}]').appName('sparkov-pipeline')
             .config('spark.sql.shuffle.partitions', str(shuffle_partitions))
             .config('spark.driver.memory', driver_memory)
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.driver.bindAddress', '127.0.0.1').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    frames = [spark.read.option('header', True).csv(str(source)).select(*FIELDS)
              .withColumn('source_file', F.lit(input_file.name))
              for source, input_file in zip(sources, input_files)]
    raw = frames[0]
    for frame in frames[1:]:
        raw = raw.unionByName(frame)
    source_counts = {r['source_file']: r['count'] for r in raw.groupBy('source_file').count().collect()}
    before = sum(source_counts.values())
    parsed = (raw.select(*[F.col(c) for c in FIELDS], F.col('source_file'))
        .withColumn('event_time', F.to_timestamp('trans_date_trans_time'))
        .withColumn('amount', F.col('amt').cast('double'))
        .withColumn('fraud_label', F.col('is_fraud').cast('integer')))
    null_pred = (F.col('event_time').isNull() | F.col('cc_num').isNull() |
                 F.col('merchant').isNull() | F.col('category').isNull() |
                 F.col('trans_num').isNull() | F.col('amount').isNull() |
                 F.col('fraud_label').isNull())
    range_pred = ((F.col('amount') < 0) | (F.col('amount') > 100000) |
                  (~F.col('fraud_label').isin(0, 1)))
    nulls = parsed.filter(null_pred).count()
    ranges = parsed.filter(range_pred).count()
    valid = parsed.filter(~null_pred & ~range_pred)
    duplicate_count = valid.count() - valid.dropDuplicates(['trans_num']).count()
    valid = valid.dropDuplicates(['trans_num'])
    # Salt comes from the environment. The synthetic fixture uses a public test salt.
    salt = os.environ.get('CARD_HASH_SALT', 'synthetic-fixture-only')
    if any(input_file.name != 'synthetic.csv' for input_file in input_files) and salt == 'synthetic-fixture-only':
        raise ValueError('Set CARD_HASH_SALT for Sparkov runs')
    valid = valid.withColumn('card_id', F.sha2(F.concat(F.lit(salt), F.col('cc_num')), 256))
    # Use event time plus transaction id as a total order; only prior rows are counted.
    w = Window.partitionBy('card_id').orderBy(F.col('event_time').cast('long'), 'trans_num')
    rows = (valid.withColumn('prior_txn_count', F.row_number().over(w) - 1)
            .withColumn('prior_1h_count', F.expr("count(*) over (partition by card_id order by cast(event_time as long) range between 3600 preceding and 1 preceding)"))
            .withColumn('prior_24h_count', F.expr("count(*) over (partition by card_id order by cast(event_time as long) range between 86400 preceding and 1 preceding)"))
            .select(F.col('trans_num').alias('transaction_id'), 'event_time', 'card_id', 'source_file',
                    F.col('merchant').alias('merchant_id'), 'category', 'amount', 'fraud_label',
                    'prior_txn_count', 'prior_1h_count', 'prior_24h_count'))
    # Materialize Parquet without exposing raw card numbers.
    bronze_dir = staging / 'bronze'
    rows.coalesce(parallelism).write.mode('overwrite').parquet(str(bronze_dir))
    after = rows.count()
    t_transform = time.perf_counter()
    quality = {'run_id': run_id, 'source_rows': before, 'source_file_rows': source_counts,
               'rows_after': after,
               'null_rows_before': nulls, 'duplicate_ids_before': duplicate_count,
               'out_of_range_rows_before': ranges,
               'null_rows_after': rows.filter(F.col('card_id').isNull() | F.col('event_time').isNull()).count(),
               'duplicate_ids_after': after - rows.select('transaction_id').distinct().count(),
               'out_of_range_rows_after': rows.filter((F.col('amount') < 0) | (F.col('amount') > 100000) | (~F.col('fraud_label').isin(0, 1))).count()}
    (staging / 'quality.json').write_text(json.dumps(quality, indent=2))
    t_quality = time.perf_counter()
    spark.stop()
    if any(quality[k] for k in ['null_rows_after','duplicate_ids_after','out_of_range_rows_after']) or after == 0:
        raise RuntimeError('Quality gate failed; staging retained and publication skipped')
    if limit:
        for source in sources:
            source.unlink()
    if run_dir.exists():
        shutil.rmtree(run_dir)
    staging.rename(run_dir)
    quality['spark_config'] = {'local_threads': parallelism, 'shuffle_partitions': shuffle_partitions, 'driver_memory': driver_memory}
    quality['seconds'] = {'ingest_subset': round(t_ingest-start, 3), 'spark_transform': round(t_transform-t_ingest, 3), 'quality': round(t_quality-t_transform, 3), 'publish': round(time.perf_counter()-t_quality, 3)}
    (run_dir / 'quality.json').write_text(json.dumps(quality, indent=2))
    return quality


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--input', type=Path, required=True, action='append')
    p.add_argument('--output', type=Path, default=Path('output'))
    p.add_argument('--run-id', default='local')
    p.add_argument('--limit', type=int, default=0)
    args = p.parse_args()
    print(json.dumps(run([p.resolve() for p in args.input], args.output.resolve(), args.run_id, args.limit), indent=2))
