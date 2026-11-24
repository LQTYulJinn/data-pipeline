"""LocalExecutor DAG. Downstream tasks run only after successful quality checks."""
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path('/opt/airflow/project')
OUT = ROOT / 'output' / 'airflow'


def ingest():
    source = Path(os.environ['SPARKOV_SOURCE'])
    if not source.is_file():
        raise FileNotFoundError(source)
    OUT.mkdir(parents=True, exist_ok=True)


def transform():
    command = ['python', '-m', 'pipeline.run', '--input', os.environ['SPARKOV_SOURCE']]
    test_source = Path(os.environ.get('SPARKOV_TEST_SOURCE', ''))
    if test_source.is_file():
        command += ['--input', str(test_source)]
    limit = int(os.environ.get('PIPELINE_ROW_LIMIT', '0'))
    if limit:
        command += ['--limit', str(limit)]
    command += ['--output', str(ROOT / 'output'), '--run-id', 'airflow']
    subprocess.run(command,
                   cwd=ROOT, check=True)


def quality_checks():
    report = json.loads((OUT / 'quality.json').read_text())
    if report['rows_after'] == 0 or any(report[k] for k in
        ['null_rows_after', 'duplicate_ids_after', 'out_of_range_rows_after']):
        raise ValueError('Quality gate failed')
    result = subprocess.run(['dbt', 'build', '--project-dir', str(ROOT / 'dbt'),
                             '--profiles-dir', str(ROOT / 'dbt')], cwd=ROOT,
                            capture_output=True, text=True)
    print(result.stdout[-6000:], result.stderr[-2000:])  # subprocess output is otherwise lost from the task log
    result.check_returncode()


def publish():
    subprocess.run(['python', '-m', 'pipeline.export', '--database', str(OUT / 'warehouse.duckdb'),
                    '--output', str(OUT / 'gold')], cwd=ROOT, check=True)


with DAG('card_pipeline', start_date=datetime(2024, 1, 1), schedule=None,
         catchup=False, max_active_runs=1) as dag:
    a = PythonOperator(task_id='ingest', python_callable=ingest)
    b = PythonOperator(task_id='transform', python_callable=transform)
    c = PythonOperator(task_id='quality_checks', python_callable=quality_checks)
    d = PythonOperator(task_id='publish', python_callable=publish)
    a >> b >> c >> d
