# Local card transaction data pipeline

Sparkov CSV → PySpark cleaned, past-only bronze Parquet → dbt/DuckDB bronze, silver and gold tables → CSV/Parquet for Power BI. Airflow defines `ingest → transform → quality_checks → publish`; failed Spark or dbt checks stop publication.

## Source and scope

The source is the **simulated Sparkov** `fraudTrain.csv` and `fraudTest.csv` in the neighboring `fraud-risk/data/sparkov` directory, originally from the public Kaggle Sparkov fraud-detection dataset (CC0, per that project's README). The verified full run combines all 1,852,394 rows; the earlier measured subset used the first 20,000 training rows by file order. ULB's anonymized 48-hour data was inspected as an alternative but has no card or merchant identifier, so it cannot support actual card velocity or merchant dimensions. Sparkov is synthetic, so these results do not estimate real fraud rates or model performance.

Only event time, card number, merchant, category, amount, transaction ID and fraud label are read. Card numbers are SHA-256 hashed with a private salt before bronze publication; raw subsets are deleted after transformation. The raw source remains outside this repo. Set a stable private `CARD_HASH_SALT` for comparable card keys across runs and keep it out of Git. `fixtures/synthetic.csv` contains fictional identifiers and is the sole committed data file. `data/`, `output/`, `state/`, logs, databases and secrets are gitignored.

## Reproduce locally on Windows

Run from this directory. Python 3.11, Java 17, and Hadoop Windows `winutils.exe` plus `hadoop.dll` are required for Windows PySpark. On this machine the Python environment is `C:\dpvenv`, Java 17 is provided by `jdk4py==17.0.9.2` inside it, and Hadoop helpers are in `C:\hadoop\bin`. Install project dependencies with `python -m pip install -r requirements.txt`. The portable paths below can be adjusted for another machine.

```powershell
$env:JAVA_HOME='C:\dpvenv\Lib\site-packages\jdk4py\java-runtime'
$env:HADOOP_HOME='C:\hadoop'
$env:PATH='C:\hadoop\bin;'+$env:PATH
$env:SPARK_LOCAL_IP='127.0.0.1'
$env:CARD_HASH_SALT='<your private stable salt>'
C:\dpvenv\Scripts\python.exe -m pipeline.run --input 'C:\Users\thinh\Demo\Personal\fraud-risk\data\sparkov\fraudTrain.csv' --input 'C:\Users\thinh\Demo\Personal\fraud-risk\data\sparkov\fraudTest.csv' --output output --run-id full-combined
$env:PIPELINE_DB='output/full-combined/warehouse.duckdb'
$env:BRONZE_GLOB='output/full-combined/bronze/*.parquet'
C:\dpvenv\Scripts\dbt.exe build --project-dir dbt --profiles-dir dbt
C:\dpvenv\Scripts\python.exe -m pipeline.export --database output/full-combined/warehouse.duckdb --output output/full-combined/gold
C:\dpvenv\Scripts\python.exe -m pipeline.audit --run-dir output/full-combined
```

For a sample use `--limit 20000` with just the train `--input`; this limit applies per source when multiple inputs are given. For the CI fixture use `--input fixtures/synthetic.csv --run-id ci`; set `PIPELINE_DB` and `BRONZE_GLOB` to `output/ci/...` before `dbt build`. The workflow in `.github/workflows/ci.yml` uses Linux Java 17 and runs these equivalent Spark, dbt build, fixture assertion and export commands. The public repository is `https://github.com/LQTYulJinn/data-pipeline`; its initial fixture workflow [passed](https://github.com/LQTYulJinn/data-pipeline/actions/runs/45382305822). The full Sparkov run was measured locally, not in GitHub Actions.

## Airflow Docker Compose

Set `SPARKOV_DIR` to the absolute directory containing both CSV files, `CARD_HASH_SALT` to a private value, and `AIRFLOW_DB_PASSWORD` to a local Postgres password; keep these values out of Git. Then run `docker compose up --build` and trigger `card_pipeline` in the UI at localhost:8080. Compose mounts the source read-only at `/opt/airflow/source`, project at `/opt/airflow/project`, and uses a local Postgres metadata service for `LocalExecutor`. The DAG defaults to the full combined run; set `PIPELINE_ROW_LIMIT` to a positive number for a per-file sample. `quality_checks` runs `dbt build`, so failing tests block `publish`. The Dockerfile installs Java 17 and project Python dependencies. The Compose DAG was executed in Docker Desktop (Airflow 2.10.5, LocalExecutor). Measured task times (wall clock from Airflow task start/end):

| Run | ingest | transform | quality_checks (dbt 23/23 pass) | publish | DAG total |
|---|---|---|---|---|---|
| Sample: `PIPELINE_ROW_LIMIT=200000` per file = 400,000 rows; WSL capped at 6 GB / 4 CPUs / 4 GB swap, Spark driver 2g | 1.0 s | 83.2 s | 14.0 s | 2.9 s | 115.2 s |
| Full: 1,852,394 rows; earlier run on uncapped WSL (8 GB, 8 CPUs), Spark driver 4g | 1.0 s | 135.2 s | 19.0 s | 8.1 s | 169.7 s |

Caveats: single runs, no repeats. The first full attempt failed in `quality_checks` for a reason that was not captured (dbt output was not in the task log); two later full runs passed, so treat that failure as unexplained. The DAG now prints dbt output into the task log. The host restarted twice during these sessions while Docker/Spark/Power BI were running; the cause was not established, so the sample run uses the memory caps above and a 2g driver (`SPARK_DRIVER_MEMORY`, default 2g in Compose).

## Power BI

Import the four `output/full-combined/gold/*.csv` files (or Parquet files) using Power BI Desktop's Get Data. Relate `fact_transactions.card_id → dim_card.card_id`, `merchant_id → dim_merchant.merchant_id`, and `date_key → dim_date.date_key`, many to one with single-direction filtering. Suggested DAX measures:

```dax
Transactions = COUNTROWS(fact_transactions)
Amount = SUM(fact_transactions[amount])
Fraud Transactions = CALCULATE([Transactions], fact_transactions[fraud_label] = 1)
Fraud Rate = DIVIDE([Fraud Transactions], [Transactions])
```

`powerbi/card_fraud.pbip` is a Power BI Project (TMDL semantic model + one report page) with the four gold CSVs, the three many-to-one relationships above, and measures `Transaction Count`, `Total Amount`, `Fraud Count`, `Fraud Rate`, `Fraud Amount`, `Avg Ticket`. It was opened in Power BI Desktop and refreshed against `output/full-combined/gold`; the cards showed 2M transactions, 0.52% fraud rate and $5M fraud amount, matching a direct CSV count (1,852,394 rows, 9,651 fraud, 0.521%, $5.12M). Screenshot: `docs/powerbi-dashboard.png`. The CSV folder is the `GoldFolder` parameter in `powerbi/card_fraud.SemanticModel/definition/model.tmdl` and is an absolute path on this machine; edit it first. No `.pbix` binary was produced (Desktop's Save As was not automated); use File > Save As in Desktop to make one.

## Measured full local run (2026-11-24)

Machine: Microsoft Windows 11 Home Single Language 10.0.26300; 8 logical CPUs; 16,910,155,776 bytes (15.75 GiB) physical RAM; 17,072,504,832 bytes (15.90 GiB) free on C: before the attempt. PySpark 3.5.4; Java Temurin 17.0.9; Spark `local[4]`, 8 shuffle partitions, 4 GiB driver. Spark's session timezone is UTC so the source's naive timestamp strings are preserved in Parquet and DuckDB. The source CSVs are 351,238,196 bytes (train) and 150,354,339 bytes (test).

| Stage | Measured result | Elapsed |
|---|---:|---:|
| Input selection | 1,296,675 train + 555,719 test | 0.000 s (no subset copy) |
| PySpark transform | 1,852,394 bronze rows | 41.900 s |
| PySpark quality | 0 residual failures | 14.098 s |
| Bronze publication | 4 Parquet files | 0.375 s |
| dbt build | 6 models, 17 data tests passed, 0 failed | 10.698 s wall time |
| Gold export | 4 CSV and 4 Parquet | 2.714 s |

Bronze, silver and gold fact tables each have **1,852,394 rows**. The fact table retains source counts of 1,296,675 train and 555,719 test; 555,703 test rows have at least one prior transaction for their card in the combined chronology. Gold dimensions have 999 cards, 693 merchants and 730 dates. Event times range from `2019-01-01 00:00:18` to `2020-12-31 23:59:34`. The quality report has zero null, duplicate ID and out-of-range rows both before and after cleaning. Bronze Parquet is **89,429,507 bytes (85.287 MiB)** across four files. The four gold Parquet files together are **102,294,747 bytes (97.556 MiB)**; the fact Parquet alone is 102,183,828 bytes. These scopes exclude CSV exports, DuckDB storage and Spark temporary files. Full machine-readable counts, sizes and quality are in `output/full-combined/audit.json`; dbt results are in `output/full-combined/dbt-build.log`. Both are local ignored artifacts.

## Earlier measured 20,000-row subset (2026-11-24)

| Stage | Result | Elapsed |
|---|---:|---:|
| CSV subset | 20,000 rows | 0.170 s |
| PySpark transform | 20,000 rows | 10.975 s |
| PySpark quality | 0 residual failures | 1.245 s |
| Bronze publication | Parquet | 0.452 s |
| dbt build | 6 models, 17 tests passed | 8.279 s wall time |
| Gold export | 4 CSV and 4 Parquet | 0.395 s |

The subset quality report is `output/local/quality.json`: before and after row counts, null rows, duplicate transaction IDs, and out-of-range amount or fraud-label rows. All four defect counts were zero before and after on this subset. Gold counts: 20,000 facts, 909 cards, 693 merchants, 13 dates. These earlier artifacts were produced before the UTC parsing correction and should be regenerated for date analysis. The synthetic fixture was rerun after the full-run changes and passed Spark, all 17 dbt tests and five explicit history assertions. Sparkov's `unix_time` is deliberately not used because the neighboring project notes inconsistency with its timestamp string. Velocity windows use strictly earlier timestamps, excluding the current row and same-second peers; `prior_txn_count` uses transaction ID to break timestamp ties. Windows are 1 hour and 24 hours.
