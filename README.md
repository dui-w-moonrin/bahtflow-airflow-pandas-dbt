# BahtFlow: Airflow, Pandas, BigQuery, and dbt

BahtFlow is a production-minded batch ELT portfolio project for practising the data-engineering workflow used in fintech teams: schedule work with Airflow, validate incoming files with Pandas, load immutable raw data into BigQuery, and transform it into trusted models with dbt.

## Current phase

The repository contains a committed, reproducible source corpus: 360 daily batches across five regional sales feeds. It deliberately preserves duplicate transactions, conflicting records, and invalid values such as `N/A`. Those records are evidence that later Pandas and dbt stages must classify, quarantine, test, and document data rather than silently discard it.

Feature 01 adds the local Apache Airflow 3 orchestration runtime and a no-op `bahtflow_daily` DAG skeleton. GCS, BigQuery, Pandas ingestion, dbt transformation, and real data-quality behavior remain later features.

Read the [data contract](data/README.md) for the source layout and validation manifest. The fuller architecture is in [the design specification](docs/superpowers/specs/2026-09-01-bahtflow-airflow-pandas-dbt-design.md).

## Target architecture

```text
Daily regional gzip CSVs
          |
          v
Airflow (local Docker scheduler)
          |
          v
Pandas contract checks + load metadata
          |
          v
GCS immutable landing --> BigQuery raw append-only tables
                                  |
                                  v
                         dbt staging / quarantine / marts
```

Starting with a fixed, auditable source lets each subsequent component be developed and tested against the same data contract.

## Reproduce the source corpus

The committed corpus is generated from the earlier Databricks-edition monthly fixtures. The scripts use only the Python standard library.

```powershell
python scripts/split_regional_bootstrap_to_daily.py `
  --input-root C:\workspace\projects\bahtflow-databricks-declarative-pipeline\data\bootstrap_csv_gz `
  --output-root data\daily_regional_sales

python scripts/validate_daily_source.py `
  --root data\daily_regional_sales `
  --manifest data\daily_source_manifest.csv
```

For local checks, direct pytest to a repository-local temporary directory:

```powershell
python -m pytest tests -v --basetemp .pytest_tmp
```

## Local Docker environment

Feature 00 provides the shared PostgreSQL and toolbox foundation.

```powershell
Copy-Item .env.example .env
docker compose up -d --build postgres toolbox
docker compose ps
docker compose exec postgres pg_isready -U bahtflow -d bahtflow
docker compose exec toolbox python --version
docker compose down
```

`.env` is ignored by Git. Values in `.env.example` are local-development defaults only.

## Airflow 3 orchestration skeleton

Feature 01 runs Apache Airflow `3.3.1` with LocalExecutor, the existing PostgreSQL metadata backend, a separate DAG processor, and the API server on `http://localhost:8080`.

The SimpleAuthManager all-admin setting and example API/JWT/Fernet secrets are intentionally local-development settings. Do not reuse them for a shared or production deployment.

Initialize the metadata schema:

```powershell
docker compose up airflow-init
```

Start the Airflow components:

```powershell
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
```

Verify the runtime and DAG discovery:

```powershell
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow config get-value core executor
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected runtime values are Airflow `3.3.1` and `LocalExecutor`, with `bahtflow_daily` visible and no import errors.

Check the API health endpoint from PowerShell:

```powershell
Invoke-RestMethod http://localhost:8080/api/v2/monitor/health
```

Run one isolated logical-date smoke test outside the bounded backfill window:

```powershell
docker compose exec airflow-scheduler `
  airflow dags test bahtflow_daily 2025-07-25 --use-executor
```

Create the Feature 01 three-date backfill:

```powershell
docker compose exec airflow-scheduler `
  airflow backfill create `
    --dag-id bahtflow_daily `
    --from-date 2025-07-22 `
    --to-date 2025-07-24 `
    --max-active-runs 2
```

Inspect the resulting DAG runs:

```powershell
docker compose exec airflow-scheduler `
  airflow dags list-runs bahtflow_daily `
    --start-date 2025-07-22 `
    --end-date 2025-07-24
```

The DAG is intentionally all `EmptyOperator` tasks in Feature 01. It proves orchestration shape, Airflow 3 component wiring, logical-date execution, and explicit backfill only; it does not access GCS, BigQuery, Pandas, or dbt.

Stop the local environment when finished:

```powershell
docker compose down
```
