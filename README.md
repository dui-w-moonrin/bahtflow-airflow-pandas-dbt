# BahtFlow: Airflow, Pandas, BigQuery, and dbt

BahtFlow is a production-minded batch ELT portfolio project for practising the data-engineering workflow used in fintech teams: schedule work with Airflow, validate incoming files with Pandas, load immutable raw data into BigQuery, and transform it into trusted models with dbt.

## Current phase

This first release contains a committed, reproducible source corpus: 360 daily batches across five regional sales feeds. It deliberately preserves duplicate transactions, conflicting records, and invalid values such as `N/A`. Those records are evidence that later Pandas and dbt stages must classify, quarantine, test, and document data rather than silently discard it.

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

Airflow, Pandas checks, Google Cloud loading, and dbt models are intentionally delivered in later phases. Starting with a fixed, auditable source lets each subsequent component be developed and tested against the same data contract.

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

F00 provides a local-only Docker environment. Airflow, GCP, BigQuery, and dbt
are intentionally deferred to later features.

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
docker compose exec postgres pg_isready -U bahtflow -d bahtflow
docker compose exec toolbox python --version
docker compose down
```
