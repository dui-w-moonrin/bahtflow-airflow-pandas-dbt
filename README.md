# BahtFlow: Airflow, Pandas, BigQuery, and dbt

BahtFlow is a production-minded batch ELT portfolio project for practising the data-engineering workflow used in fintech teams: schedule work with Airflow, validate incoming files with Pandas, load immutable raw data into BigQuery, and transform it into trusted models. The v1 roadmap keeps business transformation in Pandas; dbt is deferred to a later v2 evolution.

## Current phase

The repository contains a committed, reproducible source corpus: 360 daily batches across five regional sales feeds. It deliberately preserves duplicate transactions, conflicting records, and invalid values such as `N/A` so later stages can classify and quarantine them instead of silently discarding evidence.

Feature 01 provides the local Apache Airflow 3 orchestration runtime and a no-op `bahtflow_daily` DAG skeleton. Feature 02 provides credential-safe immutable GCS landing with ADC impersonation and SHA-256 conflict protection. Feature 03 adds the BigQuery warehouse boundary: four datasets plus two empty partitioned raw tables, created and verified idempotently. Feature 04 adds one-date Pandas intake and idempotent raw loading from GCS into those BigQuery raw tables. Feature 05 adds Pandas data-quality classification into typed accepted rows and auditable raw quarantine rows with deterministic batch-local duplicate handling and idempotent BigQuery persistence.

Read the [data contract](data/README.md) for the source layout and validation manifest. The current v1 roadmap is in [the Pandas v1 design](docs/superpowers/specs/2026-09-02-pandas-v1-roadmap-design.md).

## Target architecture

```text
Daily regional gzip CSVs
          |
          v
GCS immutable landing
          |
          v
Airflow logical-date orchestration
          |
          v
Pandas intake / DQ / FX transformation
          |
          v
BigQuery raw -> accepted/quarantine -> fact/marts
```

## Reproduce the source corpus

```powershell
python scripts/split_regional_bootstrap_to_daily.py `
  --input-root C:\workspace\projects\bahtflow-databricks-declarative-pipeline\data\bootstrap_csv_gz `
  --output-root data\daily_regional_sales

python scripts/validate_daily_source.py `
  --root data\daily_regional_sales `
  --manifest data\daily_source_manifest.csv
```

For local checks:

```powershell
python -m pytest tests -v --basetemp .pytest_tmp
```

## Local Docker environment

```powershell
Copy-Item .env.example .env
docker compose up -d --build postgres toolbox
docker compose ps
docker compose exec postgres pg_isready -U bahtflow -d bahtflow
docker compose exec toolbox python --version
docker compose down
```

`.env` is ignored by Git. Values in `.env.example` are local-development examples only.

## Feature 01: Airflow 3 orchestration skeleton

Feature 01 runs Apache Airflow `3.3.1` with LocalExecutor, PostgreSQL metadata, a separate DAG processor, and the API server on `http://localhost:8080`.

```powershell
docker compose up airflow-init
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps

docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow config get-value core executor
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected runtime values are Airflow `3.3.1` and `LocalExecutor`, with `bahtflow_daily` visible and no import errors.

The DAG is intentionally all `EmptyOperator` tasks in Feature 01. It proves orchestration shape and logical-date execution only.

## Feature 02: GCS landing boundary

Feature 02 keeps Google credentials outside the repository and image. The dedicated `gcp-toolbox` Compose service receives the host ADC file as a read-only bind mount.

The landing layout is:

```text
gs://<BAHTFLOW_GCS_BUCKET>/
├── transactions/business_date=YYYY-MM-DD/sales_{region}_YYYYMMDD.csv.gz
├── fx/YYYY/MM/fx_YYYYMMDD.csv
└── manifests/
    ├── daily_source_manifest.csv
    └── fx_manifest.csv
```

Every uploaded object stores the exact local file SHA-256 in custom metadata key `bahtflow-source-sha256`. An absent object uploads, an existing object with the same checksum is skipped, and a missing/different checksum fails rather than overwriting data.

Required local configuration:

```text
BAHTFLOW_GCP_PROJECT=<project-id>
BAHTFLOW_GCS_BUCKET=<globally-unique-bucket-name>
BAHTFLOW_GCP_LOCATION=asia-southeast1
BAHTFLOW_RUNTIME_SERVICE_ACCOUNT=bahtflow-runtime@<project-id>.iam.gserviceaccount.com
GOOGLE_ADC_HOST_PATH=C:/Users/<windows-user>/AppData/Roaming/gcloud/application_default_credentials.json
```

Create or verify the bucket, grant bucket-scoped object access, and switch ADC to the impersonated runtime identity:

```powershell
docker compose --profile gcp build gcp-toolbox

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_gcs --create-if-missing

gcloud storage buckets add-iam-policy-binding gs://$bucketName `
  --member="serviceAccount:$runtimeSa" `
  --role="roles/storage.objectAdmin"

gcloud auth application-default login `
  --impersonate-service-account=$runtimeSa
```

Smoke and live verification:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.upload_landing_sources --smoke

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.verify_gcs_live
```

The full landing uploader is:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.upload_landing_sources
```

## Feature 03: BigQuery warehouse bootstrap

Feature 03 creates the minimum warehouse boundary needed by later Pandas ingestion. It creates exactly four datasets:

```text
bahtflow_raw
bahtflow_ops
bahtflow_analytics
bahtflow_public
```

It creates exactly two raw tables in `bahtflow_raw`:

```text
transactions  partitioned DAY by batch_date
fx_rates      partitioned DAY by rate_date
```

The bootstrap is rerun-safe. Existing resources are verified for dataset location, exact table schema, partition field, and DAY partition type. It never silently replaces a mismatched table.

### 1. Enable BigQuery and grant the runtime identity

```powershell
gcloud services enable bigquery.googleapis.com `
  --project=$env:BAHTFLOW_GCP_PROJECT

gcloud projects add-iam-policy-binding $env:BAHTFLOW_GCP_PROJECT `
  --member="serviceAccount:$env:BAHTFLOW_RUNTIME_SERVICE_ACCOUNT" `
  --role="roles/bigquery.user"

gcloud projects add-iam-policy-binding $env:BAHTFLOW_GCP_PROJECT `
  --member="serviceAccount:$env:BAHTFLOW_RUNTIME_SERVICE_ACCOUNT" `
  --role="roles/bigquery.dataEditor"
```

The runtime identity remains below BigQuery Admin. Python clients continue to use normal ADC impersonation plus the explicit project ID from `BAHTFLOW_GCP_PROJECT`; no service-account JSON key is stored in the repository or image.

### 2. Build the GCP toolbox

```powershell
docker compose --profile gcp build gcp-toolbox
```

### 3. Bootstrap BigQuery twice

First run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_bigquery
```

On a fresh project the four datasets and two raw tables report `status=created`.

Run the same command again:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_bigquery
```

The second run must report `status=verified` for all four datasets and both raw tables. This is the Feature 03 idempotency evidence.

### 4. Run the live verifier

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.verify_bigquery_live
```

Required Feature 03 evidence:

```text
datasets=4
raw_tables=2
transactions_partition=batch_date
fx_rates_partition=rate_date
transactions_rows=0
fx_rates_rows=0
```

Feature 04 owns GCS discovery, Pandas reads/validation, source metadata, and idempotent BigQuery raw loading. Feature 03 intentionally stops before ingestion.

## Feature 04: Pandas intake + idempotent raw load

Feature 04 loads one logical date from immutable GCS into the existing BigQuery raw tables through Pandas.

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.load_raw_batch --batch-date 2025-07-22
```

Transaction intake requires exactly five canonical regional files: `bkk`, `central`, `north`, `northeast`, and `south`. Same-day FX is optional; if no published FX file exists for the logical date, the run reports `fx_status=NO_NEW_RATE` rather than failing the transaction batch.

Before Pandas reads a source object, Feature 04 verifies its bytes against GCS custom metadata `bahtflow-source-sha256`. Transaction business fields (`txn`, `dtts`, `amount`, `currency`) are preserved as raw strings, including literal `N/A`, blanks, malformed timestamps, malformed amounts, and lowercase currency values.

Idempotency uses deterministic `source_row_id = SHA256(source_file | source_checksum | source_row_number)`. Existing IDs are queried only for the target BigQuery partition, Pandas anti-filters already loaded rows, and unseen rows are appended with `WRITE_APPEND`. An unchanged rerun must report `tx_inserted_rows=0` and, when same-day FX exists, `fx_inserted_rows=0` while partition row counts remain unchanged.

Feature 04 intentionally does not classify business-quality failures, split accepted/quarantine records, resolve effective FX, or perform currency conversion. Those transformations belong to later v1 features.

## Feature 05: Pandas data-quality classification

Feature 05 reads exactly one `bahtflow_raw.transactions` partition and classifies every raw source row with Pandas into one of two BigQuery outputs:

```text
bahtflow_analytics.transactions_accepted   typed + canonical
bahtflow_ops.transactions_quarantine       raw evidence + ordered reason_codes
```

Bootstrap or verify the two output tables:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification
```

Classify one batch:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Accepted rows use canonical `txn`, typed BigQuery `DATETIME`, BigQuery `NUMERIC` backed by Python `Decimal`, canonical `THB`/`USD`/`EUR`, and validated region values. Quarantine rows preserve the original raw business fields and source lineage and add deterministic ordered `reason_codes`; no partially canonicalized business values overwrite the raw evidence.

Base-invalid rows are quarantined before duplicate comparison. Duplicate handling is batch-local in v1: canonical `txn` is the duplicate key, exact canonical payload replays keep the lowest `(source_file, source_row_number)` and quarantine later copies as `DUPLICATE_REPLAY`, while conflicting canonical payloads quarantine every base-valid occurrence as `DUPLICATE_CONFLICT`.

Persistence is append-only and idempotent by target partition plus `source_row_id`. An unchanged rerun inserts zero accepted rows and zero quarantine rows while persisted partition counts stay unchanged. If accepted rows were written but the quarantine write failed, a retry skips already-persisted accepted IDs and appends only the missing quarantine rows.

For the live acceptance batch `2025-07-22`, the observed source partition contained `8,978` raw rows. The classification run measured `8,803` accepted rows and `175` quarantine rows, satisfying `8,978 = 8,803 + 175`; the immediate unchanged rerun inserted `0` accepted and `0` quarantine rows. The observed quarantine reason distribution was:

```text
DUPLICATE_CONFLICT  52
DUPLICATE_REPLAY    29
INVALID_AMOUNT      51
INVALID_CURRENCY    21
NEGATIVE_AMOUNT     22
```

Live duplicate evidence for that batch showed one `DUPLICATE_REPLAY` transaction with exactly one accepted winner and one `DUPLICATE_CONFLICT` transaction with zero accepted occurrences.

Feature 05 intentionally stops before effective-FX resolution and currency conversion (Feature 06) and before production Airflow wiring/backfill (Feature 07).

## Development checks

```powershell
pytest

python -m py_compile `
  pipeline/bigquery_contract.py `
  pipeline/bigquery_adapter.py `
  pipeline/pandas_intake.py `
  pipeline/raw_load.py `
  pipeline/transaction_classification.py `
  pipeline/classification_load.py `
  scripts/bootstrap_bigquery.py `
  scripts/verify_bigquery_live.py `
  scripts/load_raw_batch.py `
  scripts/bootstrap_classification.py `
  scripts/classify_transactions.py

docker compose config --quiet
git diff --check
```

Credential hygiene check:

```powershell
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
```

The tracked-file search must produce no output.

Stop the local environment when finished:

```powershell
docker compose down
```
