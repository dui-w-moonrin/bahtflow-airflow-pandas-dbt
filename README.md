# BahtFlow: Airflow, Pandas, BigQuery, and dbt

BahtFlow is a production-minded batch ELT portfolio project for practising the data-engineering workflow used in fintech teams: schedule work with Airflow, validate incoming files with Pandas, load immutable raw data into BigQuery, and transform it into trusted models with dbt.

## Current phase

The repository contains a committed, reproducible source corpus: 360 daily batches across five regional sales feeds. It deliberately preserves duplicate transactions, conflicting records, and invalid values such as `N/A`. Those records are evidence that later Pandas and dbt stages must classify, quarantine, test, and document data rather than silently discard it.

Feature 01 provides the local Apache Airflow 3 orchestration runtime and a no-op `bahtflow_daily` DAG skeleton. Feature 02 adds the first real cloud boundary: a credential-safe local-to-GCS landing path using Application Default Credentials (ADC), service-account impersonation, canonical object paths, and immutable SHA-256 checks. BigQuery, Pandas ingestion, dbt transformation, and real Airflow data-processing tasks remain later features.

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

`.env` is ignored by Git. Values in `.env.example` are local-development examples only.

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

The DAG is intentionally all `EmptyOperator` tasks in Feature 01. It proves orchestration shape, Airflow 3 component wiring, logical-date execution, and explicit backfill only.

## Feature 02: GCS landing boundary

Feature 02 keeps Google credentials outside the repository and image. The dedicated `gcp-toolbox` Compose service receives the host ADC file as a read-only bind mount. PostgreSQL and the current EmptyOperator-only Airflow services do not receive GCP credentials.

The landing layout is:

```text
gs://<BAHTFLOW_GCS_BUCKET>/
├── transactions/business_date=YYYY-MM-DD/sales_{region}_YYYYMMDD.csv.gz
├── fx/YYYY/MM/fx_YYYYMMDD.csv
└── manifests/
    ├── daily_source_manifest.csv
    └── fx_manifest.csv
```

Every uploaded object stores the exact local file SHA-256 in custom metadata key `bahtflow-source-sha256`. An absent object uploads, an existing object with the same checksum is skipped, and an existing object with a missing or different checksum fails instead of being overwritten. Uploads also use a GCS generation precondition so a race cannot silently replace an object.

### 1. Prepare local configuration

Sync the Feature 02 branch, then update the ignored `.env` from `.env.example` and replace the GCP placeholders with real values:

```text
BAHTFLOW_GCP_PROJECT=<project-id>
BAHTFLOW_GCS_BUCKET=<globally-unique-bucket-name>
BAHTFLOW_GCP_LOCATION=asia-southeast1
BAHTFLOW_RUNTIME_SERVICE_ACCOUNT=bahtflow-runtime@<project-id>.iam.gserviceaccount.com
GOOGLE_ADC_HOST_PATH=C:/Users/<windows-user>/AppData/Roaming/gcloud/application_default_credentials.json
```

Do not commit `.env` or the ADC file. The project must have active Cloud Billing and the Service Usage, IAM Service Account Credentials, and Cloud Storage APIs enabled before the live setup can complete.

### 2. Create the runtime service account and impersonation grant

The developer/admin identity creates infrastructure. The runtime service account performs normal object operations and does not administer bucket IAM or create/delete the bucket.

In PowerShell, set non-secret operator variables for the current shell:

```powershell
$projectId = "<project-id>"
$bucketName = "<globally-unique-bucket-name>"
$developerEmail = "<your-google-account-email>"
$runtimeSa = "bahtflow-runtime@$projectId.iam.gserviceaccount.com"
```

Create the runtime service account once if it does not already exist:

```powershell
gcloud iam service-accounts create bahtflow-runtime `
  --project=$projectId `
  --display-name="BahtFlow local runtime"
```

Allow the developer identity to impersonate it:

```powershell
gcloud iam service-accounts add-iam-policy-binding $runtimeSa `
  --project=$projectId `
  --member="user:$developerEmail" `
  --role="roles/iam.serviceAccountTokenCreator"
```

### 3. Bootstrap or verify the bucket with developer ADC

Create normal developer ADC:

```powershell
gcloud auth application-default login
```

Build the GCP toolbox and verify the Google Storage client import:

```powershell
docker compose --profile gcp build gcp-toolbox
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from google.cloud import storage; print(storage.__name__)"
```

Create the bucket if absent, or verify that an existing bucket uses the configured location:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_gcs --create-if-missing
```

For a pre-existing bucket, a non-creating check is available:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_gcs --check-only
```

After the bucket exists, grant only bucket-scoped object access to the runtime service account:

```powershell
gcloud storage buckets add-iam-policy-binding gs://$bucketName `
  --member="serviceAccount:$runtimeSa" `
  --role="roles/storage.objectAdmin"
```

### 4. Replace developer ADC with impersonated runtime ADC

Create an ADC file that the Python client libraries can use to impersonate the runtime service account:

```powershell
gcloud auth application-default login `
  --impersonate-service-account=$runtimeSa
```

Do not print or paste the ADC JSON. The developer identity needs `roles/iam.serviceAccountTokenCreator` on the runtime service account.

Verify credential resolution inside Docker without printing a token:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "import google.auth; c,p=google.auth.default(); print('project=', p); print('credential_type=', type(c).__name__)"
```

The ADC metadata may not provide a project ID for impersonated credentials. The application still uses the explicit `BAHTFLOW_GCP_PROJECT` setting when it constructs the GCS client.

### 5. Run the bounded immutable-landing smoke test

The smoke selection contains exactly one transaction file, one FX file, and both manifests:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.upload_landing_sources --smoke
```

On a fresh bucket the first run reports `uploaded=4 skipped=0`. Run the same command again; the required idempotency evidence is `uploaded=0 skipped=4`.

Run the repeatable live verifier. It reads a canonical manifest back from GCS, compares its bytes with the stored SHA-256 metadata, creates one disposable object outside the canonical landing prefixes, proves a changed source checksum is rejected, and deletes the disposable object again:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.verify_gcs_live
```

Expected non-secret evidence is:

```text
object=manifests/daily_source_manifest.csv
readback_checksum_match=True
conflict_detected=True
disposable_cleanup=True
```

The verifier refuses to overwrite or delete `_smoke_conflict/disposable.txt` if that object already exists before the run.

### 6. Optional full-corpus landing

The default uploader processes the complete committed transaction/FX/manifest corpus. Do this only after the bounded smoke test is green:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.upload_landing_sources
```

This is not required merely to prove Feature 02 wiring; later features can use the full landing set when needed.

## Feature 02 development checks

Credential-free logic tests do not require ADC or a live bucket:

```powershell
python -m pytest tests -v --basetemp .pytest_tmp
python -m py_compile `
  pipeline/config.py `
  pipeline/gcs_landing.py `
  pipeline/gcs_adapter.py `
  pipeline/gcs_workflows.py `
  scripts/bootstrap_gcs.py `
  scripts/upload_landing_sources.py `
  scripts/verify_gcs_live.py
docker compose config --quiet
```

Repository hygiene checks:

```powershell
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
git diff main...HEAD --check
```

The tracked-file search should not reveal any credential file. Do not grep or print `.env` or ADC contents.

Stop the local environment when finished:

```powershell
docker compose down
```
