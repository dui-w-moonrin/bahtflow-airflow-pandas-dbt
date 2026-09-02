# Feature 03 BigQuery Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the minimum BigQuery warehouse boundary required by later Pandas ingestion: four datasets and two empty partitioned raw tables, with rerun-safe bootstrap and live verification.

**Architecture:** Reuse the existing explicit GCP project/location settings and ADC impersonation boundary from F02. Keep dataset IDs fixed (`bahtflow_raw`, `bahtflow_ops`, `bahtflow_analytics`, `bahtflow_public`) to avoid new configuration surface. A small contract module defines the two raw table schemas; a thin BigQuery adapter creates or verifies datasets/tables and fails on location/schema/partition drift. No ingestion, Pandas transformation, Airflow task wiring, audit tables, marts, or publication logic belongs in F03.

**Tech Stack:** Python 3.12, `google-cloud-bigquery==3.44.0`, BigQuery, pytest, Docker Compose, existing ADC + service-account impersonation.

**Spec:** `docs/superpowers/specs/2026-09-02-pandas-v1-roadmap-design.md`

## Global Constraints

- Thin mode: F03 has one primary outcome only — BigQuery bootstrap.
- Create exactly four datasets: `bahtflow_raw`, `bahtflow_ops`, `bahtflow_analytics`, `bahtflow_public`.
- Create exactly two F03 tables: `bahtflow_raw.transactions` and `bahtflow_raw.fx_rates`.
- Do not create ops audit tables yet; they belong to later features.
- Do not ingest GCS data in F03.
- Do not add Pandas transformation or business-quality logic in F03.
- Do not add dbt; dbt is deferred to v2.
- Reuse `BAHTFLOW_GCP_PROJECT` and `BAHTFLOW_GCP_LOCATION`; add no dataset-name environment variables.
- Python BigQuery clients must use normal ADC with explicit `project=settings.project_id`; no service-account key files.
- Existing dataset location mismatch is a hard failure.
- Existing raw table schema or partition mismatch is a hard failure; bootstrap must never silently replace a mismatched table.
- F03 bootstrap is rerun-safe: unchanged resources are verified/skipped, not recreated destructively.
- Runtime permissions stay below BigQuery Admin. For thin v1, use project-scoped `roles/bigquery.user` and `roles/bigquery.dataEditor` on the dedicated BahtFlow project.

---

## File Map

- Modify: `requirements-gcp.txt` — add pinned BigQuery client dependency.
- Create: `pipeline/bigquery_contract.py` — dataset IDs, raw table schemas, partition fields.
- Create: `pipeline/bigquery_adapter.py` — narrow network adapter for create/verify operations.
- Create: `scripts/bootstrap_bigquery.py` — idempotent bootstrap CLI.
- Create: `scripts/verify_bigquery_live.py` — read-only live acceptance verifier.
- Modify: `README.md` — BigQuery API/IAM/bootstrap/verify commands only.
- Create: `tests/pipeline/test_bigquery_contract.py` — exact schema/partition contract tests.
- Create: `tests/pipeline/test_bigquery_adapter.py` — credential-free create/verify/drift tests with fake client.

---

### Task 1: Lock the BigQuery Raw Contract

**Files:**
- Modify: `requirements-gcp.txt`
- Create: `pipeline/bigquery_contract.py`
- Create: `tests/pipeline/test_bigquery_contract.py`

**Interfaces:**
- Consumes: existing `GcpSettings.project_id` and `GcpSettings.location` from `pipeline.config`.
- Produces: `DATASET_IDS: tuple[str, ...]`, `TRANSACTIONS_SCHEMA`, `FX_RATES_SCHEMA`, `TRANSACTIONS_PARTITION_FIELD`, `FX_RATES_PARTITION_FIELD`.

- [ ] **Step 1: Add the failing contract test**

```python
# tests/pipeline/test_bigquery_contract.py
from pipeline.bigquery_contract import (
    DATASET_IDS,
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)


def _shape(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def test_dataset_ids_are_fixed_and_minimal():
    assert DATASET_IDS == (
        "bahtflow_raw",
        "bahtflow_ops",
        "bahtflow_analytics",
        "bahtflow_public",
    )


def test_transactions_raw_schema_and_partition_contract():
    assert _shape(TRANSACTIONS_SCHEMA) == [
        ("txn", "STRING", "NULLABLE"),
        ("dtts", "STRING", "NULLABLE"),
        ("amount", "STRING", "NULLABLE"),
        ("currency", "STRING", "NULLABLE"),
        ("region", "STRING", "REQUIRED"),
        ("source_file", "STRING", "REQUIRED"),
        ("source_checksum", "STRING", "REQUIRED"),
        ("source_row_number", "INTEGER", "REQUIRED"),
        ("source_row_id", "STRING", "REQUIRED"),
        ("batch_date", "DATE", "REQUIRED"),
        ("ingested_at", "TIMESTAMP", "REQUIRED"),
    ]
    assert TRANSACTIONS_PARTITION_FIELD == "batch_date"


def test_fx_raw_schema_and_partition_contract():
    assert _shape(FX_RATES_SCHEMA) == [
        ("rate_date_raw", "STRING", "REQUIRED"),
        ("currency", "STRING", "REQUIRED"),
        ("mid_rate", "STRING", "REQUIRED"),
        ("rate_unit", "STRING", "REQUIRED"),
        ("source_provider", "STRING", "REQUIRED"),
        ("source_url", "STRING", "REQUIRED"),
        ("source_file", "STRING", "REQUIRED"),
        ("source_checksum", "STRING", "REQUIRED"),
        ("source_row_number", "INTEGER", "REQUIRED"),
        ("source_row_id", "STRING", "REQUIRED"),
        ("rate_date", "DATE", "REQUIRED"),
        ("ingested_at", "TIMESTAMP", "REQUIRED"),
    ]
    assert FX_RATES_PARTITION_FIELD == "rate_date"
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: collection/import failure because `pipeline.bigquery_contract` does not exist.

- [ ] **Step 3: Add the BigQuery dependency and minimal contract**

Append to `requirements-gcp.txt`:

```text
google-cloud-bigquery==3.44.0
```

Create:

```python
# pipeline/bigquery_contract.py
from google.cloud import bigquery

DATASET_IDS = (
    "bahtflow_raw",
    "bahtflow_ops",
    "bahtflow_analytics",
    "bahtflow_public",
)

TRANSACTIONS_SCHEMA = (
    bigquery.SchemaField("txn", "STRING"),
    bigquery.SchemaField("dtts", "STRING"),
    bigquery.SchemaField("amount", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
)
TRANSACTIONS_PARTITION_FIELD = "batch_date"

FX_RATES_SCHEMA = (
    bigquery.SchemaField("rate_date_raw", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mid_rate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate_unit", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_provider", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
)
FX_RATES_PARTITION_FIELD = "rate_date"
```

- [ ] **Step 4: Install/rebuild the toolbox dependency and run GREEN**

Run locally in the venv after dependency install, or rebuild the toolbox before Docker verification:

```powershell
pip install -r requirements-gcp.txt
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```powershell
git add requirements-gcp.txt pipeline/bigquery_contract.py tests/pipeline/test_bigquery_contract.py
git commit -m "feat: define BigQuery raw contract"
```

---

### Task 2: Add Rerun-Safe BigQuery Adapter

**Files:**
- Create: `pipeline/bigquery_adapter.py`
- Create: `tests/pipeline/test_bigquery_adapter.py`

**Interfaces:**
- Consumes: `project_id: str`, dataset IDs, schema tuples, partition fields.
- Produces: `BigQueryAdapter(project_id, client=None)`, `ensure_dataset(dataset_id, location) -> str`, `ensure_partitioned_table(dataset_id, table_id, schema, partition_field) -> str`, `get_table(...)`.
- Return values are exactly `"created"` or `"verified"` for ensure operations.

- [ ] **Step 1: Write credential-free failing tests with a fake client**

The fake client only needs `get_dataset`, `create_dataset`, `get_table`, and `create_table`. Tests must cover these cases:

```python
def test_missing_dataset_is_created_in_expected_location(): ...
def test_existing_dataset_same_location_is_verified(): ...
def test_existing_dataset_wrong_location_fails(): ...
def test_missing_partitioned_table_is_created(): ...
def test_existing_table_exact_schema_and_partition_is_verified(): ...
def test_existing_table_schema_drift_fails(): ...
def test_existing_table_partition_drift_fails(): ...
```

Use `google.api_core.exceptions.NotFound("missing")` from the fake for absent resources. Build fake dataset/table objects with the same attributes the adapter reads (`dataset_id`, `location`, `schema`, `time_partitioning`). Do not call the network.

- [ ] **Step 2: Run focused tests and confirm RED**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -v
```

Expected: import failure because `pipeline.bigquery_adapter` does not exist.

- [ ] **Step 3: Implement the minimal adapter**

The implementation must follow this shape:

```python
# pipeline/bigquery_adapter.py
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BigQueryContractError(RuntimeError):
    pass


class BigQueryAdapter:
    def __init__(self, project_id: str, client=None):
        self._project_id = project_id
        self._client = client or bigquery.Client(project=project_id)

    def ensure_dataset(self, dataset_id: str, location: str) -> str:
        full_id = f"{self._project_id}.{dataset_id}"
        try:
            existing = self._client.get_dataset(full_id)
        except NotFound:
            dataset = bigquery.Dataset(full_id)
            dataset.location = location
            self._client.create_dataset(dataset)
            return "created"

        if (existing.location or "").upper() != location.upper():
            raise BigQueryContractError(
                f"Dataset location mismatch for {full_id}: "
                f"expected={location} actual={existing.location}"
            )
        return "verified"

    def ensure_partitioned_table(
        self,
        dataset_id: str,
        table_id: str,
        schema,
        partition_field: str,
    ) -> str:
        full_id = f"{self._project_id}.{dataset_id}.{table_id}"
        try:
            existing = self._client.get_table(full_id)
        except NotFound:
            table = bigquery.Table(full_id, schema=list(schema))
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
            self._client.create_table(table)
            return "created"

        expected_shape = [
            (field.name, field.field_type, field.mode) for field in schema
        ]
        actual_shape = [
            (field.name, field.field_type, field.mode) for field in existing.schema
        ]
        if actual_shape != expected_shape:
            raise BigQueryContractError(f"Table schema mismatch for {full_id}")

        actual_partition = getattr(existing.time_partitioning, "field", None)
        if actual_partition != partition_field:
            raise BigQueryContractError(
                f"Table partition mismatch for {full_id}: "
                f"expected={partition_field} actual={actual_partition}"
            )
        return "verified"

    def get_table(self, dataset_id: str, table_id: str):
        return self._client.get_table(
            f"{self._project_id}.{dataset_id}.{table_id}"
        )
```

Do not add update/delete methods in F03.

- [ ] **Step 4: Run adapter tests and contract tests**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_bigquery_contract.py -v
```

Expected: all focused tests pass.

- [ ] **Step 5: Commit**

```powershell
git add pipeline/bigquery_adapter.py tests/pipeline/test_bigquery_adapter.py
git commit -m "feat: add rerun-safe BigQuery adapter"
```

---

### Task 3: Add Bootstrap CLI and Live Verification

**Files:**
- Create: `scripts/bootstrap_bigquery.py`
- Create: `scripts/verify_bigquery_live.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: `load_gcp_settings()`, `DATASET_IDS`, raw schemas/partition fields, `BigQueryAdapter`.
- Produces: one bootstrap command and one read-only live verifier.

- [ ] **Step 1: Add the bootstrap CLI**

Create `scripts/bootstrap_bigquery.py` with a `main()` that:

```python
settings = load_gcp_settings()
adapter = BigQueryAdapter(settings.project_id)

for dataset_id in DATASET_IDS:
    status = adapter.ensure_dataset(dataset_id, settings.location)
    print(f"dataset={dataset_id} status={status}")

status = adapter.ensure_partitioned_table(
    "bahtflow_raw",
    "transactions",
    TRANSACTIONS_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
)
print(f"table=bahtflow_raw.transactions status={status}")

status = adapter.ensure_partitioned_table(
    "bahtflow_raw",
    "fx_rates",
    FX_RATES_SCHEMA,
    FX_RATES_PARTITION_FIELD,
)
print(f"table=bahtflow_raw.fx_rates status={status}")
```

Use module execution only:

```powershell
python -m scripts.bootstrap_bigquery
```

- [ ] **Step 2: Add a read-only live verifier**

Create `scripts/verify_bigquery_live.py`. It must read all four datasets and both raw tables and print exactly these evidence fields:

```text
datasets=4
raw_tables=2
transactions_partition=batch_date
fx_rates_partition=rate_date
transactions_rows=0
fx_rates_rows=0
```

For row counts, execute two explicit BigQuery queries:

```sql
SELECT COUNT(*) AS row_count FROM `<project>.bahtflow_raw.transactions`
SELECT COUNT(*) AS row_count FROM `<project>.bahtflow_raw.fx_rates`
```

The verifier must fail if dataset count is not 4, raw table count is not 2, partitions differ, or either table is non-empty at F03 acceptance time.

- [ ] **Step 3: Document the minimum GCP setup**

Add a concise F03 README section with these operator commands, using the existing configured project/runtime service account values rather than hardcoded real email addresses:

```powershell
gcloud services enable bigquery.googleapis.com --project $env:BAHTFLOW_GCP_PROJECT

gcloud projects add-iam-policy-binding $env:BAHTFLOW_GCP_PROJECT `
  --member="serviceAccount:$env:BAHTFLOW_RUNTIME_SERVICE_ACCOUNT" `
  --role="roles/bigquery.user"

gcloud projects add-iam-policy-binding $env:BAHTFLOW_GCP_PROJECT `
  --member="serviceAccount:$env:BAHTFLOW_RUNTIME_SERVICE_ACCOUNT" `
  --role="roles/bigquery.dataEditor"
```

Then document:

```powershell
docker compose --profile gcp build gcp-toolbox

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_bigquery

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.verify_bigquery_live
```

State that the second bootstrap run must report `status=verified` for all six resources.

- [ ] **Step 4: Run static and credential-free verification**

```powershell
pytest

python -m py_compile `
  pipeline/bigquery_contract.py `
  pipeline/bigquery_adapter.py `
  scripts/bootstrap_bigquery.py `
  scripts/verify_bigquery_live.py

docker compose config --quiet
git diff --check
git status --short
```

Expected: pytest has 0 failures; remaining commands return no errors; status contains only intended uncommitted F03 files before commit.

- [ ] **Step 5: Run live bootstrap twice**

First run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_bigquery
```

Expected first-run shape on a new project:

```text
dataset=bahtflow_raw status=created
dataset=bahtflow_ops status=created
dataset=bahtflow_analytics status=created
dataset=bahtflow_public status=created
table=bahtflow_raw.transactions status=created
table=bahtflow_raw.fx_rates status=created
```

Second run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_bigquery
```

Expected:

```text
dataset=bahtflow_raw status=verified
dataset=bahtflow_ops status=verified
dataset=bahtflow_analytics status=verified
dataset=bahtflow_public status=verified
table=bahtflow_raw.transactions status=verified
table=bahtflow_raw.fx_rates status=verified
```

- [ ] **Step 6: Run the live verifier**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.verify_bigquery_live
```

Required evidence:

```text
datasets=4
raw_tables=2
transactions_partition=batch_date
fx_rates_partition=rate_date
transactions_rows=0
fx_rates_rows=0
```

- [ ] **Step 7: Commit**

```powershell
git add scripts/bootstrap_bigquery.py scripts/verify_bigquery_live.py README.md
git commit -m "feat: bootstrap BigQuery warehouse boundary"
```

---

## Final F03 Gate

F03 is complete only with fresh evidence for all of these:

```text
4 datasets exist in BAHTFLOW_GCP_LOCATION
2 raw tables exist
raw.transactions partition = batch_date
raw.fx_rates partition = rate_date
both raw tables = 0 rows
second bootstrap = all verified
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
credential files remain untracked
```

Do not start F04 until this gate is green. F04 owns GCS discovery, Pandas reads/validation, source metadata, and idempotent BigQuery raw loading.
