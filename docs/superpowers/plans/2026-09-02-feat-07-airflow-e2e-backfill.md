# Feature 07 — Airflow E2E + Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn proven F04-F06 Pandas/GCS/BigQuery services into one Airflow 3 TaskFlow DAG that uses Airflow logical date for normal execution and explicit serial historical backfill, ending at `bahtflow_analytics.fct_transactions`.

**Architecture:** Build a custom Airflow 3.3.1 image with existing GCP/Pandas dependencies, split F04 raw ingestion into TX and FX boundaries while preserving `load_raw_batch`, add read-only preflight, and replace the skeleton DAG with thin TaskFlow wrappers. Durable data stays in BigQuery; XCom carries only ISO batch date and small JSON-serializable summaries. Daily scheduling uses `catchup=False`; backfill runs oldest-to-newest with one active run.

**Tech Stack:** Python 3.12, Apache Airflow 3.3.1, `airflow.sdk`, Pandas 2.3.3, google-cloud-storage 3.13.1, google-cloud-bigquery 3.44.0, BigQuery, GCS, Docker Compose, pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-07-airflow-e2e-backfill-design.md`

## Global Constraints

- Use normal branch `feat/07-airflow-e2e-backfill`; do not require a worktree.
- Retain the feature branch after integration.
- Base Airflow image: `apache/airflow:3.3.1-python3.12`.
- DAG authoring uses `@dag`, `@task`, and `get_current_context` from `airflow.sdk`.
- DAG schedule: `@daily`, `catchup=False`, `max_active_runs=1`.
- DAG retry policy: 2 retries, 2-minute delay, configured centrally in `default_args`.
- Airflow logical date converted to `Asia/Bangkok` is the only batch-date source; no independent DAG batch-date parameter.
- Missing or timezone-naive logical date fails.
- Backfill uses `--max-active-runs 1` and never `--run-backwards`.
- Preflight validates only; it never creates GCS/BigQuery resources.
- Missing same-day FX is `NO_NEW_RATE`; present malformed FX fails.
- Effective FX remains newest complete valid USD/EUR snapshot with `rate_date <= batch_date`.
- No DataFrame, source bytes, accepted rows, quarantine rows, or fact rows through XCom.
- Business transformations remain in `pipeline/`.
- Preserve F04-F06 schemas, idempotency, and single-writer behavior.
- F07 stops at fact. Marts/publish/recovery are F08. Full 360-date measured execution is F09.
- No dbt, Spark, Kafka, streaming, Composer, Kubernetes, Terraform, Great Expectations, BI, or ML.
- Use RED -> GREEN -> REFACTOR; observe failing test output before production changes.
- Commit each independently testable task.

## File Map

**Create:**
- `docker/airflow.Dockerfile`
- `pipeline/preflight.py`
- `pipeline/orchestration_date.py`
- `tests/pipeline/test_preflight.py`
- `tests/pipeline/test_orchestration_date.py`
- `tests/airflow/test_airflow_runtime_config.py`

**Modify:**
- `.env.example`
- `docker-compose.yml`
- `airflow/dags/bahtflow_daily.py`
- `pipeline/raw_load.py`
- `pipeline/bigquery_adapter.py`
- `tests/pipeline/test_raw_load.py`
- `tests/pipeline/test_bigquery_adapter.py`
- `tests/airflow/test_bahtflow_daily_dag.py`
- `README.md`

**Keep business semantics unchanged:**
- `pipeline/transaction_classification.py`
- `pipeline/classification_load.py`
- `pipeline/fx_resolution.py`
- `pipeline/currency_fact.py`
- `pipeline/currency_fact_load.py`

---

### Task 1: Split F04 Raw Load into TX and FX Services

**Files:**
- Modify: `pipeline/raw_load.py`
- Modify: `tests/pipeline/test_raw_load.py`

**Interfaces:**
- `TransactionRawLoadSummary`
- `FxRawLoadSummary`
- `load_transaction_raw_batch(*, batch_date: date, bucket_name: str, gcs_adapter, bigquery_adapter, ingested_at: datetime | None = None) -> TransactionRawLoadSummary`
- `load_fx_raw_batch(*, batch_date: date, bucket_name: str, gcs_adapter, bigquery_adapter, ingested_at: datetime | None = None) -> FxRawLoadSummary`
- Existing `load_raw_batch` remains compatible.

- [ ] **Step 1: Write RED tests**

Update imports:

```python
from pipeline.raw_load import load_fx_raw_batch, load_raw_batch, load_transaction_raw_batch
```

Add:

```python
def test_transaction_raw_loader_does_not_require_same_day_fx():
    d = date(2025, 7, 22)
    objects = _objects_with_fx(d)
    del objects[f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"]
    bq = FakeBigQueryAdapter()
    summary = load_transaction_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(objects),
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert (summary.tx_files, summary.tx_source_rows) == (5, 5)
    assert (summary.tx_inserted_rows, summary.tx_partition_rows) == (5, 5)
    assert bq.rows[("fx_rates", "2025-07-22")] == []


def test_fx_raw_loader_returns_no_new_rate_without_touching_transactions():
    d = date(2025, 7, 22)
    objects = _objects_with_fx(d)
    del objects[f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"]
    bq = FakeBigQueryAdapter()
    summary = load_fx_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(objects),
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert summary.fx_status == "NO_NEW_RATE"
    assert (summary.fx_source_rows, summary.fx_inserted_rows, summary.fx_partition_rows) == (0, 0, 0)
    assert bq.rows[("transactions", "2025-07-22")] == []


def test_combined_raw_loader_remains_backward_compatible_after_split():
    d = date(2025, 7, 22)
    summary = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(_objects_with_fx(d)),
        bigquery_adapter=FakeBigQueryAdapter(),
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert (summary.tx_files, summary.tx_source_rows, summary.tx_inserted_rows, summary.tx_partition_rows) == (5, 5, 5, 5)
    assert (summary.fx_status, summary.fx_source_rows, summary.fx_inserted_rows, summary.fx_partition_rows) == ("LOADED", 2, 2, 2)
```

- [ ] **Step 2: Observe RED**

```powershell
pytest tests/pipeline/test_raw_load.py -q
```

Expected: missing new functions.

- [ ] **Step 3: Add summary dataclasses**

```python
@dataclass(frozen=True)
class TransactionRawLoadSummary:
    batch_date: str
    tx_files: int
    tx_source_rows: int
    tx_inserted_rows: int
    tx_partition_rows: int


@dataclass(frozen=True)
class FxRawLoadSummary:
    batch_date: str
    fx_status: str
    fx_source_rows: int
    fx_inserted_rows: int
    fx_partition_rows: int
```

- [ ] **Step 4: Implement TX-only loader**

```python
def load_transaction_raw_batch(
    *,
    batch_date: date,
    bucket_name: str,
    gcs_adapter,
    bigquery_adapter,
    ingested_at: datetime | None = None,
) -> TransactionRawLoadSummary:
    invocation_time = ingested_at or datetime.now(timezone.utc)
    tx_frame = _load_tx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    existing = bigquery_adapter.query_source_row_ids(
        "bahtflow_raw", "transactions", TRANSACTIONS_PARTITION_FIELD, batch_date
    )
    new_rows = anti_filter_existing(tx_frame, existing)
    inserted = bigquery_adapter.append_rows(
        "bahtflow_raw", "transactions", new_rows.to_dict(orient="records"), TRANSACTIONS_SCHEMA
    )
    partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw", "transactions", TRANSACTIONS_PARTITION_FIELD, batch_date
    )
    return TransactionRawLoadSummary(
        batch_date=batch_date.isoformat(),
        tx_files=5,
        tx_source_rows=len(tx_frame),
        tx_inserted_rows=inserted,
        tx_partition_rows=partition_rows,
    )
```

- [ ] **Step 5: Implement FX-only loader**

```python
def load_fx_raw_batch(
    *,
    batch_date: date,
    bucket_name: str,
    gcs_adapter,
    bigquery_adapter,
    ingested_at: datetime | None = None,
) -> FxRawLoadSummary:
    invocation_time = ingested_at or datetime.now(timezone.utc)
    status, fx_frame = _load_fx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    inserted = 0
    if status == "LOADED":
        existing = bigquery_adapter.query_source_row_ids(
            "bahtflow_raw", "fx_rates", FX_RATES_PARTITION_FIELD, batch_date
        )
        new_rows = anti_filter_existing(fx_frame, existing)
        inserted = bigquery_adapter.append_rows(
            "bahtflow_raw", "fx_rates", new_rows.to_dict(orient="records"), FX_RATES_SCHEMA
        )
    partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw", "fx_rates", FX_RATES_PARTITION_FIELD, batch_date
    )
    return FxRawLoadSummary(
        batch_date=batch_date.isoformat(),
        fx_status=status,
        fx_source_rows=len(fx_frame),
        fx_inserted_rows=inserted,
        fx_partition_rows=partition_rows,
    )
```

- [ ] **Step 6: Rebuild combined compatibility facade explicitly**

```python
def load_raw_batch(
    *,
    batch_date: date,
    bucket_name: str,
    gcs_adapter,
    bigquery_adapter,
    ingested_at: datetime | None = None,
) -> RawLoadSummary:
    invocation_time = ingested_at or datetime.now(timezone.utc)
    tx = load_transaction_raw_batch(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        bigquery_adapter=bigquery_adapter,
        ingested_at=invocation_time,
    )
    fx = load_fx_raw_batch(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        bigquery_adapter=bigquery_adapter,
        ingested_at=invocation_time,
    )
    return RawLoadSummary(
        batch_date=batch_date.isoformat(),
        tx_files=tx.tx_files,
        tx_source_rows=tx.tx_source_rows,
        tx_inserted_rows=tx.tx_inserted_rows,
        tx_partition_rows=tx.tx_partition_rows,
        fx_status=fx.fx_status,
        fx_source_rows=fx.fx_source_rows,
        fx_inserted_rows=fx.fx_inserted_rows,
        fx_partition_rows=fx.fx_partition_rows,
    )
```

- [ ] **Step 7: GREEN + regression**

```powershell
pytest tests/pipeline/test_raw_load.py -q
pytest -q
```

- [ ] **Step 8: Commit**

```powershell
git add pipeline/raw_load.py tests/pipeline/test_raw_load.py
git commit -m "refactor: split transaction and fx raw loading"
```

---

### Task 2: Add Read-Only BigQuery Validation and Preflight

**Files:**
- Modify: `pipeline/bigquery_adapter.py`
- Modify: `tests/pipeline/test_bigquery_adapter.py`
- Create: `pipeline/preflight.py`
- Create: `tests/pipeline/test_preflight.py`

**Interfaces:**
- `validate_dataset(dataset_id: str, location: str) -> str`
- `validate_partitioned_table(dataset_id: str, table_id: str, schema, partition_field: str) -> str`
- `PreflightSummary`
- `run_preflight(*, settings, gcs_adapter, bigquery_adapter) -> PreflightSummary`

- [ ] **Step 1: Add adapter RED tests using existing `FakeClient`**

```python
def test_validate_dataset_returns_verified_for_existing_match():
    client = FakeClient()
    dataset = bigquery.Dataset("proj.bahtflow_raw")
    dataset.location = "asia-southeast1"
    client.datasets["proj.bahtflow_raw"] = dataset
    adapter = BigQueryAdapter("proj", client=client)
    assert adapter.validate_dataset("bahtflow_raw", "ASIA-SOUTHEAST1") == "verified"


def test_validate_dataset_missing_fails_without_creation():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    with pytest.raises(BigQueryContractError, match="Dataset does not exist"):
        adapter.validate_dataset("bahtflow_raw", "asia-southeast1")
    assert client.datasets == {}


def test_validate_partitioned_table_returns_verified_for_existing_match():
    client = FakeClient()
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)
    table = bigquery.Table("proj.bahtflow_raw.transactions", schema=list(schema))
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="batch_date",
    )
    client.tables["proj.bahtflow_raw.transactions"] = table
    adapter = BigQueryAdapter("proj", client=client)
    assert adapter.validate_partitioned_table(
        "bahtflow_raw", "transactions", schema, "batch_date"
    ) == "verified"


def test_validate_partitioned_table_missing_fails_without_creation():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)
    with pytest.raises(BigQueryContractError, match="Table does not exist"):
        adapter.validate_partitioned_table(
            "bahtflow_raw", "transactions", schema, "batch_date"
        )
    assert client.tables == {}
```

- [ ] **Step 2: Observe RED**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -q
```

- [ ] **Step 3: Implement read-only methods**

```python
def validate_dataset(self, dataset_id: str, location: str) -> str:
    full_id = f"{self._project_id}.{dataset_id}"
    try:
        existing = self._client.get_dataset(full_id)
    except NotFound as exc:
        raise BigQueryContractError(f"Dataset does not exist: {full_id}") from exc
    if (existing.location or "").upper() != location.upper():
        raise BigQueryContractError(
            f"Dataset location mismatch for {full_id}: expected={location} actual={existing.location}"
        )
    return "verified"


def validate_partitioned_table(self, dataset_id, table_id, schema, partition_field) -> str:
    full_id = f"{self._project_id}.{dataset_id}.{table_id}"
    try:
        existing = self._client.get_table(full_id)
    except NotFound as exc:
        raise BigQueryContractError(f"Table does not exist: {full_id}") from exc
    if _schema_shape(existing.schema) != _schema_shape(schema):
        raise BigQueryContractError(f"Table schema mismatch for {full_id}")
    actual_field = getattr(existing.time_partitioning, "field", None)
    actual_type = getattr(existing.time_partitioning, "type_", None)
    if actual_field != partition_field or actual_type != "DAY":
        raise BigQueryContractError(
            f"Table partition mismatch for {full_id}: expected=DAY:{partition_field} actual={actual_type}:{actual_field}"
        )
    return "verified"
```

- [ ] **Step 4: GREEN adapter tests**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -q
```

- [ ] **Step 5: Write preflight RED test**

Create `tests/pipeline/test_preflight.py`:

```python
from pipeline.config import GcpSettings
from pipeline.preflight import run_preflight


class FakeGcs:
    def __init__(self):
        self.calls = []
    def ensure_bucket(self, bucket_name, location, *, create_if_missing):
        self.calls.append((bucket_name, location, create_if_missing))
        return location.lower()


class FakeBigQuery:
    def __init__(self):
        self.datasets = []
        self.tables = []
    def validate_dataset(self, dataset_id, location):
        self.datasets.append((dataset_id, location))
        return "verified"
    def validate_partitioned_table(self, dataset_id, table_id, schema, partition_field):
        self.tables.append((dataset_id, table_id, partition_field))
        return "verified"


def test_preflight_is_validate_only_and_checks_f04_f06_resources():
    settings = GcpSettings(
        project_id="proj",
        bucket_name="bucket",
        location="asia-southeast1",
        runtime_service_account="runtime@proj.iam.gserviceaccount.com",
    )
    gcs = FakeGcs()
    bq = FakeBigQuery()
    summary = run_preflight(settings=settings, gcs_adapter=gcs, bigquery_adapter=bq)
    assert gcs.calls == [("bucket", "asia-southeast1", False)]
    assert {d for d, _ in bq.datasets} == {"bahtflow_raw", "bahtflow_ops", "bahtflow_analytics"}
    assert set(bq.tables) == {
        ("bahtflow_raw", "transactions", "batch_date"),
        ("bahtflow_raw", "fx_rates", "rate_date"),
        ("bahtflow_analytics", "transactions_accepted", "batch_date"),
        ("bahtflow_ops", "transactions_quarantine", "batch_date"),
        ("bahtflow_analytics", "fct_transactions", "batch_date"),
    }
    assert summary.datasets_verified == 3
    assert summary.tables_verified == 5
```

- [ ] **Step 6: Observe RED**

```powershell
pytest tests/pipeline/test_preflight.py -q
```

- [ ] **Step 7: Implement preflight**

Create `pipeline/preflight.py`:

```python
from dataclasses import dataclass
from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID, ACCEPTED_TABLE_ID, ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA, FACT_DATASET_ID, FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD, FACT_TRANSACTIONS_SCHEMA,
    FX_RATES_PARTITION_FIELD, FX_RATES_SCHEMA, QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID, QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA, TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)


@dataclass(frozen=True)
class PreflightSummary:
    project_id: str
    bucket_name: str
    location: str
    datasets_verified: int
    tables_verified: int


_REQUIRED_DATASETS = ("bahtflow_raw", "bahtflow_ops", "bahtflow_analytics")
_REQUIRED_TABLES = (
    ("bahtflow_raw", "transactions", TRANSACTIONS_SCHEMA, TRANSACTIONS_PARTITION_FIELD),
    ("bahtflow_raw", "fx_rates", FX_RATES_SCHEMA, FX_RATES_PARTITION_FIELD),
    (ACCEPTED_DATASET_ID, ACCEPTED_TABLE_ID, ACCEPTED_TRANSACTIONS_SCHEMA, ACCEPTED_TRANSACTIONS_PARTITION_FIELD),
    (QUARANTINE_DATASET_ID, QUARANTINE_TABLE_ID, QUARANTINE_TRANSACTIONS_SCHEMA, QUARANTINE_TRANSACTIONS_PARTITION_FIELD),
    (FACT_DATASET_ID, FACT_TABLE_ID, FACT_TRANSACTIONS_SCHEMA, FACT_TRANSACTIONS_PARTITION_FIELD),
)


def run_preflight(*, settings, gcs_adapter, bigquery_adapter) -> PreflightSummary:
    gcs_adapter.ensure_bucket(settings.bucket_name, settings.location, create_if_missing=False)
    for dataset_id in _REQUIRED_DATASETS:
        bigquery_adapter.validate_dataset(dataset_id, settings.location)
    for dataset_id, table_id, schema, partition_field in _REQUIRED_TABLES:
        bigquery_adapter.validate_partitioned_table(dataset_id, table_id, schema, partition_field)
    return PreflightSummary(
        project_id=settings.project_id,
        bucket_name=settings.bucket_name,
        location=settings.location,
        datasets_verified=len(_REQUIRED_DATASETS),
        tables_verified=len(_REQUIRED_TABLES),
    )
```

- [ ] **Step 8: GREEN + regression + commit**

```powershell
pytest tests/pipeline/test_preflight.py tests/pipeline/test_bigquery_adapter.py -q
pytest -q
git add pipeline/bigquery_adapter.py pipeline/preflight.py tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_preflight.py
git commit -m "feat: add read-only pipeline preflight"
```

---

### Task 3: Build Custom Airflow Runtime

**Files:**
- Create: `docker/airflow.Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `.env.example`
- Create: `tests/airflow/test_airflow_runtime_config.py`

- [ ] **Step 1: Write static RED tests**

```python
from pathlib import Path

ROOT = Path(__file__).parents[2]
DOCKERFILE = ROOT / "docker" / "airflow.Dockerfile"
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"


def test_custom_airflow_dockerfile_contract():
    source = DOCKERFILE.read_text(encoding="utf-8")
    assert "FROM apache/airflow:3.3.1-python3.12" in source
    assert "COPY requirements-gcp.txt /tmp/requirements-gcp.txt" in source
    assert "pip install --no-cache-dir -r /tmp/requirements-gcp.txt" in source


def test_compose_custom_airflow_runtime_contract():
    source = COMPOSE.read_text(encoding="utf-8")
    for required in (
        "dockerfile: docker/airflow.Dockerfile",
        "${BAHTFLOW_AIRFLOW_IMAGE_NAME:-bahtflow-airflow:3.3.1}",
        "AIRFLOW__CORE__DAGS_FOLDER: /opt/bahtflow/airflow/dags",
        "PYTHONPATH: /opt/bahtflow",
        "GOOGLE_APPLICATION_CREDENTIALS: /var/secrets/google/application_default_credentials.json",
        "- ./:/opt/bahtflow",
        "target: /var/secrets/google/application_default_credentials.json",
        "read_only: true",
    ):
        assert required in source
    for name in (
        "BAHTFLOW_GCP_PROJECT", "BAHTFLOW_GCS_BUCKET",
        "BAHTFLOW_GCP_LOCATION", "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT",
    ):
        assert f"{name}: ${{{name}}}" in source


def test_env_example_uses_custom_image_variable():
    source = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert "BAHTFLOW_AIRFLOW_IMAGE_NAME=bahtflow-airflow:3.3.1" in source
    assert "AIRFLOW_IMAGE_NAME=apache/airflow:3.3.1-python3.12" not in source
```

- [ ] **Step 2: Observe RED**

```powershell
pytest tests/airflow/test_airflow_runtime_config.py -q
```

- [ ] **Step 3: Create Dockerfile**

```dockerfile
FROM apache/airflow:3.3.1-python3.12

COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt
```

- [ ] **Step 4: Update Compose Airflow common block**

Use:

```yaml
  build:
    context: .
    dockerfile: docker/airflow.Dockerfile
  image: ${BAHTFLOW_AIRFLOW_IMAGE_NAME:-bahtflow-airflow:3.3.1}
```

Add to common environment:

```yaml
    AIRFLOW__CORE__DAGS_FOLDER: /opt/bahtflow/airflow/dags
    BAHTFLOW_GCP_PROJECT: ${BAHTFLOW_GCP_PROJECT}
    BAHTFLOW_GCS_BUCKET: ${BAHTFLOW_GCS_BUCKET}
    BAHTFLOW_GCP_LOCATION: ${BAHTFLOW_GCP_LOCATION}
    BAHTFLOW_RUNTIME_SERVICE_ACCOUNT: ${BAHTFLOW_RUNTIME_SERVICE_ACCOUNT}
    GOOGLE_APPLICATION_CREDENTIALS: /var/secrets/google/application_default_credentials.json
    PYTHONPATH: /opt/bahtflow
```

Use common volumes:

```yaml
  volumes:
    - ./:/opt/bahtflow
    - ./airflow/logs:/opt/airflow/logs
    - type: bind
      source: ${GOOGLE_ADC_HOST_PATH}
      target: /var/secrets/google/application_default_credentials.json
      read_only: true
```

Remove old DAG-only mount.

- [ ] **Step 5: Update `.env.example`**

Replace old Airflow image line with:

```text
BAHTFLOW_AIRFLOW_IMAGE_NAME=bahtflow-airflow:3.3.1
```

- [ ] **Step 6: GREEN static/runtime config**

```powershell
pytest tests/airflow/test_airflow_runtime_config.py -q
docker compose config --quiet
```

- [ ] **Step 7: Build and prove imports**

```powershell
docker compose build airflow-api-server airflow-scheduler airflow-dag-processor
docker compose run --rm airflow-scheduler python -c "import pandas; import google.cloud.bigquery; import google.cloud.storage; import pipeline; print('airflow_pipeline_import_ok')"
```

Expected: `airflow_pipeline_import_ok`.

- [ ] **Step 8: Full regression and commit**

```powershell
pytest -q
git add docker/airflow.Dockerfile docker-compose.yml .env.example tests/airflow/test_airflow_runtime_config.py
git commit -m "build: add custom Airflow pipeline runtime"
```

---

### Task 4: Add Logical-Date Helper and Production TaskFlow DAG

**Files:**
- Create: `pipeline/orchestration_date.py`
- Create: `tests/pipeline/test_orchestration_date.py`
- Modify: `airflow/dags/bahtflow_daily.py`
- Modify: `tests/airflow/test_bahtflow_daily_dag.py`

- [ ] **Step 1: Write logical-date RED tests**

```python
from datetime import datetime, timezone
import pytest
from pipeline.orchestration_date import batch_date_from_logical_date


def test_logical_date_converts_to_bangkok_before_taking_date():
    logical = datetime(2025, 7, 21, 18, 30, tzinfo=timezone.utc)
    assert batch_date_from_logical_date(logical).isoformat() == "2025-07-22"


def test_missing_logical_date_fails():
    with pytest.raises(ValueError, match="logical date is required"):
        batch_date_from_logical_date(None)


def test_naive_logical_date_fails():
    with pytest.raises(ValueError, match="timezone-aware"):
        batch_date_from_logical_date(datetime(2025, 7, 22))
```

- [ ] **Step 2: Observe RED**

```powershell
pytest tests/pipeline/test_orchestration_date.py -q
```

- [ ] **Step 3: Implement helper**

```python
from __future__ import annotations
from datetime import date, datetime
from zoneinfo import ZoneInfo

BAHTFLOW_TIMEZONE = ZoneInfo("Asia/Bangkok")


def batch_date_from_logical_date(logical_date: datetime | None) -> date:
    if logical_date is None:
        raise ValueError("Airflow logical date is required")
    if logical_date.tzinfo is None or logical_date.utcoffset() is None:
        raise ValueError("Airflow logical date must be timezone-aware")
    return logical_date.astimezone(BAHTFLOW_TIMEZONE).date()
```

- [ ] **Step 4: GREEN helper**

```powershell
pytest tests/pipeline/test_orchestration_date.py -q
```

- [ ] **Step 5: Replace old DAG tests with TaskFlow RED contract**

```python
from pathlib import Path

DAG_PATH = Path(__file__).parents[2] / "airflow" / "dags" / "bahtflow_daily.py"


def _source():
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_uses_airflow3_taskflow_api():
    s = _source()
    assert "from airflow.sdk import dag, get_current_context, task" in s
    assert "@dag(" in s and "@task" in s
    assert "dbt_transform" not in s and "dbt_test" not in s


def test_dag_operational_contract():
    s = _source()
    for text in (
        'schedule="@daily"', "catchup=False", "max_active_runs=1",
        '"retries": 2', "timedelta(minutes=2)", 'tz="Asia/Bangkok"',
    ):
        assert text in s


def test_dag_has_exact_f07_task_ids():
    s = _source()
    for task_id in (
        "resolve_batch_date", "preflight", "load_tx_raw", "load_fx_raw",
        "classify_transactions", "build_currency_fact", "finish",
    ):
        assert f'task_id="{task_id}"' in s


def test_dag_uses_pipeline_services_without_dataframe_business_logic():
    s = _source()
    for symbol in (
        "load_transaction_raw_batch", "load_fx_raw_batch",
        "classify_and_load_batch", "build_and_load_currency_fact", "run_preflight",
    ):
        assert symbol in s
    assert "pd.DataFrame" not in s
    assert "resolve_effective_fx" not in s


def test_dependency_edges_match_f07():
    n = "\n".join(line.strip() for line in _source().splitlines())
    assert "ready >> [tx_raw, fx_raw]" in n
    assert "tx_raw >> classified" in n
    assert "[classified, fx_raw] >> fact" in n
    assert "fact >> finish" in n
```

- [ ] **Step 6: Observe DAG RED**

```powershell
pytest tests/airflow/test_bahtflow_daily_dag.py -q
```

- [ ] **Step 7: Implement TaskFlow DAG**

Imports:

```python
from __future__ import annotations
from dataclasses import asdict
from datetime import date, timedelta
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, get_current_context, task
from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.classification_load import classify_and_load_batch
from pipeline.config import load_gcp_settings
from pipeline.currency_fact_load import build_and_load_currency_fact
from pipeline.gcs_adapter import GcsAdapter
from pipeline.orchestration_date import batch_date_from_logical_date
from pipeline.preflight import run_preflight
from pipeline.raw_load import load_fx_raw_batch, load_transaction_raw_batch
```

DAG declaration:

```python
@dag(
    dag_id="bahtflow_daily",
    description="BahtFlow daily Pandas pipeline through currency fact",
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bahtflow", "pandas", "bigquery"],
)
def bahtflow_daily():
```

Task bodies:

```python
    @task(task_id="resolve_batch_date")
    def resolve_batch_date() -> str:
        context = get_current_context()
        return batch_date_from_logical_date(context["dag_run"].logical_date).isoformat()

    @task(task_id="preflight")
    def preflight(batch_date: str) -> dict:
        date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(run_preflight(
            settings=settings,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        ))

    @task(task_id="load_tx_raw")
    def load_tx_raw(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(load_transaction_raw_batch(
            batch_date=d,
            bucket_name=settings.bucket_name,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        ))

    @task(task_id="load_fx_raw")
    def load_fx_raw(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(load_fx_raw_batch(
            batch_date=d,
            bucket_name=settings.bucket_name,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        ))

    @task(task_id="classify_transactions")
    def classify_transactions_task(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(classify_and_load_batch(
            batch_date=d,
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        ))

    @task(task_id="build_currency_fact")
    def build_currency_fact_task(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(build_and_load_currency_fact(
            batch_date=d,
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        ))
```

Dependency wiring:

```python
    batch_date = resolve_batch_date()
    ready = preflight(batch_date)
    tx_raw = load_tx_raw(batch_date)
    fx_raw = load_fx_raw(batch_date)
    classified = classify_transactions_task(batch_date)
    fact = build_currency_fact_task(batch_date)
    finish = EmptyOperator(task_id="finish")

    ready >> [tx_raw, fx_raw]
    tx_raw >> classified
    [classified, fx_raw] >> fact
    fact >> finish


bahtflow_daily()
```

- [ ] **Step 8: GREEN host tests**

```powershell
pytest tests/pipeline/test_orchestration_date.py tests/airflow/test_bahtflow_daily_dag.py -q
pytest -q
```

- [ ] **Step 9: Prove real runtime import**

```powershell
docker compose run --rm airflow-scheduler python -c "import importlib.util; p='/opt/bahtflow/airflow/dags/bahtflow_daily.py'; s=importlib.util.spec_from_file_location('bahtflow_daily', p); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); print('dag_import_ok')"
docker compose run --rm airflow-scheduler airflow dags list | Select-String bahtflow_daily
```

Expected: `dag_import_ok` and `bahtflow_daily` listed.

- [ ] **Step 10: Commit**

```powershell
git add pipeline/orchestration_date.py airflow/dags/bahtflow_daily.py tests/pipeline/test_orchestration_date.py tests/airflow/test_bahtflow_daily_dag.py
git commit -m "feat: orchestrate daily pipeline with Airflow TaskFlow"
```

---

### Task 5: Prove One-Day Airflow E2E and Idempotency

**Files:**
- Modify after execution: `README.md`

**Logical date:** `2025-07-22`.

- [ ] **Step 1: Start custom Airflow services**

```powershell
docker compose down
docker compose build airflow-api-server airflow-scheduler airflow-dag-processor airflow-init
docker compose up airflow-init
docker compose up -d postgres airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
```

Expected: Postgres/API healthy; scheduler and DAG processor running.

- [ ] **Step 2: Run read-only preflight inside Airflow image**

```powershell
@'
from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.preflight import run_preflight
s = load_gcp_settings()
print(run_preflight(
    settings=s,
    gcs_adapter=GcsAdapter(s.project_id),
    bigquery_adapter=BigQueryAdapter(s.project_id),
))
'@ | docker compose run --rm -T airflow-scheduler python -
```

Expected: `datasets_verified=3`, `tables_verified=5`.

- [ ] **Step 3: Verify DAG parse state**

```powershell
docker compose exec airflow-scheduler airflow dags list | Select-String bahtflow_daily
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected: DAG listed and no import error for its file.

- [ ] **Step 4: Run explicit one-date backfill**

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-22 --to-date 2025-07-22 --reprocess-behavior completed --max-active-runs 1
```

Do not use `--run-backwards`.

- [ ] **Step 5: Inspect task states and summaries**

Required successful tasks:

```text
resolve_batch_date
preflight
load_tx_raw
load_fx_raw
classify_transactions
build_currency_fact
finish
```

Because F04-F06 already persisted this date, verify:

```text
tx_inserted_rows=0
fx_inserted_rows=0
accepted_inserted_rows=0
quarantine_inserted_rows=0
fact_inserted_rows=0
classification reconciled=True
fact reconciled=True
```

- [ ] **Step 6: Repeat Step 4**

Expected: persisted inserted counts remain zero.

- [ ] **Step 7: Query persisted baseline dynamically**

```powershell
@'
from google.cloud import bigquery
from pipeline.config import load_gcp_settings
s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
tables = {
    "raw": "bahtflow_raw.transactions",
    "accepted": "bahtflow_analytics.transactions_accepted",
    "quarantine": "bahtflow_ops.transactions_quarantine",
    "fact": "bahtflow_analytics.fct_transactions",
}
for name, table in tables.items():
    sql = f"SELECT COUNT(*) n FROM `{s.project_id}.{table}` WHERE batch_date = DATE('2025-07-22')"
    print(f"{name}={next(iter(c.query(sql).result())).n}")
'@ | docker compose run --rm -T airflow-scheduler python -
```

Expected current baseline:

```text
raw=8978
accepted=8803
quarantine=175
fact=8803
```

- [ ] **Step 8: Document executed one-day evidence only**

README records custom runtime, logical-date rule, exact command, actual task/run state, inserted metrics, partition counts, and second-run zero-insert evidence.

- [ ] **Step 9: Verify and commit**

```powershell
pytest -q
docker compose config --quiet
git diff --check
git add README.md
git commit -m "docs: add Airflow one-day E2E evidence"
```

---

### Task 6: Prove Forward Serial Backfill and Live Carry-Forward

**Files:**
- Modify after execution: `README.md`

**Window:** `2025-07-25` through `2025-07-27`.

- [ ] **Step 1: Dry run in chronological order**

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-25 --to-date 2025-07-27 --reprocess-behavior completed --max-active-runs 1 --dry-run
```

Verify order `2025-07-25`, `2025-07-26`, `2025-07-27`. Do not use `--run-backwards`.

- [ ] **Step 2: Execute real serial backfill**

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-25 --to-date 2025-07-27 --reprocess-behavior completed --max-active-runs 1
```

- [ ] **Step 3: Capture FX task summaries**

Verify actual source behavior:

```text
2025-07-25 -> LOADED
2025-07-26 -> NO_NEW_RATE
2025-07-27 -> NO_NEW_RATE
```

If source data contradicts this window, stop and inspect source objects before changing acceptance dates.

- [ ] **Step 4: Query carry-forward lineage**

```powershell
@'
from google.cloud import bigquery
from pipeline.config import load_gcp_settings
s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
sql = f"""
SELECT batch_date, fx_rate_date, is_carried_forward, staleness_days,
       COUNT(*) fact_rows, COUNT(DISTINCT source_row_id) distinct_source_rows
FROM `{s.project_id}.bahtflow_analytics.fct_transactions`
WHERE batch_date BETWEEN DATE('2025-07-25') AND DATE('2025-07-27')
GROUP BY batch_date, fx_rate_date, is_carried_forward, staleness_days
ORDER BY batch_date
"""
for row in c.query(sql).result():
    print(dict(row.items()))
'@ | docker compose run --rm -T airflow-scheduler python -
```

Required:

```text
2025-07-25 -> fx_rate_date=2025-07-25, carried=False, staleness=0
2025-07-26 -> fx_rate_date=2025-07-25, carried=True, staleness=1
2025-07-27 -> fx_rate_date=2025-07-25, carried=True, staleness=2
```

Require `fact_rows == distinct_source_rows` each date.

- [ ] **Step 5: Query accepted/fact reconciliation**

```powershell
@'
from google.cloud import bigquery
from pipeline.config import load_gcp_settings
s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
sql = f"""
WITH a AS (
  SELECT batch_date, COUNT(*) accepted_rows
  FROM `{s.project_id}.bahtflow_analytics.transactions_accepted`
  WHERE batch_date BETWEEN DATE('2025-07-25') AND DATE('2025-07-27')
  GROUP BY batch_date
), f AS (
  SELECT batch_date, COUNT(*) fact_rows
  FROM `{s.project_id}.bahtflow_analytics.fct_transactions`
  WHERE batch_date BETWEEN DATE('2025-07-25') AND DATE('2025-07-27')
  GROUP BY batch_date
)
SELECT a.batch_date, accepted_rows, fact_rows, accepted_rows = fact_rows reconciled
FROM a JOIN f USING (batch_date)
ORDER BY batch_date
"""
for row in c.query(sql).result():
    print(dict(row.items()))
'@ | docker compose run --rm -T airflow-scheduler python -
```

Expected: 3 rows, all `reconciled=True`.

- [ ] **Step 6: Re-run Step 2 for idempotency**

Verify TX, FX, accepted, quarantine, and fact inserted counts are zero on replay; both reconciliation summaries remain true.

- [ ] **Step 7: Document executed historical evidence**

README records exact command, forward order, concurrency 1, FX statuses, actual FX lineage/staleness, fact counts, reconciliation, and replay zero-insert evidence. State full 360-day execution remains F09.

- [ ] **Step 8: Verify and commit**

```powershell
pytest -q
docker compose config --quiet
git diff --check
git add README.md
git commit -m "docs: add Airflow backfill and carry-forward evidence"
```

---

### Task 7: Final F07 Verification and Merge Readiness

**Files:** verification only unless a reproduced defect requires TDD changes.

- [ ] **Step 1: Fresh full tests**

```powershell
pytest -q
```

Expected: zero failures.

- [ ] **Step 2: Compile F07 Python**

```powershell
python -m py_compile pipeline/raw_load.py pipeline/bigquery_adapter.py pipeline/preflight.py pipeline/orchestration_date.py airflow/dags/bahtflow_daily.py
```

Expected: no output.

- [ ] **Step 3: Validate and rebuild runtime**

```powershell
docker compose config --quiet
docker compose build airflow-api-server airflow-scheduler airflow-dag-processor
```

- [ ] **Step 4: Fresh DAG parse gate**

```powershell
docker compose run --rm airflow-scheduler airflow dags list | Select-String bahtflow_daily
docker compose run --rm airflow-scheduler airflow dags list-import-errors
```

Expected: DAG listed; no import error.

- [ ] **Step 5: Repository clean gates**

```powershell
git diff --check
git status --short
git diff --check main...feat/07-airflow-e2e-backfill
git diff --stat main...feat/07-airflow-e2e-backfill
```

- [ ] **Step 6: Tracked credential scan**

```powershell
$patterns = 'private_key|client_secret|ya29\.|-----BEGIN PRIVATE KEY-----'
git grep -n -I -E $patterns -- . ':!docs/superpowers/plans/*' ':!docs/superpowers/specs/*'
```

Inspect every hit; no credential value may be newly tracked.

- [ ] **Step 7: Scope diff review**

```powershell
git diff --name-status main...feat/07-airflow-e2e-backfill
git log --oneline main..feat/07-airflow-e2e-backfill
```

Verify:

```text
custom Airflow image
GCP/Pandas runtime imports
repo + ADC read-only wiring
preflight validate-only
TX/FX split
load_raw_batch compatibility
logical-date-only batch date
2 retries / 2-minute delay
TaskFlow thin wrappers
catchup=False / max_active_runs=1
no DataFrame XCom
same DAG for single-date and historical execution
oldest-to-newest backfill
live carry-forward evidence
F07 stops at fact
no marts/publish/full-360 scope creep
```

- [ ] **Step 8: Request code review**

Invoke `superpowers:requesting-code-review`. If the harness cannot dispatch subagents, review the GitHub diff directly and state that limitation. Resolve Critical/Important findings only after reproducing them and applying TDD.

- [ ] **Step 9: Fresh verification after any review change**

If any file changes after Step 1, rerun:

```powershell
pytest -q
python -m py_compile pipeline/raw_load.py pipeline/bigquery_adapter.py pipeline/preflight.py pipeline/orchestration_date.py airflow/dags/bahtflow_daily.py
docker compose config --quiet
git diff --check
git status --short
```

- [ ] **Step 10: Finish branch and retain it**

Invoke `superpowers:finishing-a-development-branch`, present its standard integration menu, and retain `feat/07-airflow-e2e-backfill` after integration.
