# Feature 07 — Airflow E2E + Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the proven F04-F06 Pandas/GCS/BigQuery services into one Airflow 3 TaskFlow DAG that uses Airflow logical date for both daily execution and explicit serial historical backfill, ending at `bahtflow_analytics.fct_transactions`.

**Architecture:** Build a custom Airflow 3.3.1 image with the existing GCP/Pandas dependencies, split F04 raw ingestion into transaction and FX service boundaries while preserving `load_raw_batch()`, add read-only runtime preflight, and replace the skeleton DAG with thin TaskFlow wrappers. Durable task state stays in BigQuery; XCom carries only small JSON summaries. Daily scheduling uses `catchup=False`; explicit backfill runs oldest-to-newest with one active run so sparse FX history is deterministic.

**Tech Stack:** Python 3.12, Apache Airflow 3.3.1 TaskFlow / `airflow.sdk`, Pandas 2.3.3, google-cloud-storage 3.13.1, google-cloud-bigquery 3.44.0, BigQuery, GCS, Docker Compose, pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-07-airflow-e2e-backfill-design.md`

## Global Constraints

- Work on the normal Git branch `feat/07-airflow-e2e-backfill`; do not create or require a git worktree.
- Keep the feature branch after completion; branch cleanup is deferred until all project features are finished.
- Base Airflow runtime is exactly `apache/airflow:3.3.1-python3.12`.
- Airflow DAG authoring uses the Airflow 3 public interface from `airflow.sdk`, including `@dag`, `@task`, and `get_current_context`.
- The DAG schedule remains daily with `catchup=False` and `max_active_runs=1`.
- Airflow logical date converted to `Asia/Bangkok` is the only application batch-date source of truth. The DAG exposes no independent batch-date override.
- A missing logical date is an error; do not substitute wall-clock `today()` or `now()` as a batch date.
- Explicit backfill must run oldest-to-newest. Do not use Airflow's `--run-backwards` option for F07 acceptance.
- Backfill uses `--max-active-runs 1` in addition to DAG-level `max_active_runs=1`.
- F07 ends at persisted `bahtflow_analytics.fct_transactions`; marts, publish state, final quality gate, rollback, and persisted run-audit lifecycle are F08 scope.
- Full 360-date measured execution is F09 scope.
- Preflight is validate-only. It must never create or alter GCS or BigQuery resources.
- Same-day FX absence is valid `NO_NEW_RATE`; a present malformed FX publication fails.
- Effective FX remains the F06 rule: newest complete valid USD/EUR publication with `rate_date <= batch_date`; no future rate and no magic default.
- No Pandas DataFrame, source bytes, accepted rows, quarantine rows, or fact rows are passed through XCom.
- Business transformation logic remains in `pipeline/`; DAG code is orchestration only.
- Existing F04-F06 interfaces and persisted contracts remain backward-compatible unless this plan explicitly introduces a narrow orchestration boundary.
- Preserve single-writer/idempotent append semantics already proven by F04-F06.
- Do not add dbt, Spark/PySpark, Kafka, streaming, Composer, Kubernetes, Terraform, Great Expectations, BI, or ML.
- Follow RED -> GREEN -> REFACTOR for production changes. No production implementation is written before the corresponding failing tests have been observed.
- Commit after each independently testable task.

---

## File Structure Map

### New files

- `docker/airflow.Dockerfile` — custom Airflow 3.3.1 runtime image that installs `requirements-gcp.txt`.
- `pipeline/preflight.py` — read-only GCS/BigQuery/runtime readiness validation.
- `pipeline/orchestration_date.py` — pure, Airflow-independent conversion from an aware logical datetime to the BahtFlow `batch_date`.
- `tests/pipeline/test_preflight.py` — preflight behavior with fake adapters.
- `tests/pipeline/test_orchestration_date.py` — timezone conversion and missing/naive logical-date behavior.
- `tests/airflow/test_airflow_runtime_config.py` — static contract tests for custom image and Compose runtime wiring.

### Modified files

- `.env.example` — replace the old upstream-image variable with a BahtFlow custom-image variable.
- `docker-compose.yml` — build/use the custom Airflow image, mount the repository and ADC read-only, expose required GCP environment, and set `PYTHONPATH=/opt/bahtflow` for Airflow services.
- `airflow/dags/bahtflow_daily.py` — replace EmptyOperator skeleton with the production TaskFlow DAG.
- `pipeline/raw_load.py` — add transaction-only and FX-only orchestration boundaries; preserve `load_raw_batch()` as the combined compatibility facade.
- `pipeline/bigquery_adapter.py` — add narrow read-only dataset/table contract validation methods.
- `tests/pipeline/test_raw_load.py` — prove split services and backward compatibility.
- `tests/pipeline/test_bigquery_adapter.py` — prove read-only validation success/missing/mismatch behavior.
- `tests/airflow/test_bahtflow_daily_dag.py` — replace old skeleton assertions with TaskFlow/source contract assertions.
- `README.md` — F07 custom runtime, one-day E2E, explicit backfill, and executed evidence only.

### Intentionally unchanged business modules

- `pipeline/transaction_classification.py`
- `pipeline/classification_load.py`
- `pipeline/fx_resolution.py`
- `pipeline/currency_fact.py`
- `pipeline/currency_fact_load.py`

If implementation discovers an unavoidable interface issue in one of these modules, stop and review the need before changing it; F05/F06 business semantics are not part of F07 redesign.

---

### Task 1: Split F04 Raw Loading into Transaction and FX Boundaries

**Files:**
- Modify: `pipeline/raw_load.py`
- Modify: `tests/pipeline/test_raw_load.py`

**Interfaces:**
- Consumes: existing `_load_tx_frame(...)`, `_load_fx_frame(...)`, `anti_filter_existing(...)`, `BigQueryAdapter` query/append methods.
- Produces:
  - `TransactionRawLoadSummary`
  - `FxRawLoadSummary`
  - `load_transaction_raw_batch(*, batch_date: date, bucket_name: str, gcs_adapter, bigquery_adapter, ingested_at: datetime | None = None) -> TransactionRawLoadSummary`
  - `load_fx_raw_batch(*, batch_date: date, bucket_name: str, gcs_adapter, bigquery_adapter, ingested_at: datetime | None = None) -> FxRawLoadSummary`
  - Existing `load_raw_batch(...) -> RawLoadSummary` preserved as a compatibility facade.

- [ ] **Step 1: Extend the raw-load tests with split-boundary RED cases**

Update imports in `tests/pipeline/test_raw_load.py`:

```python
from pipeline.raw_load import (
    load_fx_raw_batch,
    load_raw_batch,
    load_transaction_raw_batch,
)
```

Add these tests using the existing `FakeGcsAdapter`, `FakeBigQueryAdapter`, `_objects_with_fx()`, and fixed invocation timestamp:

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

    assert summary.batch_date == "2025-07-22"
    assert summary.tx_files == 5
    assert summary.tx_source_rows == 5
    assert summary.tx_inserted_rows == 5
    assert summary.tx_partition_rows == 5
    assert bq.rows[("fx_rates", "2025-07-22")] == []


def test_fx_raw_loader_returns_no_new_rate_without_loading_transactions():
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

    assert summary.batch_date == "2025-07-22"
    assert summary.fx_status == "NO_NEW_RATE"
    assert summary.fx_source_rows == 0
    assert summary.fx_inserted_rows == 0
    assert summary.fx_partition_rows == 0
    assert bq.rows[("transactions", "2025-07-22")] == []


def test_combined_raw_loader_remains_backward_compatible_after_split():
    d = date(2025, 7, 22)
    bq = FakeBigQueryAdapter()

    summary = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(_objects_with_fx(d)),
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )

    assert summary.tx_files == 5
    assert summary.tx_source_rows == 5
    assert summary.tx_inserted_rows == 5
    assert summary.tx_partition_rows == 5
    assert summary.fx_status == "LOADED"
    assert summary.fx_source_rows == 2
    assert summary.fx_inserted_rows == 2
    assert summary.fx_partition_rows == 2
```

- [ ] **Step 2: Run the focused tests and observe RED**

Run:

```powershell
pytest tests/pipeline/test_raw_load.py -q
```

Expected: collection/import failure because `load_transaction_raw_batch` and `load_fx_raw_batch` do not exist yet.

- [ ] **Step 3: Add the two narrow summary types**

In `pipeline/raw_load.py`, add:

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

Keep the existing `RawLoadSummary` unchanged so the CLI and existing tests retain their current field names.

- [ ] **Step 4: Move transaction persistence into `load_transaction_raw_batch()`**

Implement the function by extracting the existing transaction half of `load_raw_batch()` exactly:

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
    tx_existing = bigquery_adapter.query_source_row_ids(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    tx_new = anti_filter_existing(tx_frame, tx_existing)
    tx_inserted = bigquery_adapter.append_rows(
        "bahtflow_raw",
        "transactions",
        tx_new.to_dict(orient="records"),
        TRANSACTIONS_SCHEMA,
    )
    tx_partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    return TransactionRawLoadSummary(
        batch_date=batch_date.isoformat(),
        tx_files=5,
        tx_source_rows=len(tx_frame),
        tx_inserted_rows=tx_inserted,
        tx_partition_rows=tx_partition_rows,
    )
```

- [ ] **Step 5: Move sparse FX persistence into `load_fx_raw_batch()`**

Implement the function by extracting the existing FX half exactly:

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
    fx_status, fx_frame = _load_fx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    fx_inserted = 0
    if fx_status == "LOADED":
        fx_existing = bigquery_adapter.query_source_row_ids(
            "bahtflow_raw",
            "fx_rates",
            FX_RATES_PARTITION_FIELD,
            batch_date,
        )
        fx_new = anti_filter_existing(fx_frame, fx_existing)
        fx_inserted = bigquery_adapter.append_rows(
            "bahtflow_raw",
            "fx_rates",
            fx_new.to_dict(orient="records"),
            FX_RATES_SCHEMA,
        )

    fx_partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw",
        "fx_rates",
        FX_RATES_PARTITION_FIELD,
        batch_date,
    )
    return FxRawLoadSummary(
        batch_date=batch_date.isoformat(),
        fx_status=fx_status,
        fx_source_rows=len(fx_frame),
        fx_inserted_rows=fx_inserted,
        fx_partition_rows=fx_partition_rows,
    )
```

- [ ] **Step 6: Rebuild `load_raw_batch()` as the compatibility facade**

The combined function must call the two new services with the same invocation timestamp and compose the old summary:

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

- [ ] **Step 7: Run focused and full regression tests**

Run:

```powershell
pytest tests/pipeline/test_raw_load.py -q
pytest -q
```

Expected: all tests pass; existing F04-F06 behavior remains green.

- [ ] **Step 8: Commit Task 1**

```powershell
git add pipeline/raw_load.py tests/pipeline/test_raw_load.py
git commit -m "refactor: split transaction and fx raw loading"
```

---

### Task 2: Add Read-Only BigQuery Contract Validation and Preflight

**Files:**
- Modify: `pipeline/bigquery_adapter.py`
- Modify: `tests/pipeline/test_bigquery_adapter.py`
- Create: `pipeline/preflight.py`
- Create: `tests/pipeline/test_preflight.py`

**Interfaces:**
- Produces:
  - `BigQueryAdapter.validate_dataset(dataset_id: str, location: str) -> str`
  - `BigQueryAdapter.validate_partitioned_table(dataset_id: str, table_id: str, schema, partition_field: str) -> str`
  - `PreflightSummary`
  - `run_preflight(*, settings: GcpSettings, gcs_adapter, bigquery_adapter) -> PreflightSummary`
- Consumes: `GcsAdapter.ensure_bucket(..., create_if_missing=False)`, F03-F06 schema constants.

- [ ] **Step 1: Add adapter RED tests for validate-only behavior**

Extend `tests/pipeline/test_bigquery_adapter.py` using its existing fake BigQuery client style. Cover these exact outcomes:

```python
def test_validate_dataset_returns_verified_for_matching_existing_dataset():
    adapter = BigQueryAdapter("project", client=FakeClient.with_dataset("project.bahtflow_raw", "asia-southeast1"))
    assert adapter.validate_dataset("bahtflow_raw", "asia-southeast1") == "verified"


def test_validate_dataset_missing_raises_without_create():
    client = FakeClient()
    adapter = BigQueryAdapter("project", client=client)
    with pytest.raises(BigQueryContractError, match="Dataset does not exist"):
        adapter.validate_dataset("bahtflow_raw", "asia-southeast1")
    assert client.created_datasets == []


def test_validate_partitioned_table_missing_raises_without_create():
    client = FakeClient()
    adapter = BigQueryAdapter("project", client=client)
    with pytest.raises(BigQueryContractError, match="Table does not exist"):
        adapter.validate_partitioned_table(
            "bahtflow_raw",
            "transactions",
            TRANSACTIONS_SCHEMA,
            "batch_date",
        )
    assert client.created_tables == []
```

If the current fake client has different helper names, extend that existing fake rather than introducing a second incompatible fake style.

- [ ] **Step 2: Run adapter RED tests**

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -q
```

Expected: failures because the two `validate_*` methods do not exist.

- [ ] **Step 3: Implement `validate_dataset()` with no create path**

Add:

```python
def validate_dataset(self, dataset_id: str, location: str) -> str:
    full_id = f"{self._project_id}.{dataset_id}"
    try:
        existing = self._client.get_dataset(full_id)
    except NotFound as exc:
        raise BigQueryContractError(f"Dataset does not exist: {full_id}") from exc

    if (existing.location or "").upper() != location.upper():
        raise BigQueryContractError(
            f"Dataset location mismatch for {full_id}: "
            f"expected={location} actual={existing.location}"
        )
    return "verified"
```

Do not call `ensure_dataset()` from this method because `ensure_dataset()` has a create path.

- [ ] **Step 4: Implement `validate_partitioned_table()` with no create path**

Add:

```python
def validate_partitioned_table(
    self,
    dataset_id: str,
    table_id: str,
    schema,
    partition_field: str,
) -> str:
    full_id = f"{self._project_id}.{dataset_id}.{table_id}"
    try:
        existing = self._client.get_table(full_id)
    except NotFound as exc:
        raise BigQueryContractError(f"Table does not exist: {full_id}") from exc

    if _schema_shape(existing.schema) != _schema_shape(schema):
        raise BigQueryContractError(f"Table schema mismatch for {full_id}")

    actual_partition = getattr(existing.time_partitioning, "field", None)
    actual_partition_type = getattr(existing.time_partitioning, "type_", None)
    if actual_partition != partition_field or actual_partition_type != "DAY":
        raise BigQueryContractError(
            f"Table partition mismatch for {full_id}: "
            f"expected=DAY:{partition_field} "
            f"actual={actual_partition_type}:{actual_partition}"
        )
    return "verified"
```

- [ ] **Step 5: Run adapter tests GREEN**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -q
```

Expected: pass.

- [ ] **Step 6: Write preflight RED tests**

Create `tests/pipeline/test_preflight.py` with narrow fake adapters:

```python
from pipeline.config import GcpSettings
from pipeline.preflight import run_preflight


class FakeGcs:
    def __init__(self):
        self.calls = []

    def ensure_bucket(self, bucket_name, location, *, create_if_missing):
        self.calls.append((bucket_name, location, create_if_missing))
        assert create_if_missing is False
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


def _settings():
    return GcpSettings(
        project_id="project",
        bucket_name="bucket",
        location="asia-southeast1",
        runtime_service_account="runtime@project.iam.gserviceaccount.com",
    )


def test_preflight_is_validate_only_and_checks_f04_f06_contracts():
    gcs = FakeGcs()
    bq = FakeBigQuery()

    summary = run_preflight(settings=_settings(), gcs_adapter=gcs, bigquery_adapter=bq)

    assert gcs.calls == [("bucket", "asia-southeast1", False)]
    assert {dataset for dataset, _ in bq.datasets} == {
        "bahtflow_raw",
        "bahtflow_ops",
        "bahtflow_analytics",
    }
    assert {(dataset, table, partition) for dataset, table, partition in bq.tables} == {
        ("bahtflow_raw", "transactions", "batch_date"),
        ("bahtflow_raw", "fx_rates", "rate_date"),
        ("bahtflow_analytics", "transactions_accepted", "batch_date"),
        ("bahtflow_ops", "transactions_quarantine", "batch_date"),
        ("bahtflow_analytics", "fct_transactions", "batch_date"),
    }
    assert summary.datasets_verified == 3
    assert summary.tables_verified == 5
```

- [ ] **Step 7: Run preflight RED test**

```powershell
pytest tests/pipeline/test_preflight.py -q
```

Expected: import failure because `pipeline.preflight` does not exist.

- [ ] **Step 8: Implement the preflight contract table and summary**

Create `pipeline/preflight.py` with imports from `pipeline.bigquery_contract` and:

```python
from dataclasses import dataclass

from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)


@dataclass(frozen=True)
class PreflightSummary:
    project_id: str
    bucket_name: str
    location: str
    datasets_verified: int
    tables_verified: int
```

Define table contracts exactly:

```python
_REQUIRED_DATASETS = (
    "bahtflow_raw",
    "bahtflow_ops",
    "bahtflow_analytics",
)

_REQUIRED_TABLES = (
    ("bahtflow_raw", "transactions", TRANSACTIONS_SCHEMA, TRANSACTIONS_PARTITION_FIELD),
    ("bahtflow_raw", "fx_rates", FX_RATES_SCHEMA, FX_RATES_PARTITION_FIELD),
    (ACCEPTED_DATASET_ID, ACCEPTED_TABLE_ID, ACCEPTED_TRANSACTIONS_SCHEMA, ACCEPTED_TRANSACTIONS_PARTITION_FIELD),
    (QUARANTINE_DATASET_ID, QUARANTINE_TABLE_ID, QUARANTINE_TRANSACTIONS_SCHEMA, QUARANTINE_TRANSACTIONS_PARTITION_FIELD),
    (FACT_DATASET_ID, FACT_TABLE_ID, FACT_TRANSACTIONS_SCHEMA, FACT_TRANSACTIONS_PARTITION_FIELD),
)
```

Implement:

```python
def run_preflight(*, settings, gcs_adapter, bigquery_adapter) -> PreflightSummary:
    gcs_adapter.ensure_bucket(
        settings.bucket_name,
        settings.location,
        create_if_missing=False,
    )
    for dataset_id in _REQUIRED_DATASETS:
        bigquery_adapter.validate_dataset(dataset_id, settings.location)
    for dataset_id, table_id, schema, partition_field in _REQUIRED_TABLES:
        bigquery_adapter.validate_partitioned_table(
            dataset_id,
            table_id,
            schema,
            partition_field,
        )
    return PreflightSummary(
        project_id=settings.project_id,
        bucket_name=settings.bucket_name,
        location=settings.location,
        datasets_verified=len(_REQUIRED_DATASETS),
        tables_verified=len(_REQUIRED_TABLES),
    )
```

- [ ] **Step 9: Run preflight, adapter, and full tests**

```powershell
pytest tests/pipeline/test_preflight.py tests/pipeline/test_bigquery_adapter.py -q
pytest -q
```

Expected: all pass.

- [ ] **Step 10: Commit Task 2**

```powershell
git add pipeline/bigquery_adapter.py pipeline/preflight.py tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_preflight.py
git commit -m "feat: add read-only pipeline preflight"
```

---

### Task 3: Build the Custom Airflow Runtime Contract

**Files:**
- Create: `docker/airflow.Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `.env.example`
- Create: `tests/airflow/test_airflow_runtime_config.py`

**Interfaces:**
- Produces local Airflow image tag default `bahtflow-airflow:3.3.1`.
- Airflow services receive `BAHTFLOW_GCP_PROJECT`, `BAHTFLOW_GCS_BUCKET`, `BAHTFLOW_GCP_LOCATION`, `BAHTFLOW_RUNTIME_SERVICE_ACCOUNT`, `GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/google/application_default_credentials.json`, and `PYTHONPATH=/opt/bahtflow`.
- Repository mounted at `/opt/bahtflow`; ADC mounted read-only at `/var/secrets/google/application_default_credentials.json`.

- [ ] **Step 1: Write static RED tests for Dockerfile and Compose wiring**

Create `tests/airflow/test_airflow_runtime_config.py`:

```python
from pathlib import Path


ROOT = Path(__file__).parents[2]
DOCKERFILE = ROOT / "docker" / "airflow.Dockerfile"
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"


def test_custom_airflow_dockerfile_uses_pinned_airflow_and_gcp_requirements():
    source = DOCKERFILE.read_text(encoding="utf-8")
    assert "FROM apache/airflow:3.3.1-python3.12" in source
    assert "COPY requirements-gcp.txt" in source
    assert "pip install --no-cache-dir -r /tmp/requirements-gcp.txt" in source


def test_compose_builds_custom_airflow_and_mounts_repo_and_adc_read_only():
    source = COMPOSE.read_text(encoding="utf-8")
    assert "dockerfile: docker/airflow.Dockerfile" in source
    assert "${BAHTFLOW_AIRFLOW_IMAGE_NAME:-bahtflow-airflow:3.3.1}" in source
    assert "PYTHONPATH: /opt/bahtflow" in source
    assert "GOOGLE_APPLICATION_CREDENTIALS: /var/secrets/google/application_default_credentials.json" in source
    assert "- ./:/opt/bahtflow" in source
    assert "target: /var/secrets/google/application_default_credentials.json" in source
    assert "read_only: true" in source


def test_airflow_common_receives_required_bahtflow_gcp_environment():
    source = COMPOSE.read_text(encoding="utf-8")
    for name in (
        "BAHTFLOW_GCP_PROJECT",
        "BAHTFLOW_GCS_BUCKET",
        "BAHTFLOW_GCP_LOCATION",
        "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT",
    ):
        assert f"{name}: ${{{name}}}" in source


def test_env_example_names_custom_airflow_image():
    source = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert "BAHTFLOW_AIRFLOW_IMAGE_NAME=bahtflow-airflow:3.3.1" in source
    assert "AIRFLOW_IMAGE_NAME=apache/airflow:3.3.1-python3.12" not in source
```

- [ ] **Step 2: Run runtime-contract tests and observe RED**

```powershell
pytest tests/airflow/test_airflow_runtime_config.py -q
```

Expected: failure because `docker/airflow.Dockerfile` does not exist and Compose still uses the stock Airflow image/runtime mounts.

- [ ] **Step 3: Create the custom Airflow Dockerfile**

Create `docker/airflow.Dockerfile` exactly:

```dockerfile
FROM apache/airflow:3.3.1-python3.12

COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt
```

Do not copy pipeline source into the image; local development source is mounted by Compose at `/opt/bahtflow` per the approved design.

- [ ] **Step 4: Change the Compose Airflow common block to build the image**

In `x-airflow-common`, replace the stock-only image declaration with:

```yaml
  build:
    context: .
    dockerfile: docker/airflow.Dockerfile
  image: ${BAHTFLOW_AIRFLOW_IMAGE_NAME:-bahtflow-airflow:3.3.1}
```

Add these entries to `&airflow-common-env` without removing existing Airflow settings:

```yaml
    BAHTFLOW_GCP_PROJECT: ${BAHTFLOW_GCP_PROJECT}
    BAHTFLOW_GCS_BUCKET: ${BAHTFLOW_GCS_BUCKET}
    BAHTFLOW_GCP_LOCATION: ${BAHTFLOW_GCP_LOCATION}
    BAHTFLOW_RUNTIME_SERVICE_ACCOUNT: ${BAHTFLOW_RUNTIME_SERVICE_ACCOUNT}
    GOOGLE_APPLICATION_CREDENTIALS: /var/secrets/google/application_default_credentials.json
    PYTHONPATH: /opt/bahtflow
```

Replace the Airflow common volumes with a repository mount, logs mount, and ADC bind:

```yaml
  volumes:
    - ./:/opt/bahtflow
    - ./airflow/logs:/opt/airflow/logs
    - type: bind
      source: ${GOOGLE_ADC_HOST_PATH}
      target: /var/secrets/google/application_default_credentials.json
      read_only: true
```

The repository mount already contains `airflow/dags`; Airflow's default dags folder remains `/opt/airflow/dags`, so explicitly add:

```yaml
    AIRFLOW__CORE__DAGS_FOLDER: /opt/bahtflow/airflow/dags
```

This avoids a second overlapping DAG-only mount.

- [ ] **Step 5: Update `.env.example` image variable**

Replace:

```text
AIRFLOW_IMAGE_NAME=apache/airflow:3.3.1-python3.12
```

with:

```text
BAHTFLOW_AIRFLOW_IMAGE_NAME=bahtflow-airflow:3.3.1
```

Do not put a real credential path, project ID, bucket, or service-account secret into tracked files.

- [ ] **Step 6: Run static tests and Compose validation**

```powershell
pytest tests/airflow/test_airflow_runtime_config.py -q
docker compose config --quiet
```

Expected: tests pass and Compose config exits successfully with no output.

- [ ] **Step 7: Build the custom Airflow image and prove imports inside it**

Run:

```powershell
docker compose build airflow-api-server airflow-scheduler airflow-dag-processor

docker compose run --rm airflow-scheduler python -c "import pandas; import google.cloud.bigquery; import google.cloud.storage; import pipeline; print('airflow_pipeline_import_ok')"
```

Expected final line:

```text
airflow_pipeline_import_ok
```

If Docker/ADC setup fails, stop and use systematic debugging before altering the design.

- [ ] **Step 8: Run the full pytest suite**

```powershell
pytest -q
```

Expected: all tests pass.

- [ ] **Step 9: Commit Task 3**

```powershell
git add docker/airflow.Dockerfile docker-compose.yml .env.example tests/airflow/test_airflow_runtime_config.py
git commit -m "build: add custom Airflow pipeline runtime"
```

---

### Task 4: Add Pure Logical-Date Conversion and Replace the Skeleton with TaskFlow

**Files:**
- Create: `pipeline/orchestration_date.py`
- Create: `tests/pipeline/test_orchestration_date.py`
- Modify: `airflow/dags/bahtflow_daily.py`
- Modify: `tests/airflow/test_bahtflow_daily_dag.py`

**Interfaces:**
- Produces `batch_date_from_logical_date(logical_date: datetime | None) -> date`.
- DAG task IDs:
  - `resolve_batch_date`
  - `preflight`
  - `load_tx_raw`
  - `load_fx_raw`
  - `classify_transactions`
  - `build_currency_fact`
  - `finish`
- XCom batch date is ISO `YYYY-MM-DD` string only.

- [ ] **Step 1: Write logical-date RED tests independent of Airflow**

Create `tests/pipeline/test_orchestration_date.py`:

```python
from datetime import datetime, timezone

import pytest

from pipeline.orchestration_date import batch_date_from_logical_date


def test_logical_date_converts_to_asia_bangkok_before_taking_date():
    logical = datetime(2025, 7, 21, 18, 30, tzinfo=timezone.utc)
    assert batch_date_from_logical_date(logical).isoformat() == "2025-07-22"


def test_missing_logical_date_fails_instead_of_using_wall_clock():
    with pytest.raises(ValueError, match="logical date is required"):
        batch_date_from_logical_date(None)


def test_naive_logical_date_fails_instead_of_guessing_timezone():
    with pytest.raises(ValueError, match="timezone-aware"):
        batch_date_from_logical_date(datetime(2025, 7, 22, 0, 0))
```

- [ ] **Step 2: Run logical-date RED test**

```powershell
pytest tests/pipeline/test_orchestration_date.py -q
```

Expected: import failure because `pipeline.orchestration_date` does not exist.

- [ ] **Step 3: Implement the pure logical-date helper**

Create `pipeline/orchestration_date.py`:

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

- [ ] **Step 4: Run logical-date GREEN test**

```powershell
pytest tests/pipeline/test_orchestration_date.py -q
```

Expected: pass.

- [ ] **Step 5: Replace old skeleton assertions with TaskFlow source-contract RED tests**

Rewrite `tests/airflow/test_bahtflow_daily_dag.py` so it remains import-free in the host pytest environment and inspects the DAG source. The core assertions are:

```python
from pathlib import Path


DAG_PATH = Path(__file__).parents[2] / "airflow" / "dags" / "bahtflow_daily.py"


def _source() -> str:
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_uses_airflow3_taskflow_public_api():
    source = _source()
    assert "from airflow.sdk import dag, get_current_context, task" in source
    assert "@dag(" in source
    assert "@task" in source
    assert "EmptyOperator" in source
    assert "dbt_transform" not in source
    assert "dbt_test" not in source


def test_dag_daily_contract_is_serial_and_not_automatic_catchup():
    source = _source()
    assert 'schedule="@daily"' in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert 'tz="Asia/Bangkok"' in source


def test_dag_has_exact_f07_task_ids():
    source = _source()
    for task_id in (
        "resolve_batch_date",
        "preflight",
        "load_tx_raw",
        "load_fx_raw",
        "classify_transactions",
        "build_currency_fact",
        "finish",
    ):
        assert f'task_id="{task_id}"' in source


def test_dag_calls_existing_pipeline_services_instead_of_copying_business_logic():
    source = _source()
    for symbol in (
        "load_transaction_raw_batch",
        "load_fx_raw_batch",
        "classify_and_load_batch",
        "build_and_load_currency_fact",
        "run_preflight",
    ):
        assert symbol in source
    assert "pd.DataFrame" not in source
    assert "resolve_effective_fx" not in source
    assert "classify_transactions(" not in source


def test_dag_dependency_edges_match_approved_f07_shape():
    normalized = "\n".join(line.strip() for line in _source().splitlines())
    assert "ready >> [tx_raw, fx_raw]" in normalized
    assert "tx_raw >> classified" in normalized
    assert "[classified, fx_raw] >> fact" in normalized
    assert "fact >> finish" in normalized
```

- [ ] **Step 6: Run DAG source-contract tests and observe RED**

```powershell
pytest tests/airflow/test_bahtflow_daily_dag.py -q
```

Expected: failures because the current DAG is still the EmptyOperator/dbt skeleton.

- [ ] **Step 7: Replace `bahtflow_daily.py` with thin TaskFlow orchestration**

Use these public Airflow 3 imports:

```python
from __future__ import annotations

from dataclasses import asdict
from datetime import date

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

Define the DAG:

```python
@dag(
    dag_id="bahtflow_daily",
    description="BahtFlow daily Pandas pipeline through currency fact",
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bahtflow", "pandas", "bigquery"],
)
def bahtflow_daily():
```

Inside it, define `resolve_batch_date` as:

```python
    @task(task_id="resolve_batch_date")
    def resolve_batch_date() -> str:
        context = get_current_context()
        logical_date = context["dag_run"].logical_date
        return batch_date_from_logical_date(logical_date).isoformat()
```

Every downstream task converts the ISO string back with `date.fromisoformat(batch_date)` and constructs fresh adapters from settings. Do not pass adapters through XCom.

Preflight wrapper:

```python
    @task(task_id="preflight")
    def preflight(batch_date: str) -> dict:
        date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        summary = run_preflight(
            settings=settings,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        )
        return asdict(summary)
```

Transaction raw wrapper:

```python
    @task(task_id="load_tx_raw")
    def load_tx_raw(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(
            load_transaction_raw_batch(
                batch_date=d,
                bucket_name=settings.bucket_name,
                gcs_adapter=GcsAdapter(settings.project_id),
                bigquery_adapter=BigQueryAdapter(settings.project_id),
            )
        )
```

FX wrapper:

```python
    @task(task_id="load_fx_raw")
    def load_fx_raw(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(
            load_fx_raw_batch(
                batch_date=d,
                bucket_name=settings.bucket_name,
                gcs_adapter=GcsAdapter(settings.project_id),
                bigquery_adapter=BigQueryAdapter(settings.project_id),
            )
        )
```

Classification wrapper:

```python
    @task(task_id="classify_transactions")
    def classify_transactions_task(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(
            classify_and_load_batch(
                batch_date=d,
                bigquery_adapter=BigQueryAdapter(settings.project_id),
            )
        )
```

Currency fact wrapper:

```python
    @task(task_id="build_currency_fact")
    def build_currency_fact_task(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        return asdict(
            build_and_load_currency_fact(
                batch_date=d,
                bigquery_adapter=BigQueryAdapter(settings.project_id),
            )
        )
```

Wire dependencies exactly:

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

The `batch_date` XCom creates logical-date dependencies to all business tasks, but `ready >> ...` explicitly enforces fail-fast preflight before writes.

- [ ] **Step 8: Run host tests GREEN**

```powershell
pytest tests/pipeline/test_orchestration_date.py tests/airflow/test_bahtflow_daily_dag.py -q
pytest -q
```

Expected: all pass.

- [ ] **Step 9: Parse/import the real DAG inside the custom Airflow image**

Run:

```powershell
docker compose run --rm airflow-scheduler python -c "import importlib.util; p='/opt/bahtflow/airflow/dags/bahtflow_daily.py'; s=importlib.util.spec_from_file_location('bahtflow_daily', p); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); print('dag_import_ok')"
```

Expected final line:

```text
dag_import_ok
```

Then run Airflow's DAG list check:

```powershell
docker compose run --rm airflow-scheduler airflow dags list | Select-String bahtflow_daily
```

Expected: one `bahtflow_daily` DAG entry and no import-error output.

- [ ] **Step 10: Commit Task 4**

```powershell
git add pipeline/orchestration_date.py airflow/dags/bahtflow_daily.py tests/pipeline/test_orchestration_date.py tests/airflow/test_bahtflow_daily_dag.py
git commit -m "feat: orchestrate daily pipeline with Airflow TaskFlow"
```

---

### Task 5: Prove Runtime Preflight and One-Day E2E Idempotency in Airflow

**Files:**
- Modify after evidence: `README.md`
- No new business production files.

**Interfaces:**
- One-day logical date: `2025-07-22`.
- The acceptance uses the real `bahtflow_daily` DAG and the same TaskFlow code path used for historical processing.
- Because F04-F06 partitions for `2025-07-22` are already complete, the Airflow acceptance is intentionally a rerun/idempotency proof.

- [ ] **Step 1: Start the custom Airflow runtime cleanly**

Run:

```powershell
docker compose down
docker compose build airflow-api-server airflow-scheduler airflow-dag-processor airflow-init
docker compose up airflow-init
docker compose up -d postgres airflow-api-server airflow-scheduler airflow-dag-processor
```

Verify:

```powershell
docker compose ps
```

Expected: Postgres healthy, API server healthy, scheduler running, DAG processor running.

- [ ] **Step 2: Prove preflight dependencies from inside the Airflow runtime**

Run a direct read-only preflight invocation inside the same image/environment:

```powershell
@'
from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.preflight import run_preflight

settings = load_gcp_settings()
summary = run_preflight(
    settings=settings,
    gcs_adapter=GcsAdapter(settings.project_id),
    bigquery_adapter=BigQueryAdapter(settings.project_id),
)
print(summary)
'@ | docker compose run --rm -T airflow-scheduler python -
```

Expected: `datasets_verified=3`, `tables_verified=5`, configured project/bucket/location printed, no create operation and no credential error.

- [ ] **Step 3: Confirm the DAG is parsed before creating a historical run**

```powershell
docker compose exec airflow-scheduler airflow dags list | Select-String bahtflow_daily
```

Expected: `bahtflow_daily` appears.

Also check import errors:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected: no import error for `bahtflow_daily.py`.

- [ ] **Step 4: Create a single-date Airflow backfill run for 2025-07-22**

Use a one-date backfill so the logical date is explicit and non-null:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-22 --to-date 2025-07-22 --reprocess-behavior completed --max-active-runs 1
```

Do not pass `--run-backwards`.

- [ ] **Step 5: Wait for and inspect the one-day run**

Use the Airflow UI at local port 8080 or CLI/API-visible run state. Confirm all seven F07 tasks complete successfully:

```text
resolve_batch_date
preflight
load_tx_raw
load_fx_raw
classify_transactions
build_currency_fact
finish
```

Capture the task summaries from logs. For previously complete partitions, the important expected persistence evidence is:

```text
load_tx_raw.tx_inserted_rows = 0
load_fx_raw.fx_inserted_rows = 0
classify_transactions.accepted_inserted_rows = 0
classify_transactions.quarantine_inserted_rows = 0
build_currency_fact.fact_inserted_rows = 0
classify_transactions.reconciled = True
build_currency_fact.reconciled = True
```

Persisted counts must remain the known F04-F06 `2025-07-22` counts unless actual warehouse inspection proves otherwise:

```text
raw transactions = 8978
accepted = 8803
quarantine = 175
fact = 8803
```

- [ ] **Step 6: Run the same single-date Airflow path a second time**

Repeat:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-22 --to-date 2025-07-22 --reprocess-behavior completed --max-active-runs 1
```

Expected: a new completed Airflow run may be created for the same logical date under `reprocess-behavior completed`, but every persisted inserted-row metric remains zero and reconciliations remain true.

- [ ] **Step 7: Verify persisted one-day counts directly in BigQuery**

Use the PowerShell here-string pattern to avoid quoting problems:

```powershell
@'
from google.cloud import bigquery
from pipeline.config import load_gcp_settings

s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
queries = {
    "raw": "SELECT COUNT(*) n FROM `bahtflow-airflow-pandas-dbt.bahtflow_raw.transactions` WHERE batch_date = DATE('2025-07-22')",
    "accepted": "SELECT COUNT(*) n FROM `bahtflow-airflow-pandas-dbt.bahtflow_analytics.transactions_accepted` WHERE batch_date = DATE('2025-07-22')",
    "quarantine": "SELECT COUNT(*) n FROM `bahtflow-airflow-pandas-dbt.bahtflow_ops.transactions_quarantine` WHERE batch_date = DATE('2025-07-22')",
    "fact": "SELECT COUNT(*) n FROM `bahtflow-airflow-pandas-dbt.bahtflow_analytics.fct_transactions` WHERE batch_date = DATE('2025-07-22')",
}
for name, sql in queries.items():
    row = next(iter(c.query(sql).result()))
    print(f"{name}={row.n}")
'@ | docker compose run --rm -T airflow-scheduler python -
```

If the configured project ID ever changes, build the fully qualified identifiers from `s.project_id` rather than copying the current project literal.

Expected with the current accepted baseline:

```text
raw=8978
accepted=8803
quarantine=175
fact=8803
```

- [ ] **Step 8: Add only executed one-day evidence to README**

Add an F07 section describing:

- custom Airflow runtime
- logical-date-only batch contract
- serial explicit backfill strategy
- one-day `2025-07-22` command used
- actual Airflow run state
- actual inserted-row metrics from task logs
- actual persisted BigQuery counts
- note that repeated run inserts zero rows

Do not add Friday/weekend carry-forward results yet; those belong to Task 6 after execution.

- [ ] **Step 9: Run tests and commit one-day runbook/evidence**

```powershell
pytest -q
docker compose config --quiet
git diff --check

git add README.md
git commit -m "docs: add Airflow one-day E2E evidence"
```

---

### Task 6: Prove Forward Serial Historical Backfill and Live FX Carry-Forward

**Files:**
- Modify after evidence: `README.md`
- No new business production files unless a verified defect is discovered.

**Interfaces:**
- Backfill window: `2025-07-25` through `2025-07-27`.
- Required order: oldest-to-newest.
- Friday expected same-day FX.
- Saturday/Sunday expected `NO_NEW_RATE` on ingestion and carry Friday in facts.

- [ ] **Step 1: Dry-run the backfill date range first**

Run:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-25 --to-date 2025-07-27 --reprocess-behavior completed --max-active-runs 1 --dry-run
```

Expected considered logical dates in chronological order:

```text
2025-07-25
2025-07-26
2025-07-27
```

Do not add `--run-backwards`.

- [ ] **Step 2: Execute the real serial backfill**

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-25 --to-date 2025-07-27 --reprocess-behavior completed --max-active-runs 1
```

Expected: three DAG runs use the same seven F07 tasks and finish successfully, one active run at a time.

- [ ] **Step 3: Inspect FX task behavior for each date**

From Airflow task logs, capture the actual `load_fx_raw` summary:

```text
2025-07-25 -> fx_status=LOADED
2025-07-26 -> fx_status=NO_NEW_RATE
2025-07-27 -> fx_status=NO_NEW_RATE
```

If Friday has no published source or a holiday differs from this expected window, stop and inspect the actual source contract before changing test dates; do not fabricate evidence.

- [ ] **Step 4: Query persisted fact lineage for the three backfill dates**

Run:

```powershell
@'
from google.cloud import bigquery
from pipeline.config import load_gcp_settings

s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
sql = f"""
SELECT
  batch_date,
  fx_rate_date,
  is_carried_forward,
  staleness_days,
  COUNT(*) AS fact_rows,
  COUNT(DISTINCT source_row_id) AS distinct_source_rows
FROM `{s.project_id}.bahtflow_analytics.fct_transactions`
WHERE batch_date BETWEEN DATE('2025-07-25') AND DATE('2025-07-27')
GROUP BY batch_date, fx_rate_date, is_carried_forward, staleness_days
ORDER BY batch_date
"""
for row in c.query(sql).result():
    print(dict(row.items()))
'@ | docker compose run --rm -T airflow-scheduler python -
```

Required lineage:

```text
2025-07-25 -> fx_rate_date=2025-07-25, is_carried_forward=False, staleness_days=0
2025-07-26 -> fx_rate_date=2025-07-25, is_carried_forward=True,  staleness_days=1
2025-07-27 -> fx_rate_date=2025-07-25, is_carried_forward=True,  staleness_days=2
```

For each date, `fact_rows == distinct_source_rows` must hold.

- [ ] **Step 5: Prove accepted/fact partition reconciliation across the window**

Run:

```powershell
@'
from google.cloud import bigquery
from pipeline.config import load_gcp_settings

s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
sql = f"""
WITH accepted AS (
  SELECT batch_date, COUNT(*) AS accepted_rows
  FROM `{s.project_id}.bahtflow_analytics.transactions_accepted`
  WHERE batch_date BETWEEN DATE('2025-07-25') AND DATE('2025-07-27')
  GROUP BY batch_date
), fact AS (
  SELECT batch_date, COUNT(*) AS fact_rows
  FROM `{s.project_id}.bahtflow_analytics.fct_transactions`
  WHERE batch_date BETWEEN DATE('2025-07-25') AND DATE('2025-07-27')
  GROUP BY batch_date
)
SELECT a.batch_date, a.accepted_rows, f.fact_rows,
       a.accepted_rows = f.fact_rows AS reconciled
FROM accepted a
JOIN fact f USING (batch_date)
ORDER BY batch_date
"""
for row in c.query(sql).result():
    print(dict(row.items()))
'@ | docker compose run --rm -T airflow-scheduler python -
```

Expected: three rows and `reconciled=True` for all three dates.

- [ ] **Step 6: Re-run the same backfill range to prove persisted idempotency**

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-25 --to-date 2025-07-27 --reprocess-behavior completed --max-active-runs 1
```

Inspect the new run logs. Expected on the second completed execution of each date:

```text
transaction raw inserted = 0
FX inserted = 0 where source exists; 0 for NO_NEW_RATE dates
accepted inserted = 0
quarantine inserted = 0
fact inserted = 0
classification reconciled = True
fact reconciled = True
```

- [ ] **Step 7: Add executed historical/carry-forward evidence to README**

Record actual outputs from Steps 1-6, including:

- exact backfill command
- `max_active_runs=1`
- explicit note that `--run-backwards` was not used
- Friday/Saturday/Sunday FX ingestion statuses
- actual Friday rate date used
- `is_carried_forward` and staleness values
- actual fact row counts and reconciliation values
- second-run inserted-row evidence

Do not claim the full 360-day corpus has been processed; that remains F09.

- [ ] **Step 8: Run tests and commit historical evidence**

```powershell
pytest -q
docker compose config --quiet
git diff --check

git add README.md
git commit -m "docs: add Airflow backfill and carry-forward evidence"
```

---

### Task 7: Final F07 Verification, Diff Review, and Merge Readiness

**Files:**
- Verification only; modify code/docs only if a discovered defect is first reproduced and handled through systematic debugging + TDD.

**Interfaces:**
- Expected branch: `feat/07-airflow-e2e-backfill`.
- Base branch: `main` at or after F06 commit `71908366ca9667e4bceabd5ec4f4794917fc5b75`.

- [ ] **Step 1: Run the fresh full Python test suite**

```powershell
pytest -q
```

Expected: zero failures.

- [ ] **Step 2: Compile all F07 Python files**

```powershell
python -m py_compile pipeline/raw_load.py pipeline/bigquery_adapter.py pipeline/preflight.py pipeline/orchestration_date.py airflow/dags/bahtflow_daily.py
```

Expected: no output and exit code 0.

- [ ] **Step 3: Validate Docker Compose and rebuild the custom Airflow image**

```powershell
docker compose config --quiet
docker compose build airflow-api-server airflow-scheduler airflow-dag-processor
```

Expected: Compose validation and image build succeed.

- [ ] **Step 4: Verify DAG parsing/import from the exact runtime image**

```powershell
docker compose run --rm airflow-scheduler airflow dags list | Select-String bahtflow_daily
docker compose run --rm airflow-scheduler airflow dags list-import-errors
```

Expected: `bahtflow_daily` listed and no import error for the DAG.

- [ ] **Step 5: Run repository whitespace/status checks**

```powershell
git diff --check
git status --short
git diff --check main...feat/07-airflow-e2e-backfill
git diff --stat main...feat/07-airflow-e2e-backfill
```

Expected before the final review: no whitespace errors; working tree clean after committed work.

- [ ] **Step 6: Run tracked credential scan**

Use the same tracked-file credential scan pattern already used for F04-F06. At minimum ensure no ADC JSON, private key, access token, or actual secret was added. A portable PowerShell check can inspect tracked text for common credential markers:

```powershell
$patterns = 'private_key|client_secret|ya29\.|-----BEGIN PRIVATE KEY-----'
git grep -n -I -E $patterns -- . ':!docs/superpowers/plans/*' ':!docs/superpowers/specs/*'
```

Expected: no newly tracked credential value. If a false positive is a variable name or documentation text, inspect it rather than deleting blindly.

- [ ] **Step 7: Review the feature diff against the approved spec**

```powershell
git diff --name-status main...feat/07-airflow-e2e-backfill
git log --oneline main..feat/07-airflow-e2e-backfill
```

Confirm line-by-line:

```text
custom Airflow image present
GCP/Pandas dependencies available in Airflow runtime
repository + ADC runtime wiring present
preflight validate-only
TX/FX raw boundaries split
load_raw_batch compatibility preserved
logical-date-only batch date
TaskFlow DAG thin wrappers
catchup=False
max_active_runs=1
no DataFrame XCom
daily/single-day and historical use same DAG
backfill oldest-to-newest
live weekend carry-forward evidence recorded
F07 stops at fact
no marts/publish/full-360 scope creep
```

- [ ] **Step 8: Request code review before merge**

Invoke `superpowers:requesting-code-review`. If the current harness has no subagent dispatch capability, perform the review against the GitHub branch/diff with the available connector and state that limitation explicitly. Resolve all Critical/Important findings through reproduced tests before proceeding.

- [ ] **Step 9: Run fresh verification again after any review changes**

If review caused no code changes, the Step 1-6 evidence remains the current final gate. If any file changed, rerun:

```powershell
pytest -q
python -m py_compile pipeline/raw_load.py pipeline/bigquery_adapter.py pipeline/preflight.py pipeline/orchestration_date.py airflow/dags/bahtflow_daily.py
docker compose config --quiet
git diff --check
git status --short
```

Expected: all clean.

- [ ] **Step 10: Finish the branch without deleting it**

Invoke `superpowers:finishing-a-development-branch` and present its standard integration menu. If local merge to `main` is chosen, keep `feat/07-airflow-e2e-backfill` after merge per the project-wide branch-retention instruction.
