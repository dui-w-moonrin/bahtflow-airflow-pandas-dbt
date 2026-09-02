# Feature 04 Pandas Intake + Raw Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load one logical-date transaction batch and optional same-day FX from immutable GCS into existing BigQuery raw tables through Pandas, while preserving source strings and making an unchanged rerun insert zero additional rows.

**Architecture:** Reuse the F02 `GcsAdapter` for listing/downloading objects and checksum metadata. Put transport validation, raw-string preservation, deterministic source metadata, and anti-filtering in focused Pandas functions. Extend the F03 `BigQueryAdapter` only with partition-scoped source-ID lookup, append-only load jobs, and partition row counts. Put orchestration in `pipeline/raw_load.py`; keep `scripts/load_raw_batch.py` as a thin CLI.

**Tech Stack:** Python 3.12, pandas 2.3.3, `google-cloud-storage==3.13.1`, `google-cloud-bigquery==3.44.0`, BigQuery, GCS, pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-04-pandas-intake-design.md`

## Global Constraints

- F04 has one outcome only: one logical-date Pandas intake reaches BigQuery raw idempotently.
- Transaction intake requires exactly five canonical region objects: `bkk`, `central`, `north`, `northeast`, `south`.
- Same-day FX is optional. Missing FX returns `NO_NEW_RATE` and does not fail the transaction batch.
- Pandas reads source fields as strings with default NA interpretation disabled; literal `N/A`, blanks, malformed timestamps, malformed amounts, and lowercase currency remain source evidence.
- Do not parse/normalize `txn`, `dtts`, `amount`, or `currency` in F04.
- Do not deduplicate by transaction ID or business payload in F04.
- Verify every downloaded object against GCS metadata key `bahtflow-source-sha256` before Pandas processing.
- `source_row_number` is 1-based and excludes the CSV header.
- `source_row_id = SHA256(source_file | source_checksum | source_row_number)` for transaction and FX rows.
- `ingested_at` is one UTC timestamp per load invocation and is excluded from idempotency identity.
- Query existing `source_row_id` values only for the relevant BigQuery partition, anti-filter in Pandas, then append unseen rows.
- Thin-v1 assumes one writer per logical batch. Do not add staging tables, `MERGE`, locks, or concurrent-writer guarantees.
- Reuse only `bahtflow_raw.transactions` and `bahtflow_raw.fx_rates`.
- No F05 business DQ, F06 effective FX/conversion, F07 Airflow wiring/backfill, dbt, Spark, streaming, or full 360-day execution.
- Reuse existing GCP env vars and ADC/service-account impersonation. Add no credential mechanism.
- Live acceptance date is `2025-07-22`: committed manifests prove five TX files totaling **8,978 rows** and same-day FX with **2 rows**.

---

## File Map

- Modify: `requirements-gcp.txt` — add pinned Pandas dependency.
- Create: `pipeline/pandas_intake.py` — pure/source-focused Pandas validation, metadata shaping, deterministic IDs, anti-filter.
- Modify: `pipeline/bigquery_adapter.py` — partition-scoped ID query, append-only load job, partition row count.
- Create: `pipeline/raw_load.py` — one-date orchestration using the existing GCS/BigQuery adapters.
- Create: `scripts/load_raw_batch.py` — thin CLI.
- Create: `tests/pipeline/test_pandas_intake.py` — credential-free Pandas/source tests.
- Modify: `tests/pipeline/test_bigquery_adapter.py` — credential-free tests for new BQ methods.
- Create: `tests/pipeline/test_raw_load.py` — stateful first-run/rerun orchestration tests.
- Modify: `README.md` — minimum F04 runbook.

---

### Task 1: Add Pure Pandas Intake Logic

**Files:**
- Modify: `requirements-gcp.txt`
- Create: `pipeline/pandas_intake.py`
- Create: `tests/pipeline/test_pandas_intake.py`

**Interfaces:**
- Consumes: canonical GCS object names, source bytes, checksum metadata, `batch_date: date`, one `ingested_at: datetime`.
- Produces: `PandasIntakeError`, `EXPECTED_REGIONS`, `transaction_prefix`, `validate_transaction_objects`, `same_day_fx_object_name`, `verify_source_checksum`, `make_source_row_id`, `prepare_transaction_frame`, `prepare_fx_frame`, `anti_filter_existing`.

- [ ] **Step 1: Add Pandas and write discovery/checksum/source-ID tests**

Append to `requirements-gcp.txt`:

```text
pandas==2.3.3
```

Create `tests/pipeline/test_pandas_intake.py` with this initial content:

```python
from __future__ import annotations

import gzip
import hashlib
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    make_source_row_id,
    same_day_fx_object_name,
    transaction_prefix,
    validate_transaction_objects,
    verify_source_checksum,
)


def _gzip_csv(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"))


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tx_names(batch_date: date) -> list[str]:
    return [
        (
            f"transactions/business_date={batch_date.isoformat()}/"
            f"sales_{region}_{batch_date:%Y%m%d}.csv.gz"
        )
        for region in EXPECTED_REGIONS
    ]


def test_transaction_prefix_and_region_order_are_fixed():
    d = date(2025, 7, 22)
    assert EXPECTED_REGIONS == (
        "bkk", "central", "north", "northeast", "south"
    )
    assert transaction_prefix(d) == "transactions/business_date=2025-07-22/"


def test_five_canonical_transaction_objects_are_accepted():
    d = date(2025, 7, 22)
    result = validate_transaction_objects(d, _tx_names(d))
    assert tuple(result) == EXPECTED_REGIONS
    assert result["north"].endswith("sales_north_20250722.csv.gz")


def test_missing_or_unexpected_transaction_object_is_rejected():
    d = date(2025, 7, 22)
    names = _tx_names(d)
    with pytest.raises(PandasIntakeError, match="Transaction object set mismatch"):
        validate_transaction_objects(d, names[:-1])
    with pytest.raises(PandasIntakeError, match="Transaction object set mismatch"):
        validate_transaction_objects(
            d,
            [
                *names,
                "transactions/business_date=2025-07-22/sales_unknown_20250722.csv.gz",
            ],
        )


def test_duplicate_transaction_object_is_rejected():
    d = date(2025, 7, 22)
    names = _tx_names(d)
    with pytest.raises(PandasIntakeError, match="Duplicate transaction object"):
        validate_transaction_objects(d, [*names, names[0]])


def test_same_day_fx_object_name_is_canonical():
    assert same_day_fx_object_name(date(2025, 7, 22)) == (
        "fx/2025/07/fx_20250722.csv"
    )


def test_checksum_missing_mismatch_and_match():
    source = b"abc"
    with pytest.raises(PandasIntakeError, match="Missing source checksum metadata"):
        verify_source_checksum(source, "")
    with pytest.raises(PandasIntakeError, match="Source checksum mismatch"):
        verify_source_checksum(source, "0" * 64)
    assert verify_source_checksum(source, _sha256(source)) == _sha256(source)


def test_source_row_id_is_stable_and_row_number_sensitive():
    checksum = "a" * 64
    object_name = _tx_names(date(2025, 7, 22))[0]
    first = make_source_row_id(object_name, checksum, 1)
    assert first == make_source_row_id(object_name, checksum, 1)
    assert first != make_source_row_id(object_name, checksum, 2)
```

- [ ] **Step 2: Run RED**

```powershell
pip install -r requirements-gcp.txt
pytest tests/pipeline/test_pandas_intake.py -v
```

Expected: collection/import failure because `pipeline.pandas_intake` does not exist.

- [ ] **Step 3: Implement only the discovery/checksum/source-ID slice**

Create `pipeline/pandas_intake.py`:

```python
from __future__ import annotations

import hashlib
import io
from datetime import date, datetime, timezone

import pandas as pd

EXPECTED_REGIONS = ("bkk", "central", "north", "northeast", "south")
TRANSACTION_SOURCE_COLUMNS = ("txn", "dtts", "amount", "currency")
FX_SOURCE_COLUMNS = (
    "rate_date",
    "currency",
    "mid_rate",
    "rate_unit",
    "source_provider",
    "source_url",
)


class PandasIntakeError(RuntimeError):
    pass


def transaction_prefix(batch_date: date) -> str:
    return f"transactions/business_date={batch_date.isoformat()}/"


def _expected_transaction_name(batch_date: date, region: str) -> str:
    return (
        f"{transaction_prefix(batch_date)}"
        f"sales_{region}_{batch_date:%Y%m%d}.csv.gz"
    )


def validate_transaction_objects(
    batch_date: date,
    object_names: list[str],
) -> dict[str, str]:
    if len(object_names) != len(set(object_names)):
        raise PandasIntakeError("Duplicate transaction object discovered")
    expected = {
        region: _expected_transaction_name(batch_date, region)
        for region in EXPECTED_REGIONS
    }
    expected_names = set(expected.values())
    actual_names = set(object_names)
    if actual_names != expected_names:
        raise PandasIntakeError(
            "Transaction object set mismatch: "
            f"missing={sorted(expected_names - actual_names)} "
            f"unexpected={sorted(actual_names - expected_names)}"
        )
    return expected


def same_day_fx_object_name(batch_date: date) -> str:
    return f"fx/{batch_date:%Y}/{batch_date:%m}/fx_{batch_date:%Y%m%d}.csv"


def verify_source_checksum(source_bytes: bytes, expected_checksum: str) -> str:
    if not expected_checksum:
        raise PandasIntakeError("Missing source checksum metadata")
    actual = hashlib.sha256(source_bytes).hexdigest()
    if actual != expected_checksum:
        raise PandasIntakeError(
            "Source checksum mismatch: "
            f"expected={expected_checksum} actual={actual}"
        )
    return actual


def make_source_row_id(
    source_file: str,
    source_checksum: str,
    source_row_number: int,
) -> str:
    identity = f"{source_file}|{source_checksum}|{source_row_number}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()
```

- [ ] **Step 4: Run GREEN for the first slice**

```powershell
pytest tests/pipeline/test_pandas_intake.py -v
```

Expected: all current tests pass.

- [ ] **Step 5: Add transaction-frame imports and failing tests**

Replace the import block from `pipeline.pandas_intake` in the test file with:

```python
from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    make_source_row_id,
    prepare_transaction_frame,
    same_day_fx_object_name,
    transaction_prefix,
    validate_transaction_objects,
    verify_source_checksum,
)
```

Append:

```python

def test_transaction_frame_enforces_exact_header():
    d = date(2025, 7, 22)
    source = _gzip_csv(
        "txn,dtts,wrong,currency\nT1,2025-07-22 01:00:00,10,THB\n"
    )
    with pytest.raises(PandasIntakeError, match="Transaction header mismatch"):
        prepare_transaction_frame(
            source_bytes=source,
            source_file=_tx_names(d)[0],
            source_checksum=_sha256(source),
            region="bkk",
            batch_date=d,
            ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
        )


def test_transaction_frame_preserves_dirty_strings_and_metadata():
    d = date(2025, 7, 22)
    source = _gzip_csv(
        "txn,dtts,amount,currency\n"
        "T1,not-a-time,N/A,usd\n"
        "T1,,N/A,usd\n"
    )
    checksum = _sha256(source)
    frame = prepare_transaction_frame(
        source_bytes=source,
        source_file=_tx_names(d)[0],
        source_checksum=checksum,
        region="bkk",
        batch_date=d,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert frame.loc[0, "amount"] == "N/A"
    assert frame.loc[0, "dtts"] == "not-a-time"
    assert frame.loc[1, "dtts"] == ""
    assert frame.loc[0, "currency"] == "usd"
    assert frame["source_row_number"].tolist() == [1, 2]
    assert frame.loc[0, "source_row_id"] != frame.loc[1, "source_row_id"]
    assert frame["batch_date"].tolist() == ["2025-07-22", "2025-07-22"]
    assert frame["ingested_at"].tolist() == [
        "2025-07-22T02:00:00Z",
        "2025-07-22T02:00:00Z",
    ]
```

- [ ] **Step 6: Run RED for transaction shaping**

```powershell
pytest tests/pipeline/test_pandas_intake.py -v
```

Expected: import failure because `prepare_transaction_frame` does not exist.

- [ ] **Step 7: Implement transaction shaping**

Append to `pipeline/pandas_intake.py`:

```python

def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise PandasIntakeError("ingested_at must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _read_source_csv(
    source_bytes: bytes,
    *,
    expected_columns: tuple[str, ...],
    compression: str | None,
    label: str,
) -> pd.DataFrame:
    frame = pd.read_csv(
        io.BytesIO(source_bytes),
        compression=compression,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )
    actual = tuple(frame.columns)
    if actual != expected_columns:
        raise PandasIntakeError(
            f"{label} header mismatch: expected={expected_columns} actual={actual}"
        )
    return frame


def prepare_transaction_frame(
    *,
    source_bytes: bytes,
    source_file: str,
    source_checksum: str,
    region: str,
    batch_date: date,
    ingested_at: datetime,
) -> pd.DataFrame:
    verify_source_checksum(source_bytes, source_checksum)
    frame = _read_source_csv(
        source_bytes,
        expected_columns=TRANSACTION_SOURCE_COLUMNS,
        compression="gzip",
        label="Transaction",
    )
    frame["region"] = region
    frame["source_file"] = source_file
    frame["source_checksum"] = source_checksum
    frame["source_row_number"] = range(1, len(frame) + 1)
    frame["source_row_id"] = [
        make_source_row_id(source_file, source_checksum, row_number)
        for row_number in frame["source_row_number"]
    ]
    frame["batch_date"] = batch_date.isoformat()
    frame["ingested_at"] = _utc_text(ingested_at)
    return frame
```

- [ ] **Step 8: Run transaction GREEN**

```powershell
pytest tests/pipeline/test_pandas_intake.py -v
```

Expected: all current tests pass.

- [ ] **Step 9: Add FX/anti-filter imports and failing tests**

Replace the import block again with:

```python
from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    anti_filter_existing,
    make_source_row_id,
    prepare_fx_frame,
    prepare_transaction_frame,
    same_day_fx_object_name,
    transaction_prefix,
    validate_transaction_objects,
    verify_source_checksum,
)
```

Append:

```python

def test_fx_frame_preserves_raw_rate_date():
    d = date(2025, 7, 22)
    source = (
        "rate_date,currency,mid_rate,rate_unit,source_provider,source_url\n"
        "2025-07-22,USD,32.40,THB_PER_FOREIGN,BOT,https://example.test/usd\n"
        "2025-07-22,EUR,37.90,THB_PER_FOREIGN,BOT,https://example.test/eur\n"
    ).encode("utf-8")
    frame = prepare_fx_frame(
        source_bytes=source,
        source_file=same_day_fx_object_name(d),
        source_checksum=_sha256(source),
        rate_date=d,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert frame["rate_date_raw"].tolist() == ["2025-07-22", "2025-07-22"]
    assert frame["rate_date"].tolist() == ["2025-07-22", "2025-07-22"]
    assert frame["source_row_number"].tolist() == [1, 2]


def test_fx_header_mismatch_fails():
    source = b"wrong,currency,mid_rate,rate_unit,source_provider,source_url\n"
    with pytest.raises(PandasIntakeError, match="FX header mismatch"):
        prepare_fx_frame(
            source_bytes=source,
            source_file="fx/2025/07/fx_20250722.csv",
            source_checksum=_sha256(source),
            rate_date=date(2025, 7, 22),
            ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
        )


def test_anti_filter_keeps_only_unseen_source_rows():
    frame = pd.DataFrame(
        {"source_row_id": ["id-a", "id-b", "id-c"], "amount": ["1", "2", "3"]}
    )
    result = anti_filter_existing(frame, {"id-a", "id-c"})
    assert result["source_row_id"].tolist() == ["id-b"]
    assert result.index.tolist() == [0]
```

- [ ] **Step 10: Run FX/anti-filter RED**

```powershell
pytest tests/pipeline/test_pandas_intake.py -v
```

Expected: import failure because `prepare_fx_frame` and `anti_filter_existing` do not exist.

- [ ] **Step 11: Implement FX shaping and anti-filter**

Append:

```python

def prepare_fx_frame(
    *,
    source_bytes: bytes,
    source_file: str,
    source_checksum: str,
    rate_date: date,
    ingested_at: datetime,
) -> pd.DataFrame:
    verify_source_checksum(source_bytes, source_checksum)
    frame = _read_source_csv(
        source_bytes,
        expected_columns=FX_SOURCE_COLUMNS,
        compression=None,
        label="FX",
    )
    frame = frame.rename(columns={"rate_date": "rate_date_raw"})
    frame["source_file"] = source_file
    frame["source_checksum"] = source_checksum
    frame["source_row_number"] = range(1, len(frame) + 1)
    frame["source_row_id"] = [
        make_source_row_id(source_file, source_checksum, row_number)
        for row_number in frame["source_row_number"]
    ]
    frame["rate_date"] = rate_date.isoformat()
    frame["ingested_at"] = _utc_text(ingested_at)
    return frame


def anti_filter_existing(
    frame: pd.DataFrame,
    existing_ids: set[str],
) -> pd.DataFrame:
    if not existing_ids:
        return frame.reset_index(drop=True).copy()
    return frame.loc[~frame["source_row_id"].isin(existing_ids)].reset_index(
        drop=True
    )
```

- [ ] **Step 12: Run Task 1 GREEN and commit**

```powershell
pytest tests/pipeline/test_pandas_intake.py -v
git add requirements-gcp.txt pipeline/pandas_intake.py tests/pipeline/test_pandas_intake.py
git commit -m "feat: add Pandas raw intake shaping"
```

Expected: all Task 1 tests pass before commit.

---

### Task 2: Extend BigQuery Adapter for Partition-Scoped Raw Loading

**Files:**
- Modify: `pipeline/bigquery_adapter.py`
- Modify: `tests/pipeline/test_bigquery_adapter.py`

**Interfaces:**
- Produces: `query_source_row_ids(..., partition_date) -> set[str]`, `append_rows(..., rows, schema) -> int`, `query_partition_row_count(..., partition_date) -> int`.

- [ ] **Step 1: Replace credential-free fakes with a query/load-aware version**

At the top of `tests/pipeline/test_bigquery_adapter.py`, add:

```python
from datetime import date
```

Replace `FakeQueryJob` and `FakeClient` with this exact block, preserving the existing test functions below it:

```python
class FakeQueryJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeLoadJob:
    def result(self):
        return self


class FakeClient:
    def __init__(self):
        self.datasets = {}
        self.tables = {}
        self.query_value = 0
        self.query_rows = None
        self.query_calls = []
        self.load_calls = []

    def get_dataset(self, full_id):
        if full_id not in self.datasets:
            raise NotFound("missing dataset")
        return self.datasets[full_id]

    def create_dataset(self, dataset):
        ref = dataset.reference
        self.datasets[f"{ref.project}.{ref.dataset_id}"] = dataset
        return dataset

    def get_table(self, full_id):
        if full_id not in self.tables:
            raise NotFound("missing table")
        return self.tables[full_id]

    def create_table(self, table):
        ref = table.reference
        self.tables[f"{ref.project}.{ref.dataset_id}.{ref.table_id}"] = table
        return table

    def query(self, sql, job_config=None):
        self.query_calls.append((sql, job_config))
        rows = self.query_rows if self.query_rows is not None else [(self.query_value,)]
        return FakeQueryJob(rows)

    def load_table_from_json(self, rows, destination, job_config=None):
        self.load_calls.append((rows, destination, job_config))
        return FakeLoadJob()
```

- [ ] **Step 2: Add failing adapter tests**

Append:

```python

def test_query_source_row_ids_is_partition_scoped():
    client = FakeClient()
    client.query_rows = [("id-a",), ("id-b",)]
    adapter = BigQueryAdapter("proj", client=client)
    result = adapter.query_source_row_ids(
        "bahtflow_raw", "transactions", "batch_date", date(2025, 7, 22)
    )
    assert result == {"id-a", "id-b"}
    sql, job_config = client.query_calls[-1]
    assert "`proj.bahtflow_raw.transactions`" in sql
    assert "WHERE batch_date = @partition_date" in sql
    parameter = job_config.query_parameters[0]
    assert parameter.name == "partition_date"
    assert parameter.type_ == "DATE"
    assert parameter.value == date(2025, 7, 22)


def test_append_rows_uses_write_append_load_job():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    schema = (bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),)
    rows = [{"source_row_id": "id-a"}, {"source_row_id": "id-b"}]
    assert adapter.append_rows("bahtflow_raw", "transactions", rows, schema) == 2
    loaded_rows, destination, job_config = client.load_calls[-1]
    assert loaded_rows == rows
    assert destination == "proj.bahtflow_raw.transactions"
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert [(f.name, f.field_type, f.mode) for f in job_config.schema] == [
        ("source_row_id", "STRING", "REQUIRED")
    ]


def test_append_rows_skips_empty_input():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    assert adapter.append_rows("bahtflow_raw", "transactions", [], ()) == 0
    assert client.load_calls == []


def test_query_partition_row_count_is_partition_scoped():
    client = FakeClient()
    client.query_rows = [(8978,)]
    adapter = BigQueryAdapter("proj", client=client)
    count = adapter.query_partition_row_count(
        "bahtflow_raw", "transactions", "batch_date", date(2025, 7, 22)
    )
    assert count == 8978
    sql, job_config = client.query_calls[-1]
    assert "SELECT COUNT(*)" in sql
    assert "WHERE batch_date = @partition_date" in sql
    assert job_config.query_parameters[0].value == date(2025, 7, 22)
```

- [ ] **Step 3: Run RED**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -v
```

Expected: new tests fail because the new methods do not exist.

- [ ] **Step 4: Implement the narrow methods**

Add to imports in `pipeline/bigquery_adapter.py`:

```python
from datetime import date
```

Append these methods inside `BigQueryAdapter`:

```python
    def _partition_job_config(self, partition_date: date):
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "partition_date", "DATE", partition_date
                )
            ]
        )

    def query_source_row_ids(
        self,
        dataset_id: str,
        table_id: str,
        partition_field: str,
        partition_date: date,
    ) -> set[str]:
        full_id = f"{self._project_id}.{dataset_id}.{table_id}"
        sql = (
            f"SELECT source_row_id FROM `{full_id}` "
            f"WHERE {partition_field} = @partition_date"
        )
        rows = self._client.query(
            sql,
            job_config=self._partition_job_config(partition_date),
        ).result()
        return {str(row[0]) for row in rows}

    def append_rows(
        self,
        dataset_id: str,
        table_id: str,
        rows: list[dict],
        schema,
    ) -> int:
        if not rows:
            return 0
        full_id = f"{self._project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(
            schema=list(schema),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self._client.load_table_from_json(
            rows, full_id, job_config=job_config
        ).result()
        return len(rows)

    def query_partition_row_count(
        self,
        dataset_id: str,
        table_id: str,
        partition_field: str,
        partition_date: date,
    ) -> int:
        full_id = f"{self._project_id}.{dataset_id}.{table_id}"
        sql = (
            f"SELECT COUNT(*) FROM `{full_id}` "
            f"WHERE {partition_field} = @partition_date"
        )
        rows = self._client.query(
            sql,
            job_config=self._partition_job_config(partition_date),
        ).result()
        return int(next(iter(rows))[0])
```

- [ ] **Step 5: Run GREEN and commit**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_bigquery_contract.py -v
git add pipeline/bigquery_adapter.py tests/pipeline/test_bigquery_adapter.py
git commit -m "feat: add partition-scoped raw load adapter"
```

Expected: all adapter/contract tests pass before commit.

---

### Task 3: Add One-Date Raw Load Orchestration and CLI

**Files:**
- Create: `pipeline/raw_load.py`
- Create: `scripts/load_raw_batch.py`
- Create: `tests/pipeline/test_raw_load.py`

**Interfaces:**
- Produces: `RawLoadSummary`, `load_raw_batch(...)->RawLoadSummary`, `python -m scripts.load_raw_batch --batch-date YYYY-MM-DD`.

- [ ] **Step 1: Write stateful first-run/rerun tests**

Create `tests/pipeline/test_raw_load.py`:

```python
from __future__ import annotations

import gzip
import hashlib
from datetime import date, datetime, timezone

from pipeline.gcs_adapter import ObjectMetadata
from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY
from pipeline.raw_load import load_raw_batch


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tx_object(d: date, region: str) -> str:
    return (
        f"transactions/business_date={d.isoformat()}/"
        f"sales_{region}_{d:%Y%m%d}.csv.gz"
    )


class FakeGcsAdapter:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def list_object_names(self, bucket_name: str, prefix: str = "") -> list[str]:
        return sorted(name for name in self.objects if name.startswith(prefix))

    def get_object_metadata(self, bucket_name: str, object_name: str) -> ObjectMetadata:
        if object_name not in self.objects:
            return ObjectMetadata(exists=False, metadata={})
        data = self.objects[object_name]
        return ObjectMetadata(
            exists=True,
            metadata={SOURCE_SHA256_METADATA_KEY: _sha256(data)},
        )

    def download_bytes(self, bucket_name: str, object_name: str) -> bytes:
        return self.objects[object_name]


class FakeBigQueryAdapter:
    def __init__(self):
        self.ids = {
            ("transactions", "2025-07-22"): set(),
            ("fx_rates", "2025-07-22"): set(),
        }
        self.rows = {
            ("transactions", "2025-07-22"): [],
            ("fx_rates", "2025-07-22"): [],
        }

    def query_source_row_ids(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        return set(self.ids[(table_id, partition_date.isoformat())])

    def append_rows(self, dataset_id, table_id, rows, schema):
        if not rows:
            return 0
        key_date = (
            rows[0]["batch_date"]
            if table_id == "transactions"
            else rows[0]["rate_date"]
        )
        key = (table_id, key_date)
        self.rows[key].extend(rows)
        self.ids[key].update(row["source_row_id"] for row in rows)
        return len(rows)

    def query_partition_row_count(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        return len(self.rows[(table_id, partition_date.isoformat())])


def _objects_with_fx(d: date) -> dict[str, bytes]:
    objects = {}
    for region in ("bkk", "central", "north", "northeast", "south"):
        objects[_tx_object(d, region)] = gzip.compress(
            (
                "txn,dtts,amount,currency\n"
                f"{region}-1,not-a-time,N/A,usd\n"
            ).encode("utf-8")
        )
    fx_name = f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"
    objects[fx_name] = (
        "rate_date,currency,mid_rate,rate_unit,source_provider,source_url\n"
        f"{d.isoformat()},USD,32.4,THB_PER_FOREIGN,BOT,https://example.test/usd\n"
        f"{d.isoformat()},EUR,37.9,THB_PER_FOREIGN,BOT,https://example.test/eur\n"
    ).encode("utf-8")
    return objects


def test_first_run_loads_and_rerun_inserts_zero_rows():
    d = date(2025, 7, 22)
    gcs = FakeGcsAdapter(_objects_with_fx(d))
    bq = FakeBigQueryAdapter()
    first = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=gcs,
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    second = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=gcs,
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 3, 0, tzinfo=timezone.utc),
    )
    assert (first.tx_files, first.tx_source_rows, first.tx_inserted_rows) == (5, 5, 5)
    assert (first.fx_status, first.fx_source_rows, first.fx_inserted_rows) == (
        "LOADED", 2, 2
    )
    assert (second.tx_inserted_rows, second.tx_partition_rows) == (0, 5)
    assert (second.fx_inserted_rows, second.fx_partition_rows) == (0, 2)
    assert bq.rows[("transactions", "2025-07-22")][0]["amount"] == "N/A"
    assert bq.rows[("transactions", "2025-07-22")][0]["dtts"] == "not-a-time"


def test_missing_same_day_fx_returns_no_new_rate():
    d = date(2025, 7, 22)
    objects = _objects_with_fx(d)
    del objects[f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"]
    summary = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(objects),
        bigquery_adapter=FakeBigQueryAdapter(),
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert summary.tx_inserted_rows == 5
    assert summary.fx_status == "NO_NEW_RATE"
    assert summary.fx_source_rows == 0
    assert summary.fx_inserted_rows == 0
    assert summary.fx_partition_rows == 0
```

- [ ] **Step 2: Run RED**

```powershell
pytest tests/pipeline/test_raw_load.py -v
```

Expected: collection/import failure because `pipeline.raw_load` does not exist.

- [ ] **Step 3: Implement one-date orchestration**

Create `pipeline/raw_load.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import pandas as pd

from pipeline.bigquery_contract import (
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)
from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY
from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    anti_filter_existing,
    prepare_fx_frame,
    prepare_transaction_frame,
    same_day_fx_object_name,
    transaction_prefix,
    validate_transaction_objects,
)


@dataclass(frozen=True)
class RawLoadSummary:
    batch_date: str
    tx_files: int
    tx_source_rows: int
    tx_inserted_rows: int
    tx_partition_rows: int
    fx_status: str
    fx_source_rows: int
    fx_inserted_rows: int
    fx_partition_rows: int


def _required_checksum(metadata) -> str:
    checksum = metadata.metadata.get(SOURCE_SHA256_METADATA_KEY)
    if not checksum:
        raise PandasIntakeError("Missing source checksum metadata")
    return checksum


def _load_tx_frame(*, batch_date, bucket_name, gcs_adapter, ingested_at):
    names = gcs_adapter.list_object_names(
        bucket_name, prefix=transaction_prefix(batch_date)
    )
    by_region = validate_transaction_objects(batch_date, names)
    frames = []
    for region in EXPECTED_REGIONS:
        object_name = by_region[region]
        metadata = gcs_adapter.get_object_metadata(bucket_name, object_name)
        if not metadata.exists:
            raise PandasIntakeError(
                f"Discovered transaction object disappeared: {object_name}"
            )
        source_bytes = gcs_adapter.download_bytes(bucket_name, object_name)
        frames.append(
            prepare_transaction_frame(
                source_bytes=source_bytes,
                source_file=object_name,
                source_checksum=_required_checksum(metadata),
                region=region,
                batch_date=batch_date,
                ingested_at=ingested_at,
            )
        )
    return pd.concat(frames, ignore_index=True)


def _load_fx_frame(*, batch_date, bucket_name, gcs_adapter, ingested_at):
    object_name = same_day_fx_object_name(batch_date)
    metadata = gcs_adapter.get_object_metadata(bucket_name, object_name)
    if not metadata.exists:
        return "NO_NEW_RATE", pd.DataFrame()
    source_bytes = gcs_adapter.download_bytes(bucket_name, object_name)
    return "LOADED", prepare_fx_frame(
        source_bytes=source_bytes,
        source_file=object_name,
        source_checksum=_required_checksum(metadata),
        rate_date=batch_date,
        ingested_at=ingested_at,
    )


def load_raw_batch(
    *,
    batch_date: date,
    bucket_name: str,
    gcs_adapter,
    bigquery_adapter,
    ingested_at: datetime | None = None,
) -> RawLoadSummary:
    invocation_time = ingested_at or datetime.now(timezone.utc)
    tx_frame = _load_tx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    tx_existing = bigquery_adapter.query_source_row_ids(
        "bahtflow_raw", "transactions", TRANSACTIONS_PARTITION_FIELD, batch_date
    )
    tx_new = anti_filter_existing(tx_frame, tx_existing)
    tx_inserted = bigquery_adapter.append_rows(
        "bahtflow_raw",
        "transactions",
        tx_new.to_dict(orient="records"),
        TRANSACTIONS_SCHEMA,
    )
    tx_partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw", "transactions", TRANSACTIONS_PARTITION_FIELD, batch_date
    )

    fx_status, fx_frame = _load_fx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    fx_inserted = 0
    if fx_status == "LOADED":
        fx_existing = bigquery_adapter.query_source_row_ids(
            "bahtflow_raw", "fx_rates", FX_RATES_PARTITION_FIELD, batch_date
        )
        fx_new = anti_filter_existing(fx_frame, fx_existing)
        fx_inserted = bigquery_adapter.append_rows(
            "bahtflow_raw",
            "fx_rates",
            fx_new.to_dict(orient="records"),
            FX_RATES_SCHEMA,
        )
    fx_partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw", "fx_rates", FX_RATES_PARTITION_FIELD, batch_date
    )

    return RawLoadSummary(
        batch_date=batch_date.isoformat(),
        tx_files=5,
        tx_source_rows=len(tx_frame),
        tx_inserted_rows=tx_inserted,
        tx_partition_rows=tx_partition_rows,
        fx_status=fx_status,
        fx_source_rows=len(fx_frame),
        fx_inserted_rows=fx_inserted,
        fx_partition_rows=fx_partition_rows,
    )
```

- [ ] **Step 4: Run orchestration GREEN**

```powershell
pytest tests/pipeline/test_raw_load.py -v
```

Expected: both orchestration tests pass.

- [ ] **Step 5: Add thin CLI**

Create `scripts/load_raw_batch.py`:

```python
from __future__ import annotations

import argparse
from datetime import date

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.raw_load import load_raw_batch


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True, type=date.fromisoformat)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_gcp_settings()
    summary = load_raw_batch(
        batch_date=args.batch_date,
        bucket_name=settings.bucket_name,
        gcs_adapter=GcsAdapter(settings.project_id),
        bigquery_adapter=BigQueryAdapter(settings.project_id),
    )
    for key, value in (
        ("batch_date", summary.batch_date),
        ("tx_files", summary.tx_files),
        ("tx_source_rows", summary.tx_source_rows),
        ("tx_inserted_rows", summary.tx_inserted_rows),
        ("tx_partition_rows", summary.tx_partition_rows),
        ("fx_status", summary.fx_status),
        ("fx_source_rows", summary.fx_source_rows),
        ("fx_inserted_rows", summary.fx_inserted_rows),
        ("fx_partition_rows", summary.fx_partition_rows),
    ):
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run focused tests/static checks and commit**

```powershell
pytest tests/pipeline/test_pandas_intake.py `
       tests/pipeline/test_bigquery_adapter.py `
       tests/pipeline/test_bigquery_contract.py `
       tests/pipeline/test_raw_load.py -v
python -m py_compile `
  pipeline/pandas_intake.py `
  pipeline/raw_load.py `
  pipeline/bigquery_adapter.py `
  scripts/load_raw_batch.py
git add pipeline/raw_load.py scripts/load_raw_batch.py tests/pipeline/test_raw_load.py
git commit -m "feat: load one raw batch idempotently"
```

Expected: focused tests pass and compile emits no output before commit.

---

### Task 4: Live One-Date Acceptance and Thin Runbook

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: completed F04 CLI and current GCP runtime.
- Produces: reproducible first-run/rerun evidence for `2025-07-22`.

- [ ] **Step 1: Sync/rebuild and run full local gate**

```powershell
git switch feat/04-pandas-intake
git pull --ff-only origin feat/04-pandas-intake
pip install -r requirements-gcp.txt
docker compose --profile gcp build gcp-toolbox
pytest
python -m py_compile `
  pipeline/pandas_intake.py `
  pipeline/raw_load.py `
  pipeline/bigquery_adapter.py `
  scripts/load_raw_batch.py
docker compose config --quiet
git diff --check
```

Expected: 0 test failures and no static-check errors.

- [ ] **Step 2: Prove acceptance partitions start empty**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from pipeline.config import load_gcp_settings; from pipeline.bigquery_adapter import BigQueryAdapter; from datetime import date; s=load_gcp_settings(); a=BigQueryAdapter(s.project_id); d=date(2025,7,22); print('tx_before=',a.query_partition_row_count('bahtflow_raw','transactions','batch_date',d)); print('fx_before=',a.query_partition_row_count('bahtflow_raw','fx_rates','rate_date',d))"
```

Required:

```text
tx_before= 0
fx_before= 0
```

If nonzero, stop; do not truncate/delete automatically.

- [ ] **Step 3: First live load**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.load_raw_batch --batch-date 2025-07-22
```

Required:

```text
batch_date=2025-07-22
tx_files=5
tx_source_rows=8978
tx_inserted_rows=8978
tx_partition_rows=8978
fx_status=LOADED
fx_source_rows=2
fx_inserted_rows=2
fx_partition_rows=2
```

- [ ] **Step 4: Exact rerun idempotency proof**

Run the same command again. Required:

```text
batch_date=2025-07-22
tx_files=5
tx_source_rows=8978
tx_inserted_rows=0
tx_partition_rows=8978
fx_status=LOADED
fx_source_rows=2
fx_inserted_rows=0
fx_partition_rows=2
```

- [ ] **Step 5: Compare one deterministic raw row with source strings**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from datetime import date,datetime,timezone; from pipeline.config import load_gcp_settings; from pipeline.gcs_adapter import GcsAdapter; from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY; from pipeline.pandas_intake import prepare_transaction_frame; from google.cloud import bigquery; s=load_gcp_settings(); g=GcsAdapter(s.project_id); d=date(2025,7,22); o='transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz'; m=g.get_object_metadata(s.bucket_name,o); b=g.download_bytes(s.bucket_name,o); f=prepare_transaction_frame(source_bytes=b,source_file=o,source_checksum=m.metadata[SOURCE_SHA256_METADATA_KEY],region='bkk',batch_date=d,ingested_at=datetime.now(timezone.utc)); r=f.iloc[0]; c=bigquery.Client(project=s.project_id); q='SELECT txn,dtts,amount,currency FROM `'+s.project_id+'.bahtflow_raw.transactions` WHERE source_row_id=@id'; rows=list(c.query(q,job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('id','STRING',r.source_row_id)])).result()); print('raw_strings_match=',list(rows[0])==[r.txn,r.dtts,r.amount,r.currency])"
```

Required:

```text
raw_strings_match=True
```

Task 1 unit tests separately prove literal `N/A`, malformed timestamp text, lowercase currency, and blanks survive Pandas intake unchanged.

- [ ] **Step 6: Verify credential hygiene**

```powershell
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
```

Expected: no output.

- [ ] **Step 7: Add README F04 runbook**

Add `## Feature 04: Pandas intake + idempotent raw load` documenting this command:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.load_raw_batch --batch-date 2025-07-22
```

State explicitly:

- TX requires exactly five canonical regional files.
- Same-day FX may report `NO_NEW_RATE`.
- Source checksum metadata is verified before Pandas reads bytes.
- F04 preserves transaction business fields as raw strings.
- An unchanged rerun must report `tx_inserted_rows=0` and, when FX exists, `fx_inserted_rows=0`.
- F04 does not classify DQ failures or resolve effective FX.

- [ ] **Step 8: Final F04 gate and commit**

```powershell
pytest
python -m py_compile `
  pipeline/pandas_intake.py `
  pipeline/raw_load.py `
  pipeline/bigquery_adapter.py `
  scripts/load_raw_batch.py
docker compose config --quiet
git diff --check
git status --short
git add README.md
git commit -m "docs: add Feature 04 raw load runbook"
```

Expected: tests/static checks are green before the README commit.

---

## Final F04 Gate

F04 is complete only with fresh evidence for all of these:

```text
transaction files = 5/5
2025-07-22 transaction source rows = 8,978
first transaction insert = 8,978
second transaction insert = 0
transaction partition remains = 8,978

same-day FX status = LOADED
FX source rows = 2
first FX insert = 2
second FX insert = 0
FX partition remains = 2

raw_strings_match = True
unit tests prove dirty strings survive Pandas intake
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
credential-file search = empty
```

Do not start F05 until this gate is green. F05 owns business-quality classification, accepted/quarantine split, duplicate business semantics, and raw-to-classified reconciliation.
