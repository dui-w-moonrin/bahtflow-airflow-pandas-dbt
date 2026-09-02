# Feature 05 Pandas Data Quality Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Classify one raw transaction batch with Pandas into typed accepted rows and auditable quarantine rows, persist both outputs idempotently in BigQuery, and prove deterministic batch-local duplicate semantics and rerun recovery.

**Architecture:** Read one `bahtflow_raw.transactions` partition, run all business validation and duplicate classification in pure-ish Pandas functions, then append unseen rows to `bahtflow_analytics.transactions_accepted` and `bahtflow_ops.transactions_quarantine` using `source_row_id` as the partition-scoped idempotency key. BigQuery remains the warehouse boundary and performs only storage/query mechanics; classification is batch-local and append-only in v1.

**Tech Stack:** Python 3.12, Pandas 2.3.3, Python `decimal.Decimal`, Google Cloud BigQuery client 3.44.0, pytest 9.x, Docker Compose GCP toolbox.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-05-pandas-dq-classification-design.md`

## Global Constraints

- Pandas owns all Feature 05 business validation and classification logic.
- Classification operates on exactly one `batch_date` at a time.
- Duplicate comparison is batch-local in v1; do not add global cross-batch reclassification.
- Base-invalid rows do not participate in duplicate comparison.
- Accepted output is typed/canonical; quarantine output preserves raw business fields plus source lineage and ordered `reason_codes`.
- Accepted BigQuery table is `bahtflow_analytics.transactions_accepted`, partitioned DAY by `batch_date`.
- Quarantine BigQuery table is `bahtflow_ops.transactions_quarantine`, partitioned DAY by `batch_date`.
- Accepted `transaction_dt` is BigQuery `DATETIME`; do not invent a timezone for source `dtts`.
- Accepted `amount` is BigQuery `NUMERIC` and uses decimal semantics, not binary floating point.
- Accepted currencies are only `THB`, `USD`, and `EUR`; canonicalization is trim + uppercase.
- Canonical regions remain exactly `bkk`, `central`, `north`, `northeast`, and `south`.
- Canonical `txn` trims surrounding whitespace only; no regex and no case conversion.
- Exact replay winner is deterministic by `(source_file ASC, source_row_number ASC)`.
- Persistence is append-only and single-writer in v1: query existing `source_row_id`, Pandas anti-filter, `WRITE_APPEND` unseen rows.
- An unchanged rerun must insert zero accepted rows and zero quarantine rows.
- Partial retry must recover without rollback: already persisted output IDs are skipped and missing output rows are appended.
- Do not implement FX resolution, currency conversion, facts, marts, Airflow production wiring, dbt, Great Expectations, Spark, staging/MERGE, or concurrent-writer behavior in F05.

---

## File Structure

**Create:**

- `pipeline/transaction_classification.py` — pure-ish Pandas base validation, canonicalization, duplicate rules, and final accepted/quarantine projections.
- `pipeline/classification_load.py` — one-batch BigQuery read/classify/idempotent-write/reconciliation orchestration.
- `scripts/bootstrap_classification.py` — idempotently create/verify the two F05 output tables.
- `scripts/classify_transactions.py` — CLI for one `--batch-date` classification run.
- `tests/pipeline/test_transaction_classification.py` — business-rule and deterministic duplicate tests.
- `tests/pipeline/test_classification_load.py` — persistence, rerun, partial retry, and reconciliation tests with a stateful fake BigQuery boundary.
- `tests/scripts/test_bootstrap_classification.py` — bootstrap wiring test.
- `tests/scripts/test_classify_transactions.py` — CLI summary/wiring test.

**Modify:**

- `pipeline/bigquery_contract.py` — accepted/quarantine schema and table constants.
- `pipeline/bigquery_adapter.py` — focused partition-row read helper; reuse existing ID/count/append helpers.
- `tests/pipeline/test_bigquery_contract.py` — exact F05 schema/partition assertions.
- `tests/pipeline/test_bigquery_adapter.py` — partition-row read helper test.
- `README.md` — Feature 05 bootstrap, classification, rerun, and acceptance runbook.

No generic repository/service abstraction is introduced.

---

### Task 1: Lock the BigQuery Accepted/Quarantine Contracts and Bootstrap

**Files:**
- Modify: `pipeline/bigquery_contract.py`
- Create: `scripts/bootstrap_classification.py`
- Modify: `tests/pipeline/test_bigquery_contract.py`
- Create: `tests/scripts/test_bootstrap_classification.py`

**Interfaces:**
- Consumes: existing `BigQueryAdapter.ensure_partitioned_table(dataset_id, table_id, schema, partition_field) -> str` and `load_gcp_settings()`.
- Produces constants used by later tasks:
  - `ACCEPTED_DATASET_ID = "bahtflow_analytics"`
  - `ACCEPTED_TABLE_ID = "transactions_accepted"`
  - `ACCEPTED_TRANSACTIONS_SCHEMA`
  - `ACCEPTED_TRANSACTIONS_PARTITION_FIELD = "batch_date"`
  - `QUARANTINE_DATASET_ID = "bahtflow_ops"`
  - `QUARANTINE_TABLE_ID = "transactions_quarantine"`
  - `QUARANTINE_TRANSACTIONS_SCHEMA`
  - `QUARANTINE_TRANSACTIONS_PARTITION_FIELD = "batch_date"`
- Produces `bootstrap_classification(adapter) -> list[tuple[str, str]]` where each tuple is `(full_table_name, status)`.

- [ ] **Step 1: Write failing contract tests for exact F05 schemas**

Add to `tests/pipeline/test_bigquery_contract.py`:

```python
from pipeline.bigquery_contract import (
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
)


def _shape(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def test_accepted_transactions_contract_is_exact():
    assert _shape(ACCEPTED_TRANSACTIONS_SCHEMA) == [
        ("txn", "STRING", "REQUIRED"),
        ("transaction_dt", "DATETIME", "REQUIRED"),
        ("amount", "NUMERIC", "REQUIRED"),
        ("currency", "STRING", "REQUIRED"),
        ("region", "STRING", "REQUIRED"),
        ("source_file", "STRING", "REQUIRED"),
        ("source_checksum", "STRING", "REQUIRED"),
        ("source_row_number", "INTEGER", "REQUIRED"),
        ("source_row_id", "STRING", "REQUIRED"),
        ("batch_date", "DATE", "REQUIRED"),
        ("ingested_at", "TIMESTAMP", "REQUIRED"),
        ("classified_at", "TIMESTAMP", "REQUIRED"),
    ]
    assert ACCEPTED_TRANSACTIONS_PARTITION_FIELD == "batch_date"


def test_quarantine_transactions_contract_is_exact():
    assert _shape(QUARANTINE_TRANSACTIONS_SCHEMA) == [
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
        ("reason_codes", "STRING", "REPEATED"),
        ("quarantined_at", "TIMESTAMP", "REQUIRED"),
    ]
    assert QUARANTINE_TRANSACTIONS_PARTITION_FIELD == "batch_date"
```

- [ ] **Step 2: Run the focused contract tests and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: collection/import failure because the F05 contract constants do not exist yet.

- [ ] **Step 3: Add the exact F05 table constants and schemas**

Append to `pipeline/bigquery_contract.py`:

```python
ACCEPTED_DATASET_ID = "bahtflow_analytics"
ACCEPTED_TABLE_ID = "transactions_accepted"
ACCEPTED_TRANSACTIONS_SCHEMA = (
    bigquery.SchemaField("txn", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("transaction_dt", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("classified_at", "TIMESTAMP", mode="REQUIRED"),
)
ACCEPTED_TRANSACTIONS_PARTITION_FIELD = "batch_date"

QUARANTINE_DATASET_ID = "bahtflow_ops"
QUARANTINE_TABLE_ID = "transactions_quarantine"
QUARANTINE_TRANSACTIONS_SCHEMA = (
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
    bigquery.SchemaField("reason_codes", "STRING", mode="REPEATED"),
    bigquery.SchemaField("quarantined_at", "TIMESTAMP", mode="REQUIRED"),
)
QUARANTINE_TRANSACTIONS_PARTITION_FIELD = "batch_date"
```

- [ ] **Step 4: Re-run the focused contract tests and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: all contract tests pass.

- [ ] **Step 5: Write the failing bootstrap wiring test**

Create `tests/scripts/test_bootstrap_classification.py`:

```python
from scripts.bootstrap_classification import bootstrap_classification


class RecordingAdapter:
    def __init__(self):
        self.calls = []

    def ensure_partitioned_table(self, dataset_id, table_id, schema, partition_field):
        self.calls.append((dataset_id, table_id, schema, partition_field))
        return "verified"


def test_bootstrap_classification_targets_exactly_two_f05_tables():
    adapter = RecordingAdapter()

    statuses = bootstrap_classification(adapter)

    assert [name for name, _ in statuses] == [
        "bahtflow_analytics.transactions_accepted",
        "bahtflow_ops.transactions_quarantine",
    ]
    assert [status for _, status in statuses] == ["verified", "verified"]
    assert [(dataset, table, partition) for dataset, table, _, partition in adapter.calls] == [
        ("bahtflow_analytics", "transactions_accepted", "batch_date"),
        ("bahtflow_ops", "transactions_quarantine", "batch_date"),
    ]
```

- [ ] **Step 6: Run the bootstrap test and observe RED**

Run:

```powershell
pytest tests/scripts/test_bootstrap_classification.py -v
```

Expected: `ModuleNotFoundError: No module named 'scripts.bootstrap_classification'`.

- [ ] **Step 7: Implement the minimal bootstrap script**

Create `scripts/bootstrap_classification.py`:

```python
from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
)
from pipeline.config import load_gcp_settings


def bootstrap_classification(adapter):
    targets = (
        (
            ACCEPTED_DATASET_ID,
            ACCEPTED_TABLE_ID,
            ACCEPTED_TRANSACTIONS_SCHEMA,
            ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        ),
        (
            QUARANTINE_DATASET_ID,
            QUARANTINE_TABLE_ID,
            QUARANTINE_TRANSACTIONS_SCHEMA,
            QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
        ),
    )
    statuses = []
    for dataset_id, table_id, schema, partition_field in targets:
        status = adapter.ensure_partitioned_table(
            dataset_id, table_id, schema, partition_field
        )
        statuses.append((f"{dataset_id}.{table_id}", status))
    return statuses


def main() -> None:
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)
    for table_name, status in bootstrap_classification(adapter):
        print(f"table={table_name} status={status}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Run the Task 1 tests and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py tests/scripts/test_bootstrap_classification.py -v
```

Expected: all tests pass.

- [ ] **Step 9: Commit Task 1**

```powershell
git add pipeline/bigquery_contract.py scripts/bootstrap_classification.py tests/pipeline/test_bigquery_contract.py tests/scripts/test_bootstrap_classification.py
git commit -m "feat: add classification table contracts"
```

---

### Task 2: Implement Base Validation and Canonicalization in Pandas

**Files:**
- Create: `pipeline/transaction_classification.py`
- Create: `tests/pipeline/test_transaction_classification.py`

**Interfaces:**
- Consumes raw transaction columns exactly: `txn`, `dtts`, `amount`, `currency`, `region`, `source_file`, `source_checksum`, `source_row_number`, `source_row_id`, `batch_date`, `ingested_at`.
- Produces `validate_and_canonicalize_transactions(raw_df: pd.DataFrame, batch_date: date) -> pd.DataFrame`.
- Returned frame retains all raw columns and adds internal columns:
  - `_txn_canonical`
  - `_transaction_dt`
  - `_amount_numeric`
  - `_currency_canonical`
  - `_base_reason_codes`
- Produces constants:
  - `BASE_REASON_ORDER`
  - `RAW_TRANSACTION_COLUMNS`
  - `VALID_CURRENCIES`
  - `VALID_REGIONS`

- [ ] **Step 1: Write failing tests for canonical valid values and typed output internals**

Create `tests/pipeline/test_transaction_classification.py` with a reusable raw-row helper:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd

from pipeline.transaction_classification import validate_and_canonicalize_transactions


def raw_row(**overrides):
    row = {
        "txn": " TX-1 ",
        "dtts": "2025-07-22 09:30:00",
        "amount": "100.50",
        "currency": " usd ",
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": 1,
        "source_row_id": "row-1",
        "batch_date": date(2025, 7, 22),
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def test_base_validation_canonicalizes_valid_values_without_inventing_txn_case():
    frame = pd.DataFrame([raw_row()])

    result = validate_and_canonicalize_transactions(frame, date(2025, 7, 22))

    row = result.iloc[0]
    assert row._txn_canonical == "TX-1"
    assert row._transaction_dt == pd.Timestamp("2025-07-22 09:30:00")
    assert row._amount_numeric == Decimal("100.50")
    assert row._currency_canonical == "USD"
    assert row._base_reason_codes == []
```

- [ ] **Step 2: Run the test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_base_validation_canonicalizes_valid_values_without_inventing_txn_case -v
```

Expected: import/collection failure because `pipeline.transaction_classification` does not exist.

- [ ] **Step 3: Add the module, constants, raw-column guard, and canonicalization helpers**

Create `pipeline/transaction_classification.py` with these exact public constants and helper semantics:

```python
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

RAW_TRANSACTION_COLUMNS = (
    "txn",
    "dtts",
    "amount",
    "currency",
    "region",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "batch_date",
    "ingested_at",
)
BASE_REASON_ORDER = (
    "MISSING_TXN",
    "INVALID_DTTS",
    "DTTS_BATCH_DATE_MISMATCH",
    "INVALID_AMOUNT",
    "NEGATIVE_AMOUNT",
    "INVALID_CURRENCY",
    "INVALID_REGION",
)
VALID_CURRENCIES = frozenset({"THB", "USD", "EUR"})
VALID_REGIONS = frozenset({"bkk", "central", "north", "northeast", "south"})


class TransactionClassificationError(RuntimeError):
    pass


def _require_raw_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in RAW_TRANSACTION_COLUMNS if column not in frame.columns]
    if missing:
        raise TransactionClassificationError(f"Missing raw transaction columns: {missing!r}")


def _parse_bigquery_numeric(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None

    _, digits, exponent = parsed.as_tuple()
    scale = max(-exponent, 0)
    integer_digits = max(len(digits) + exponent, 0) if exponent >= 0 else max(len(digits) - scale, 0)
    if scale > 9 or integer_digits > 29 or integer_digits + scale > 38:
        return None
    return parsed
```

Then implement `validate_and_canonicalize_transactions()` using Pandas string/datetime operations plus deterministic reason collection:

```python
def validate_and_canonicalize_transactions(raw_df: pd.DataFrame, batch_date: date) -> pd.DataFrame:
    _require_raw_columns(raw_df)
    frame = raw_df.loc[:, RAW_TRANSACTION_COLUMNS].copy()

    txn_text = frame["txn"].fillna("").astype(str).str.strip()
    dtts_text = frame["dtts"].fillna("").astype(str).str.strip()
    amount_text = frame["amount"].fillna("").astype(str).str.strip()
    currency_text = frame["currency"].fillna("").astype(str).str.strip().str.upper()

    frame["_txn_canonical"] = txn_text
    frame["_transaction_dt"] = pd.to_datetime(
        dtts_text,
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    frame["_amount_numeric"] = amount_text.map(_parse_bigquery_numeric)
    frame["_currency_canonical"] = currency_text

    missing_txn = frame["txn"].isna() | txn_text.eq("")
    invalid_dtts = frame["_transaction_dt"].isna()
    dtts_mismatch = (~invalid_dtts) & frame["_transaction_dt"].dt.date.ne(batch_date)
    invalid_amount = frame["_amount_numeric"].isna()
    negative_amount = frame["_amount_numeric"].map(
        lambda value: value is not None and value < Decimal("0")
    )
    invalid_currency = ~currency_text.isin(VALID_CURRENCIES)
    invalid_region = ~frame["region"].isin(VALID_REGIONS)

    masks = (
        ("MISSING_TXN", missing_txn),
        ("INVALID_DTTS", invalid_dtts),
        ("DTTS_BATCH_DATE_MISMATCH", dtts_mismatch),
        ("INVALID_AMOUNT", invalid_amount),
        ("NEGATIVE_AMOUNT", negative_amount),
        ("INVALID_CURRENCY", invalid_currency),
        ("INVALID_REGION", invalid_region),
    )
    frame["_base_reason_codes"] = [
        [reason for reason, mask in masks if bool(mask.loc[index])]
        for index in frame.index
    ]
    return frame
```

- [ ] **Step 4: Run the canonical valid-value test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_base_validation_canonicalizes_valid_values_without_inventing_txn_case -v
```

Expected: PASS.

- [ ] **Step 5: Add failing tests for every base reason and deterministic multi-reason order**

Add tests covering:

```python
def test_zero_amount_is_valid_and_negative_amount_is_not():
    frame = pd.DataFrame([
        raw_row(source_row_id="zero", amount="0"),
        raw_row(source_row_id="negative", amount="-0.01"),
    ])
    result = validate_and_canonicalize_transactions(frame, date(2025, 7, 22))
    by_id = result.set_index("source_row_id")
    assert by_id.loc["zero", "_base_reason_codes"] == []
    assert by_id.loc["negative", "_base_reason_codes"] == ["NEGATIVE_AMOUNT"]


def test_invalid_amount_includes_blank_text_non_numeric_and_bigquery_numeric_overflow():
    frame = pd.DataFrame([
        raw_row(source_row_id="blank", amount=" "),
        raw_row(source_row_id="text", amount="N/A"),
        raw_row(source_row_id="scale", amount="0.1234567891"),
        raw_row(source_row_id="width", amount="100000000000000000000000000000"),
    ])
    result = validate_and_canonicalize_transactions(frame, date(2025, 7, 22))
    assert result["_base_reason_codes"].tolist() == [
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
    ]


def test_datetime_and_batch_date_rules_are_independent_and_ordered():
    frame = pd.DataFrame([
        raw_row(source_row_id="bad", dtts="not-a-date"),
        raw_row(source_row_id="wrong-day", dtts="2025-07-23 00:00:00"),
    ])
    result = validate_and_canonicalize_transactions(frame, date(2025, 7, 22))
    assert result["_base_reason_codes"].tolist() == [
        ["INVALID_DTTS"],
        ["DTTS_BATCH_DATE_MISMATCH"],
    ]


def test_currency_region_and_txn_failures_collect_multiple_reasons_in_fixed_order():
    frame = pd.DataFrame([
        raw_row(txn=" ", currency="gbp", region="unknown", amount="N/A")
    ])
    result = validate_and_canonicalize_transactions(frame, date(2025, 7, 22))
    assert result.iloc[0]._base_reason_codes == [
        "MISSING_TXN",
        "INVALID_AMOUNT",
        "INVALID_CURRENCY",
        "INVALID_REGION",
    ]
```

Also add a test that a missing required DataFrame column raises `TransactionClassificationError` with the missing column name.

- [ ] **Step 6: Run all base-validation tests and fix only defects revealed by those tests**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py -v
```

Expected at this Task boundary: all currently written base-validation tests pass.

- [ ] **Step 7: Commit Task 2**

```powershell
git add pipeline/transaction_classification.py tests/pipeline/test_transaction_classification.py
git commit -m "feat: validate and canonicalize transactions"
```

---

### Task 3: Add Deterministic Duplicate Classification and Final Accepted/Quarantine Projections

**Files:**
- Modify: `pipeline/transaction_classification.py`
- Modify: `tests/pipeline/test_transaction_classification.py`

**Interfaces:**
- Consumes the validated frame returned by `validate_and_canonicalize_transactions()`.
- Produces:

```python
@dataclass(frozen=True)
class ClassificationResult:
    accepted: pd.DataFrame
    quarantine: pd.DataFrame
```

- Produces `classify_duplicates(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`, where the first frame contains deterministic accepted base-valid rows and the second contains duplicate-quarantine base-valid rows with `_duplicate_reason`.
- Produces `classify_transactions(raw_df: pd.DataFrame, batch_date: date, classified_at: datetime) -> ClassificationResult`.

- [ ] **Step 1: Write failing exact-replay tests**

Append to `tests/pipeline/test_transaction_classification.py`:

```python
from pipeline.transaction_classification import classify_transactions


def test_exact_replay_accepts_lowest_source_lineage_and_quarantines_losers():
    frame = pd.DataFrame([
        raw_row(
            source_row_id="later",
            source_file="transactions/business_date=2025-07-22/sales_north_20250722.csv.gz",
            source_row_number=8,
            txn=" TX-R ",
        ),
        raw_row(
            source_row_id="winner",
            source_file="transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
            source_row_number=10,
            txn="TX-R",
        ),
        raw_row(
            source_row_id="same-file-later-row",
            source_file="transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
            source_row_number=11,
            txn="TX-R",
        ),
    ])
    classified_at = datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc)

    result = classify_transactions(frame, date(2025, 7, 22), classified_at)

    assert result.accepted["source_row_id"].tolist() == ["winner"]
    assert set(result.quarantine["source_row_id"]) == {"later", "same-file-later-row"}
    assert result.quarantine["reason_codes"].tolist() == [
        ["DUPLICATE_REPLAY"],
        ["DUPLICATE_REPLAY"],
    ]
```

Sort the asserted quarantine frame by `source_row_id` before comparing reason rows if test ordering would otherwise be incidental; do not make quarantine output ordering part of the business contract.

- [ ] **Step 2: Run the replay test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_exact_replay_accepts_lowest_source_lineage_and_quarantines_losers -v
```

Expected: import failure for `classify_transactions`.

- [ ] **Step 3: Add result type, duplicate classification, and projections**

Add to `pipeline/transaction_classification.py`:

```python
from dataclasses import dataclass

ACCEPTED_COLUMNS = (
    "txn",
    "transaction_dt",
    "amount",
    "currency",
    "region",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "batch_date",
    "ingested_at",
    "classified_at",
)
QUARANTINE_COLUMNS = (
    "txn",
    "dtts",
    "amount",
    "currency",
    "region",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "batch_date",
    "ingested_at",
    "reason_codes",
    "quarantined_at",
)


@dataclass(frozen=True)
class ClassificationResult:
    accepted: pd.DataFrame
    quarantine: pd.DataFrame
```

Implement duplicate classification without relying on incoming DataFrame order:

```python
def classify_duplicates(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    accepted_parts = []
    quarantine_parts = []

    for _, group in valid_df.groupby("_txn_canonical", sort=False, dropna=False):
        payloads = {
            (
                row._transaction_dt,
                row._amount_numeric,
                row._currency_canonical,
                row.region,
            )
            for row in group.itertuples()
        }
        ordered = group.sort_values(
            ["source_file", "source_row_number"],
            kind="stable",
        )
        if len(payloads) > 1:
            rejected = ordered.copy()
            rejected["_duplicate_reason"] = "DUPLICATE_CONFLICT"
            quarantine_parts.append(rejected)
            continue

        accepted_parts.append(ordered.iloc[[0]].copy())
        if len(ordered) > 1:
            rejected = ordered.iloc[1:].copy()
            rejected["_duplicate_reason"] = "DUPLICATE_REPLAY"
            quarantine_parts.append(rejected)

    accepted = pd.concat(accepted_parts, ignore_index=True) if accepted_parts else valid_df.iloc[0:0].copy()
    duplicate_quarantine = (
        pd.concat(quarantine_parts, ignore_index=True)
        if quarantine_parts
        else valid_df.iloc[0:0].assign(_duplicate_reason=pd.Series(dtype="object"))
    )
    return accepted, duplicate_quarantine
```

Add projection helpers and `classify_transactions()` so base-invalid rows never enter duplicate classification:

```python
def classify_transactions(
    raw_df: pd.DataFrame,
    batch_date: date,
    classified_at: datetime,
) -> ClassificationResult:
    validated = validate_and_canonicalize_transactions(raw_df, batch_date)
    base_invalid_mask = validated["_base_reason_codes"].map(bool)
    base_invalid = validated.loc[base_invalid_mask].copy()
    base_valid = validated.loc[~base_invalid_mask].copy()

    accepted_valid, duplicate_quarantine = classify_duplicates(base_valid)

    accepted = pd.DataFrame({
        "txn": accepted_valid["_txn_canonical"],
        "transaction_dt": accepted_valid["_transaction_dt"],
        "amount": accepted_valid["_amount_numeric"],
        "currency": accepted_valid["_currency_canonical"],
        "region": accepted_valid["region"],
        "source_file": accepted_valid["source_file"],
        "source_checksum": accepted_valid["source_checksum"],
        "source_row_number": accepted_valid["source_row_number"],
        "source_row_id": accepted_valid["source_row_id"],
        "batch_date": accepted_valid["batch_date"],
        "ingested_at": accepted_valid["ingested_at"],
        "classified_at": classified_at,
    }, columns=ACCEPTED_COLUMNS)

    base_quarantine = base_invalid.loc[:, RAW_TRANSACTION_COLUMNS].copy()
    base_quarantine["reason_codes"] = base_invalid["_base_reason_codes"].tolist()
    base_quarantine["quarantined_at"] = classified_at

    duplicate_output = duplicate_quarantine.loc[:, RAW_TRANSACTION_COLUMNS].copy()
    duplicate_output["reason_codes"] = duplicate_quarantine["_duplicate_reason"].map(lambda reason: [reason])
    duplicate_output["quarantined_at"] = classified_at

    quarantine = pd.concat([base_quarantine, duplicate_output], ignore_index=True)
    quarantine = quarantine.loc[:, QUARANTINE_COLUMNS]

    if len(raw_df) != len(accepted) + len(quarantine):
        raise TransactionClassificationError(
            "Classification reconciliation failed: "
            f"raw={len(raw_df)} accepted={len(accepted)} quarantine={len(quarantine)}"
        )
    return ClassificationResult(accepted=accepted, quarantine=quarantine)
```

- [ ] **Step 4: Run the replay test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_exact_replay_accepts_lowest_source_lineage_and_quarantines_losers -v
```

Expected: PASS.

- [ ] **Step 5: Write failing conflict/base-invalid isolation/final-shape tests**

Add tests with these exact semantics:

```python
def test_conflicting_canonical_payloads_quarantine_every_base_valid_occurrence():
    frame = pd.DataFrame([
        raw_row(source_row_id="a", txn="TX-C", amount="100.00"),
        raw_row(source_row_id="b", txn="TX-C", amount="101.00"),
    ])
    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )
    assert result.accepted.empty
    assert set(result.quarantine["source_row_id"]) == {"a", "b"}
    assert result.quarantine["reason_codes"].tolist() == [
        ["DUPLICATE_CONFLICT"],
        ["DUPLICATE_CONFLICT"],
    ]


def test_base_invalid_replay_does_not_poison_valid_row_with_same_txn():
    frame = pd.DataFrame([
        raw_row(source_row_id="valid", txn="TX-X", amount="100"),
        raw_row(source_row_id="invalid", txn="TX-X", amount="N/A"),
    ])
    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )
    assert result.accepted["source_row_id"].tolist() == ["valid"]
    invalid = result.quarantine.set_index("source_row_id").loc["invalid"]
    assert invalid.reason_codes == ["INVALID_AMOUNT"]


def test_quarantine_preserves_raw_business_fields_and_accepted_is_typed():
    frame = pd.DataFrame([
        raw_row(source_row_id="ok", txn=" TX-OK ", amount="100.50", currency=" usd "),
        raw_row(source_row_id="bad", txn=" raw-value ", amount="N/A", currency=" gbp "),
    ])
    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )
    accepted = result.accepted.iloc[0]
    assert accepted.txn == "TX-OK"
    assert accepted.amount == Decimal("100.50")
    assert accepted.currency == "USD"
    assert isinstance(accepted.transaction_dt, pd.Timestamp)

    quarantined = result.quarantine.iloc[0]
    assert quarantined.txn == " raw-value "
    assert quarantined.amount == "N/A"
    assert quarantined.currency == " gbp "
    assert quarantined.reason_codes == ["INVALID_AMOUNT", "INVALID_CURRENCY"]


def test_every_raw_row_has_exactly_one_classification_outcome():
    frame = pd.DataFrame([
        raw_row(source_row_id="unique", txn="TX-U"),
        raw_row(source_row_id="replay-a", txn="TX-R"),
        raw_row(source_row_id="replay-b", txn="TX-R", source_row_number=2),
        raw_row(source_row_id="invalid", txn="TX-I", amount="N/A"),
    ])
    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )
    output_ids = set(result.accepted.source_row_id) | set(result.quarantine.source_row_id)
    assert len(result.accepted) + len(result.quarantine) == len(frame)
    assert output_ids == set(frame.source_row_id)
```

- [ ] **Step 6: Run the full pure-Pandas classifier suite**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py -v
```

Expected: all classifier tests pass.

- [ ] **Step 7: Commit Task 3**

```powershell
git add pipeline/transaction_classification.py tests/pipeline/test_transaction_classification.py
git commit -m "feat: classify transaction data quality"
```

---

### Task 4: Add a Focused BigQuery Partition-Row Read Helper

**Files:**
- Modify: `pipeline/bigquery_adapter.py`
- Modify: `tests/pipeline/test_bigquery_adapter.py`

**Interfaces:**
- Consumes existing `_partition_job_config(partition_date)`.
- Produces:

```python
BigQueryAdapter.query_partition_rows(
    dataset_id: str,
    table_id: str,
    partition_field: str,
    partition_date: date,
    columns: tuple[str, ...],
) -> list[dict]
```

- [ ] **Step 1: Write the failing partition-row read test**

Extend the existing `FakeClient` usage in `tests/pipeline/test_bigquery_adapter.py`:

```python
class FakeMappingRow:
    def __init__(self, values):
        self._values = values

    def items(self):
        return self._values.items()


def test_query_partition_rows_selects_requested_columns_and_returns_dicts():
    client = FakeClient()
    client.query_rows = [
        FakeMappingRow({"txn": "TX-1", "source_row_id": "row-1"}),
        FakeMappingRow({"txn": "TX-2", "source_row_id": "row-2"}),
    ]
    adapter = BigQueryAdapter("proj", client=client)

    rows = adapter.query_partition_rows(
        "bahtflow_raw",
        "transactions",
        "batch_date",
        date(2025, 7, 22),
        ("txn", "source_row_id"),
    )

    assert rows == [
        {"txn": "TX-1", "source_row_id": "row-1"},
        {"txn": "TX-2", "source_row_id": "row-2"},
    ]
    sql, job_config = client.query_calls[-1]
    assert "SELECT txn, source_row_id" in sql
    assert "`proj.bahtflow_raw.transactions`" in sql
    assert "WHERE batch_date = @partition_date" in sql
    assert job_config.query_parameters[0].value == date(2025, 7, 22)
```

- [ ] **Step 2: Run the helper test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py::test_query_partition_rows_selects_requested_columns_and_returns_dicts -v
```

Expected: `AttributeError` because `query_partition_rows` does not exist.

- [ ] **Step 3: Implement the narrow adapter method**

Add to `BigQueryAdapter`:

```python
def query_partition_rows(
    self,
    dataset_id: str,
    table_id: str,
    partition_field: str,
    partition_date: date,
    columns: tuple[str, ...],
) -> list[dict]:
    if not columns:
        raise ValueError("columns must not be empty")
    full_id = f"{self._project_id}.{dataset_id}.{table_id}"
    select_list = ", ".join(columns)
    sql = (
        f"SELECT {select_list} FROM `{full_id}` "
        f"WHERE {partition_field} = @partition_date"
    )
    rows = self._client.query(
        sql,
        job_config=self._partition_job_config(partition_date),
    ).result()
    return [dict(row.items()) for row in rows]
```

Do not add a generic query builder or repository class.

- [ ] **Step 4: Run adapter + contract tests and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_bigquery_contract.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 4**

```powershell
git add pipeline/bigquery_adapter.py tests/pipeline/test_bigquery_adapter.py
git commit -m "feat: read partition rows from bigquery"
```

---

### Task 5: Orchestrate One-Batch Classification, Idempotent Persistence, and Retry Recovery

**Files:**
- Create: `pipeline/classification_load.py`
- Create: `tests/pipeline/test_classification_load.py`

**Interfaces:**
- Consumes `BigQueryAdapter.query_partition_rows`, `query_source_row_ids`, `append_rows`, and `query_partition_row_count`.
- Consumes `classify_transactions(raw_df, batch_date, classified_at) -> ClassificationResult`.
- Reuses `pipeline.pandas_intake.anti_filter_existing(frame, existing_ids) -> pd.DataFrame`.
- Produces:

```python
@dataclass(frozen=True)
class ClassificationLoadSummary:
    batch_date: str
    raw_rows: int
    accepted_rows: int
    quarantine_rows: int
    accepted_inserted_rows: int
    quarantine_inserted_rows: int
    accepted_partition_rows: int
    quarantine_partition_rows: int
    reconciled: bool
```

- Produces `classify_and_load_batch(*, batch_date: date, bigquery_adapter, classified_at: datetime | None = None) -> ClassificationLoadSummary`.

- [ ] **Step 1: Write a stateful fake and failing first-run/rerun test**

Create `tests/pipeline/test_classification_load.py`. The fake must model the exact boundary methods, not BigQuery internals:

```python
from datetime import date, datetime, timezone

import pytest

from pipeline.classification_load import classify_and_load_batch


class StatefulBigQueryFake:
    def __init__(self, raw_rows):
        self.raw_rows = list(raw_rows)
        self.outputs = {
            ("bahtflow_analytics", "transactions_accepted"): [],
            ("bahtflow_ops", "transactions_quarantine"): [],
        }
        self.fail_next_quarantine_append = False

    def query_partition_rows(self, dataset_id, table_id, partition_field, partition_date, columns):
        assert (dataset_id, table_id) == ("bahtflow_raw", "transactions")
        return [{column: row[column] for column in columns} for row in self.raw_rows]

    def query_source_row_ids(self, dataset_id, table_id, partition_field, partition_date):
        return {
            row["source_row_id"]
            for row in self.outputs[(dataset_id, table_id)]
            if row["batch_date"] == partition_date.isoformat()
            or row["batch_date"] == partition_date
        }

    def append_rows(self, dataset_id, table_id, rows, schema):
        if (
            self.fail_next_quarantine_append
            and (dataset_id, table_id) == ("bahtflow_ops", "transactions_quarantine")
        ):
            self.fail_next_quarantine_append = False
            raise RuntimeError("simulated quarantine write failure")
        self.outputs[(dataset_id, table_id)].extend(rows)
        return len(rows)

    def query_partition_row_count(self, dataset_id, table_id, partition_field, partition_date):
        return sum(
            1
            for row in self.outputs[(dataset_id, table_id)]
            if row["batch_date"] == partition_date.isoformat()
            or row["batch_date"] == partition_date
        )
```

Use four raw rows: one unique valid transaction, two exact replay rows for one txn, and one invalid amount. First classification must produce two accepted and two quarantine rows; unchanged rerun must insert zero:

```python
def test_first_run_classifies_and_rerun_inserts_zero(raw_rows_fixture):
    fake = StatefulBigQueryFake(raw_rows_fixture)
    when = datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc)

    first = classify_and_load_batch(
        batch_date=date(2025, 7, 22),
        bigquery_adapter=fake,
        classified_at=when,
    )
    second = classify_and_load_batch(
        batch_date=date(2025, 7, 22),
        bigquery_adapter=fake,
        classified_at=when,
    )

    assert first.raw_rows == 4
    assert first.accepted_rows == 2
    assert first.quarantine_rows == 2
    assert first.accepted_inserted_rows == 2
    assert first.quarantine_inserted_rows == 2
    assert first.accepted_partition_rows == 2
    assert first.quarantine_partition_rows == 2
    assert first.reconciled is True

    assert second.accepted_inserted_rows == 0
    assert second.quarantine_inserted_rows == 0
    assert second.accepted_partition_rows == 2
    assert second.quarantine_partition_rows == 2
    assert second.reconciled is True
```

- [ ] **Step 2: Run the orchestration test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_classification_load.py::test_first_run_classifies_and_rerun_inserts_zero -v
```

Expected: import failure because `pipeline.classification_load` does not exist.

- [ ] **Step 3: Implement JSON-safe serialization and the minimal orchestration**

Create `pipeline/classification_load.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd

from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
)
from pipeline.pandas_intake import anti_filter_existing
from pipeline.transaction_classification import (
    RAW_TRANSACTION_COLUMNS,
    classify_transactions,
)


class ClassificationLoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClassificationLoadSummary:
    batch_date: str
    raw_rows: int
    accepted_rows: int
    quarantine_rows: int
    accepted_inserted_rows: int
    quarantine_inserted_rows: int
    accepted_partition_rows: int
    quarantine_partition_rows: int
    reconciled: bool


def _json_safe_value(value):
    if isinstance(value, list):
        return list(value)
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return value


def _frame_to_records(frame: pd.DataFrame) -> list[dict]:
    return [
        {key: _json_safe_value(value) for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
```

Then implement `classify_and_load_batch()`:

```python
def classify_and_load_batch(
    *,
    batch_date: date,
    bigquery_adapter,
    classified_at: datetime | None = None,
) -> ClassificationLoadSummary:
    invocation_time = classified_at or datetime.now(timezone.utc)
    raw_rows = bigquery_adapter.query_partition_rows(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_PARTITION_FIELD,
        batch_date,
        RAW_TRANSACTION_COLUMNS,
    )
    raw_frame = pd.DataFrame(raw_rows, columns=RAW_TRANSACTION_COLUMNS)
    result = classify_transactions(raw_frame, batch_date, invocation_time)

    if len(raw_frame) != len(result.accepted) + len(result.quarantine):
        raise ClassificationLoadError("In-memory classification reconciliation failed")

    accepted_existing = bigquery_adapter.query_source_row_ids(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    accepted_new = anti_filter_existing(result.accepted, accepted_existing)
    accepted_inserted = bigquery_adapter.append_rows(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        _frame_to_records(accepted_new),
        ACCEPTED_TRANSACTIONS_SCHEMA,
    )

    quarantine_existing = bigquery_adapter.query_source_row_ids(
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    quarantine_new = anti_filter_existing(result.quarantine, quarantine_existing)
    quarantine_inserted = bigquery_adapter.append_rows(
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        _frame_to_records(quarantine_new),
        QUARANTINE_TRANSACTIONS_SCHEMA,
    )

    accepted_partition_rows = bigquery_adapter.query_partition_row_count(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    quarantine_partition_rows = bigquery_adapter.query_partition_row_count(
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    reconciled = len(raw_frame) == accepted_partition_rows + quarantine_partition_rows
    if not reconciled:
        raise ClassificationLoadError(
            "Persisted classification reconciliation failed: "
            f"raw={len(raw_frame)} accepted={accepted_partition_rows} "
            f"quarantine={quarantine_partition_rows}"
        )

    return ClassificationLoadSummary(
        batch_date=batch_date.isoformat(),
        raw_rows=len(raw_frame),
        accepted_rows=len(result.accepted),
        quarantine_rows=len(result.quarantine),
        accepted_inserted_rows=accepted_inserted,
        quarantine_inserted_rows=quarantine_inserted,
        accepted_partition_rows=accepted_partition_rows,
        quarantine_partition_rows=quarantine_partition_rows,
        reconciled=True,
    )
```

- [ ] **Step 4: Run the first-run/rerun test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_classification_load.py::test_first_run_classifies_and_rerun_inserts_zero -v
```

Expected: PASS.

- [ ] **Step 5: Add a failing partial-retry test**

```python
def test_partial_retry_skips_persisted_accepted_and_writes_missing_quarantine(raw_rows_fixture):
    fake = StatefulBigQueryFake(raw_rows_fixture)
    fake.fail_next_quarantine_append = True
    first_time = datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="simulated quarantine write failure"):
        classify_and_load_batch(
            batch_date=date(2025, 7, 22),
            bigquery_adapter=fake,
            classified_at=first_time,
        )

    assert len(fake.outputs[("bahtflow_analytics", "transactions_accepted")]) == 2
    assert len(fake.outputs[("bahtflow_ops", "transactions_quarantine")]) == 0

    retry = classify_and_load_batch(
        batch_date=date(2025, 7, 22),
        bigquery_adapter=fake,
        classified_at=datetime(2026, 9, 2, 8, 5, tzinfo=timezone.utc),
    )

    assert retry.accepted_inserted_rows == 0
    assert retry.quarantine_inserted_rows == 2
    assert retry.reconciled is True
```

- [ ] **Step 6: Run the partial-retry test and observe GREEN using the existing implementation**

Run:

```powershell
pytest tests/pipeline/test_classification_load.py::test_partial_retry_skips_persisted_accepted_and_writes_missing_quarantine -v
```

Expected: PASS. If it fails, fix only the idempotency/retry defect demonstrated by the test; do not add rollback infrastructure.

- [ ] **Step 7: Add a persisted reconciliation failure test**

Create a fake subclass or override `query_partition_row_count` so the final counts deliberately return a mismatch, then assert:

```python
with pytest.raises(ClassificationLoadError, match="Persisted classification reconciliation failed"):
    classify_and_load_batch(...)
```

- [ ] **Step 8: Run all F05 pipeline tests**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py tests/pipeline/test_classification_load.py tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_bigquery_contract.py -v
```

Expected: all tests pass.

- [ ] **Step 9: Commit Task 5**

```powershell
git add pipeline/classification_load.py tests/pipeline/test_classification_load.py
git commit -m "feat: persist classified transaction batches"
```

---

### Task 6: Add the Classification CLI

**Files:**
- Create: `scripts/classify_transactions.py`
- Create: `tests/scripts/test_classify_transactions.py`

**Interfaces:**
- Consumes `load_gcp_settings()`, `BigQueryAdapter`, and `classify_and_load_batch()`.
- Produces CLI:

```text
python -m scripts.classify_transactions --batch-date YYYY-MM-DD
```

- Prints exactly these summary keys, one per line in this order:
  - `batch_date`
  - `raw_rows`
  - `accepted_rows`
  - `quarantine_rows`
  - `accepted_inserted_rows`
  - `quarantine_inserted_rows`
  - `accepted_partition_rows`
  - `quarantine_partition_rows`
  - `reconciled`

- [ ] **Step 1: Write the failing CLI test**

Create `tests/scripts/test_classify_transactions.py` using monkeypatch for settings, adapter, and orchestration. The test should call `main(["--batch-date", "2025-07-22"])` rather than spawning a subprocess:

```python
from pipeline.classification_load import ClassificationLoadSummary
from scripts import classify_transactions as cli


def test_cli_prints_classification_summary(monkeypatch, capsys):
    summary = ClassificationLoadSummary(
        batch_date="2025-07-22",
        raw_rows=4,
        accepted_rows=2,
        quarantine_rows=2,
        accepted_inserted_rows=2,
        quarantine_inserted_rows=2,
        accepted_partition_rows=2,
        quarantine_partition_rows=2,
        reconciled=True,
    )
    monkeypatch.setattr(cli, "run_classification", lambda batch_date: summary)

    cli.main(["--batch-date", "2025-07-22"])

    assert capsys.readouterr().out.splitlines() == [
        "batch_date=2025-07-22",
        "raw_rows=4",
        "accepted_rows=2",
        "quarantine_rows=2",
        "accepted_inserted_rows=2",
        "quarantine_inserted_rows=2",
        "accepted_partition_rows=2",
        "quarantine_partition_rows=2",
        "reconciled=True",
    ]
```

- [ ] **Step 2: Run the CLI test and observe RED**

Run:

```powershell
pytest tests/scripts/test_classify_transactions.py -v
```

Expected: module import failure.

- [ ] **Step 3: Implement a dependency-light CLI**

Create `scripts/classify_transactions.py`:

```python
from __future__ import annotations

import argparse
from datetime import date

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.classification_load import classify_and_load_batch
from pipeline.config import load_gcp_settings


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Classify one raw BahtFlow transaction batch")
    parser.add_argument("--batch-date", required=True, type=date.fromisoformat)
    return parser.parse_args(argv)


def run_classification(batch_date: date):
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)
    return classify_and_load_batch(
        batch_date=batch_date,
        bigquery_adapter=adapter,
    )


def main(argv=None) -> None:
    args = parse_args(argv)
    summary = run_classification(args.batch_date)
    for field in (
        "batch_date",
        "raw_rows",
        "accepted_rows",
        "quarantine_rows",
        "accepted_inserted_rows",
        "quarantine_inserted_rows",
        "accepted_partition_rows",
        "quarantine_partition_rows",
        "reconciled",
    ):
        print(f"{field}={getattr(summary, field)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run CLI + focused F05 tests and observe GREEN**

Run:

```powershell
pytest tests/scripts/test_classify_transactions.py tests/scripts/test_bootstrap_classification.py tests/pipeline/test_transaction_classification.py tests/pipeline/test_classification_load.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 6**

```powershell
git add scripts/classify_transactions.py tests/scripts/test_classify_transactions.py
git commit -m "feat: add transaction classification CLI"
```

---

### Task 7: Live BigQuery Acceptance, Rerun Evidence, and README Runbook

**Files:**
- Modify: `README.md`

**Interfaces:**
- Uses the already loaded F04 raw partition `2025-07-22`.
- Uses `scripts.bootstrap_classification` and `scripts.classify_transactions` through the `gcp-toolbox` Compose profile.
- No source or output table is deleted automatically.

- [ ] **Step 1: Pull the feature branch locally and run the complete local gate before cloud writes**

```powershell
git switch feat/05-pandas-dq-classification
git pull --ff-only origin feat/05-pandas-dq-classification

pytest

python -m py_compile `
  pipeline/transaction_classification.py `
  pipeline/classification_load.py `
  pipeline/bigquery_contract.py `
  pipeline/bigquery_adapter.py `
  scripts/bootstrap_classification.py `
  scripts/classify_transactions.py

docker compose config --quiet
git diff --check
git status --short
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
```

Required before live writes: `pytest` has zero failures; compile/config/diff/credential checks are quiet; working tree is clean.

- [ ] **Step 2: Build the GCP toolbox with the already pinned dependencies**

```powershell
docker compose --profile gcp build gcp-toolbox
```

Expected: build exits successfully. F05 adds no new Python dependency beyond the existing Pandas/BigQuery requirements.

- [ ] **Step 3: Bootstrap the two F05 output tables twice**

First run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification
```

Expected on a fresh F05 setup:

```text
table=bahtflow_analytics.transactions_accepted status=created
table=bahtflow_ops.transactions_quarantine status=created
```

Second run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification
```

Expected:

```text
table=bahtflow_analytics.transactions_accepted status=verified
table=bahtflow_ops.transactions_quarantine status=verified
```

If the first run reports `verified`, do not delete data automatically; inspect the target partition before using it as fresh acceptance evidence.

- [ ] **Step 4: Confirm the raw source partition exists and record output pre-state**

Run a one-off query through the toolbox:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from datetime import date; from pipeline.config import load_gcp_settings; from pipeline.bigquery_adapter import BigQueryAdapter; s=load_gcp_settings(); a=BigQueryAdapter(s.project_id); d=date(2025,7,22); print('raw_before=',a.query_partition_row_count('bahtflow_raw','transactions','batch_date',d)); print('accepted_before=',a.query_partition_row_count('bahtflow_analytics','transactions_accepted','batch_date',d)); print('quarantine_before=',a.query_partition_row_count('bahtflow_ops','transactions_quarantine','batch_date',d))"
```

Required for fresh first-run evidence:

```text
raw_before=8978
accepted_before=0
quarantine_before=0
```

If either output count is nonzero, stop and decide explicitly how to establish a fresh acceptance partition; do not silently delete or overwrite existing classified data.

- [ ] **Step 5: Run the first live classification**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Record the exact output. Required relationships:

```text
batch_date=2025-07-22
raw_rows=8978
accepted_rows=<measured>
quarantine_rows=<measured>
accepted_inserted_rows=<same as measured accepted_rows>
quarantine_inserted_rows=<same as measured quarantine_rows>
accepted_partition_rows=<same as measured accepted_rows>
quarantine_partition_rows=<same as measured quarantine_rows>
reconciled=True
```

Do not guess accepted/quarantine counts in advance. Verify numerically that:

```text
8978 = accepted_rows + quarantine_rows
```

- [ ] **Step 6: Run the same classification command again for idempotency evidence**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Required rerun evidence:

```text
raw_rows=8978
accepted_inserted_rows=0
quarantine_inserted_rows=0
accepted_partition_rows=<unchanged first-run accepted count>
quarantine_partition_rows=<unchanged first-run quarantine count>
reconciled=True
```

- [ ] **Step 7: Prove typed accepted values and populated quarantine reasons from BigQuery**

Run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from pipeline.config import load_gcp_settings; from google.cloud import bigquery; s=load_gcp_settings(); c=bigquery.Client(project=s.project_id); a=list(c.query('SELECT txn, transaction_dt, amount, currency, region, source_row_id FROM `'+s.project_id+'.bahtflow_analytics.transactions_accepted` WHERE batch_date=DATE(\"2025-07-22\") ORDER BY source_file, source_row_number LIMIT 5').result()); q=list(c.query('SELECT txn, dtts, amount, currency, reason_codes, source_row_id FROM `'+s.project_id+'.bahtflow_ops.transactions_quarantine` WHERE batch_date=DATE(\"2025-07-22\") AND ARRAY_LENGTH(reason_codes)>0 ORDER BY source_file, source_row_number LIMIT 5').result()); print('accepted_sample_rows=',len(a)); print('accepted_sample=',[tuple(r) for r in a]); print('quarantine_sample_rows=',len(q)); print('quarantine_sample=',[tuple(r) for r in q])"
```

Required evidence:
- accepted sample has timezone-free datetime values returned from the BigQuery `DATETIME` column;
- accepted amounts are numeric values, not raw malformed strings;
- accepted currency values are only `THB`, `USD`, or `EUR`;
- quarantine sample retains raw values and every sampled `reason_codes` array is non-empty.

- [ ] **Step 8: Prove duplicate semantics with warehouse queries**

Run one query for any accepted duplicate winner candidates and one query for conflicts/replays represented in quarantine. Use the actual reason codes rather than assuming the fixture contains a specific txn ID:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from pipeline.config import load_gcp_settings; from google.cloud import bigquery; s=load_gcp_settings(); c=bigquery.Client(project=s.project_id); q='SELECT reason, COUNT(*) FROM `'+s.project_id+'.bahtflow_ops.transactions_quarantine`, UNNEST(reason_codes) reason WHERE batch_date=DATE(\"2025-07-22\") GROUP BY reason ORDER BY reason'; print([tuple(r) for r in c.query(q).result()])"
```

If `DUPLICATE_REPLAY` is present, select one replay txn from quarantine and verify there is exactly one accepted row for that txn in the same batch. If `DUPLICATE_CONFLICT` is present, select one conflict txn and verify there are zero accepted rows for that txn in the same batch. If a reason is absent from the live fixture, rely on the unit test evidence for that rule and state that the live batch did not contain that case; do not fabricate live evidence.

- [ ] **Step 9: Add the Feature 05 README runbook**

Add a section:

```markdown
## Feature 05: Pandas data-quality classification
```

Document these exact commands:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Document these rules concisely:
- accepted output is typed and canonical in `bahtflow_analytics.transactions_accepted`;
- quarantine preserves raw business values with ordered `reason_codes` in `bahtflow_ops.transactions_quarantine`;
- duplicate replay/conflict is batch-local in v1;
- base-invalid rows do not participate in duplicate comparison;
- unchanged reruns insert zero rows;
- F05 stops before FX enrichment/conversion and Airflow production wiring.

Do not hard-code measured accepted/quarantine counts in README unless they have been observed from the live run and are intentionally presented as dated evidence.

- [ ] **Step 10: Commit the README evidence/runbook**

```powershell
git add README.md
git commit -m "docs: add Feature 05 classification runbook"
```

- [ ] **Step 11: Run the final Feature 05 gate after the README commit**

```powershell
pytest

python -m py_compile `
  pipeline/transaction_classification.py `
  pipeline/classification_load.py `
  pipeline/bigquery_contract.py `
  pipeline/bigquery_adapter.py `
  scripts/bootstrap_classification.py `
  scripts/classify_transactions.py

docker compose config --quiet
git diff --check
git status --short
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
```

Required final state:

```text
raw partition rows = accepted partition rows + quarantine partition rows
first live classification writes every classified row exactly once
unchanged rerun writes 0 accepted and 0 quarantine rows
accepted values are typed/canonical
quarantine preserves raw evidence with ordered reason_codes
unit tests prove deterministic replay/conflict semantics
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
git status --short = empty
credential-file search = empty
```

- [ ] **Step 12: Review the final diff before integration**

Inspect:

```powershell
git diff main...feat/05-pandas-dq-classification --stat
git diff main...feat/05-pandas-dq-classification -- pipeline/transaction_classification.py pipeline/classification_load.py pipeline/bigquery_contract.py pipeline/bigquery_adapter.py scripts/bootstrap_classification.py scripts/classify_transactions.py README.md
```

Review specifically for:
- any business rule implemented in BigQuery SQL instead of Pandas;
- any global cross-batch duplicate behavior accidentally added;
- any timezone assumption applied to `transaction_dt`;
- any float conversion of monetary values;
- any destructive partition replacement or MERGE;
- any source business value overwritten in quarantine;
- any feature creep into FX/F06 or Airflow/F07.

Only after this diff review and the fresh final gate is green should the branch enter the finishing/integration workflow.
