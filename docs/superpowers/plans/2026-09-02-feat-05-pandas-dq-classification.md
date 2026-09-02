# Feature 05 Pandas Data Quality Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Classify one raw transaction batch with Pandas into typed accepted rows and auditable quarantine rows, persist both outputs idempotently in BigQuery, and prove deterministic batch-local duplicate semantics and retry recovery.

**Architecture:** Read exactly one `bahtflow_raw.transactions` partition, run all business validation and duplicate classification in pure-ish Pandas functions, then append unseen rows to `bahtflow_analytics.transactions_accepted` and `bahtflow_ops.transactions_quarantine` using `source_row_id` as the partition-scoped idempotency key. BigQuery performs storage and partition-query mechanics only; duplicate classification is batch-local and append-only in v1.

**Tech Stack:** Python 3.12, Pandas 2.3.3, Python `decimal.Decimal`, Google Cloud BigQuery client 3.44.0, pytest 9.x, Docker Compose GCP toolbox.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-05-pandas-dq-classification-design.md`

## Global Constraints

- Accepted output is typed/canonical; quarantine output preserves raw business fields, source lineage, and ordered `reason_codes`.
- `transaction_dt` is BigQuery `DATETIME`; do not invent timezone semantics for source `dtts`.
- `amount` uses `decimal.Decimal` and BigQuery `NUMERIC`; do not use float as the business representation.
- Canonical `txn` trims surrounding whitespace only; blank is `MISSING_TXN`; do not add regex or case conversion.
- Canonical `currency` trims and uppercases; valid values are exactly `THB`, `USD`, and `EUR`.
- Valid `region` values are exactly `bkk`, `central`, `north`, `northeast`, and `south`.
- Base-invalid rows never participate in duplicate comparison.
- Duplicate business key is `txn`; canonical payload is `(transaction_dt, amount, currency, region)`.
- Conflicting canonical payloads quarantine every base-valid occurrence as `DUPLICATE_CONFLICT`.
- Exact replay keeps the lowest `(source_file, source_row_number)` and quarantines extra occurrences as `DUPLICATE_REPLAY`.
- Duplicate scope is exactly one `batch_date` in v1; do not add cross-batch reclassification.
- Persistence is append-only and single-writer in v1: query existing `source_row_id`, Pandas anti-filter, then `WRITE_APPEND` unseen rows.
- An unchanged rerun inserts zero accepted rows and zero quarantine rows.
- Partial retry does not roll back a successful first output write; existing output IDs are skipped and missing rows are appended.
- Do not implement FX resolution, currency conversion, facts, marts, production Airflow wiring, dbt, Great Expectations, Spark, staging/`MERGE`, or concurrent-writer behavior in F05.

---

## File Structure

**Create:**
- `pipeline/transaction_classification.py` — pure-ish Pandas validation, canonicalization, duplicate rules, and accepted/quarantine projection.
- `pipeline/classification_load.py` — one-batch BigQuery read/classify/idempotent-write/reconciliation orchestration.
- `scripts/bootstrap_classification.py` — idempotently create or verify the two F05 output tables.
- `scripts/classify_transactions.py` — CLI for one `--batch-date` classification run.
- `tests/pipeline/test_transaction_classification.py` — business-rule and deterministic duplicate tests.
- `tests/pipeline/test_classification_load.py` — persistence, rerun, partial-retry, and reconciliation tests with a stateful fake BigQuery boundary.
- `tests/scripts/test_bootstrap_classification.py` — bootstrap wiring test.
- `tests/scripts/test_classify_transactions.py` — CLI wiring and summary test.

**Modify:**
- `pipeline/bigquery_contract.py` — accepted/quarantine schemas, IDs, and partition fields.
- `pipeline/bigquery_adapter.py` — focused target-partition row reader; reuse existing ID/count/append helpers.
- `tests/pipeline/test_bigquery_contract.py` — exact schema and partition assertions.
- `tests/pipeline/test_bigquery_adapter.py` — partition-row reader test.
- `README.md` — Feature 05 bootstrap/classification/rerun runbook.

No generic repository or service framework is introduced.

---

### Task 1: BigQuery Accepted/Quarantine Contracts and Bootstrap

**Files:**
- Modify: `pipeline/bigquery_contract.py`
- Create: `scripts/bootstrap_classification.py`
- Modify: `tests/pipeline/test_bigquery_contract.py`
- Create: `tests/scripts/test_bootstrap_classification.py`

**Interfaces:**
- Consumes: `BigQueryAdapter.ensure_partitioned_table(dataset_id, table_id, schema, partition_field) -> str` and `load_gcp_settings()`.
- Produces:
  - `ACCEPTED_DATASET_ID: str`
  - `ACCEPTED_TABLE_ID: str`
  - `ACCEPTED_TRANSACTIONS_SCHEMA: tuple[bigquery.SchemaField, ...]`
  - `ACCEPTED_TRANSACTIONS_PARTITION_FIELD: str`
  - `QUARANTINE_DATASET_ID: str`
  - `QUARANTINE_TABLE_ID: str`
  - `QUARANTINE_TRANSACTIONS_SCHEMA: tuple[bigquery.SchemaField, ...]`
  - `QUARANTINE_TRANSACTIONS_PARTITION_FIELD: str`
  - `bootstrap_classification(adapter) -> list[tuple[str, str]]`

- [ ] **Step 1: Write failing exact-schema tests**

Add to `tests/pipeline/test_bigquery_contract.py`:

```python
from pipeline.bigquery_contract import (
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
)


def schema_shape(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def test_accepted_transactions_contract_is_exact():
    assert schema_shape(ACCEPTED_TRANSACTIONS_SCHEMA) == [
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
    assert schema_shape(QUARANTINE_TRANSACTIONS_SCHEMA) == [
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

- [ ] **Step 2: Run the contract tests and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: collection/import failure because the F05 contract names do not exist.

- [ ] **Step 3: Add the exact F05 constants and schemas**

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

- [ ] **Step 4: Re-run contract tests and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: all tests pass.

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
    assert [
        (dataset, table, partition)
        for dataset, table, _schema, partition in adapter.calls
    ] == [
        ("bahtflow_analytics", "transactions_accepted", "batch_date"),
        ("bahtflow_ops", "transactions_quarantine", "batch_date"),
    ]
```

- [ ] **Step 6: Run the bootstrap test and observe RED**

Run:

```powershell
pytest tests/scripts/test_bootstrap_classification.py -v
```

Expected: module import failure for `scripts.bootstrap_classification`.

- [ ] **Step 7: Implement the minimal bootstrap**

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


def bootstrap_classification(adapter) -> list[tuple[str, str]]:
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
            dataset_id,
            table_id,
            schema,
            partition_field,
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

- [ ] **Step 8: Run Task 1 tests and observe GREEN**

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

### Task 2: Base Validation and Canonicalization in Pandas

**Files:**
- Create: `pipeline/transaction_classification.py`
- Create: `tests/pipeline/test_transaction_classification.py`

**Interfaces:**
- Consumes raw transaction columns `txn`, `dtts`, `amount`, `currency`, `region`, `source_file`, `source_checksum`, `source_row_number`, `source_row_id`, `batch_date`, `ingested_at`.
- Produces `validate_and_canonicalize_transactions(raw_df: pd.DataFrame, batch_date: date) -> pd.DataFrame`.
- Returned frame retains all raw columns and adds `_txn_canonical`, `_transaction_dt`, `_amount_numeric`, `_currency_canonical`, `_base_reason_codes`.
- Produces `TransactionClassificationError`, `RAW_TRANSACTION_COLUMNS`, `BASE_REASON_ORDER`, `VALID_CURRENCIES`, `VALID_REGIONS`.

- [ ] **Step 1: Write the first failing canonicalization test**

Create `tests/pipeline/test_transaction_classification.py`:

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


def test_base_validation_canonicalizes_valid_values():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame([raw_row()]),
        date(2025, 7, 22),
    )

    row = result.iloc[0]
    assert row["_txn_canonical"] == "TX-1"
    assert row["_transaction_dt"] == pd.Timestamp("2025-07-22 09:30:00")
    assert row["_amount_numeric"] == Decimal("100.50")
    assert row["_currency_canonical"] == "USD"
    assert row["_base_reason_codes"] == []
```

- [ ] **Step 2: Run the test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_base_validation_canonicalizes_valid_values -v
```

Expected: module import failure because `pipeline.transaction_classification` does not exist.

- [ ] **Step 3: Implement raw-column checks, Decimal parsing, and base classification**

Create `pipeline/transaction_classification.py`:

```python
from __future__ import annotations

from datetime import date
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


def _is_missing(value) -> bool:
    if value is None:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):
        return False
    return str(value).strip() == ""


def _parse_bigquery_numeric(value) -> Decimal | None:
    if _is_missing(value):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None

    _sign, digits, exponent = parsed.as_tuple()
    if exponent >= 0:
        scale = 0
        integer_digits = len(digits) + exponent
    else:
        scale = -exponent
        integer_digits = max(len(digits) - scale, 0)
    if scale > 9 or integer_digits > 29:
        return None
    return parsed


def _require_raw_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in RAW_TRANSACTION_COLUMNS if column not in frame.columns]
    if missing:
        raise TransactionClassificationError(
            f"Missing raw transaction columns: {missing!r}"
        )


def validate_and_canonicalize_transactions(
    raw_df: pd.DataFrame,
    batch_date: date,
) -> pd.DataFrame:
    _require_raw_columns(raw_df)
    frame = raw_df.loc[:, list(RAW_TRANSACTION_COLUMNS)].copy()

    txn_text = frame["txn"].map(
        lambda value: "" if _is_missing(value) else str(value).strip()
    )
    dtts_text = frame["dtts"].map(
        lambda value: "" if _is_missing(value) else str(value).strip()
    )
    amount_text = frame["amount"].map(
        lambda value: "" if _is_missing(value) else str(value).strip()
    )
    currency_text = frame["currency"].map(
        lambda value: "" if _is_missing(value) else str(value).strip().upper()
    )

    frame["_txn_canonical"] = txn_text
    frame["_transaction_dt"] = pd.to_datetime(
        dtts_text,
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    frame["_amount_numeric"] = amount_text.map(_parse_bigquery_numeric)
    frame["_currency_canonical"] = currency_text

    missing_txn = txn_text.eq("")
    invalid_dtts = frame["_transaction_dt"].isna()
    dtts_batch_mismatch = (
        frame["_transaction_dt"].notna()
        & frame["_transaction_dt"].dt.date.ne(batch_date)
    )
    invalid_amount = frame["_amount_numeric"].isna()
    negative_amount = frame["_amount_numeric"].map(
        lambda value: value is not None and value < Decimal("0")
    )
    invalid_currency = ~currency_text.isin(VALID_CURRENCIES)
    invalid_region = ~frame["region"].isin(VALID_REGIONS)

    masks = {
        "MISSING_TXN": missing_txn,
        "INVALID_DTTS": invalid_dtts,
        "DTTS_BATCH_DATE_MISMATCH": dtts_batch_mismatch,
        "INVALID_AMOUNT": invalid_amount,
        "NEGATIVE_AMOUNT": negative_amount,
        "INVALID_CURRENCY": invalid_currency,
        "INVALID_REGION": invalid_region,
    }
    frame["_base_reason_codes"] = [
        [reason for reason in BASE_REASON_ORDER if bool(masks[reason].loc[index])]
        for index in frame.index
    ]
    return frame
```

- [ ] **Step 4: Re-run the first test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_base_validation_canonicalizes_valid_values -v
```

Expected: PASS.

- [ ] **Step 5: Add failing tests for every base-validation rule and reason order**

Append concrete cases:

```python
import pytest

from pipeline.transaction_classification import TransactionClassificationError


def test_zero_amount_is_valid_and_negative_amount_is_quarantinable():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame([
            raw_row(source_row_id="zero", amount="0"),
            raw_row(source_row_id="negative", amount="-0.01"),
        ]),
        date(2025, 7, 22),
    ).set_index("source_row_id")
    assert result.loc["zero", "_base_reason_codes"] == []
    assert result.loc["negative", "_base_reason_codes"] == ["NEGATIVE_AMOUNT"]


def test_invalid_amount_covers_blank_text_non_finite_scale_and_width():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame([
            raw_row(source_row_id="blank", amount=" "),
            raw_row(source_row_id="text", amount="N/A"),
            raw_row(source_row_id="inf", amount="Infinity"),
            raw_row(source_row_id="scale", amount="0.1234567891"),
            raw_row(source_row_id="width", amount="100000000000000000000000000000"),
        ]),
        date(2025, 7, 22),
    )
    assert result["_base_reason_codes"].tolist() == [
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
    ]


def test_datetime_rules_distinguish_parse_failure_from_batch_mismatch():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame([
            raw_row(source_row_id="bad-date", dtts="not-a-date"),
            raw_row(source_row_id="wrong-day", dtts="2025-07-23 00:00:00"),
        ]),
        date(2025, 7, 22),
    )
    assert result["_base_reason_codes"].tolist() == [
        ["INVALID_DTTS"],
        ["DTTS_BATCH_DATE_MISMATCH"],
    ]


def test_multiple_base_failures_keep_fixed_reason_order():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame([
            raw_row(txn=" ", amount="N/A", currency=" gbp ", region="unknown")
        ]),
        date(2025, 7, 22),
    )
    assert result.iloc[0]["_base_reason_codes"] == [
        "MISSING_TXN",
        "INVALID_AMOUNT",
        "INVALID_CURRENCY",
        "INVALID_REGION",
    ]


def test_missing_required_raw_column_fails_explicitly():
    frame = pd.DataFrame([raw_row()]).drop(columns=["source_row_id"])
    with pytest.raises(TransactionClassificationError, match="source_row_id"):
        validate_and_canonicalize_transactions(frame, date(2025, 7, 22))
```

Also add one test that `" eur "` canonicalizes to `EUR` while `GBP` gets `INVALID_CURRENCY`, and one test that an unsupported region gets `INVALID_REGION`.

- [ ] **Step 6: Run all Task 2 tests and make them GREEN**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py -v
```

Expected: all currently written classifier tests pass.

- [ ] **Step 7: Commit Task 2**

```powershell
git add pipeline/transaction_classification.py tests/pipeline/test_transaction_classification.py
git commit -m "feat: validate and canonicalize transactions"
```

---

### Task 3: Deterministic Duplicate Classification and Final Projections

**Files:**
- Modify: `pipeline/transaction_classification.py`
- Modify: `tests/pipeline/test_transaction_classification.py`

**Interfaces:**
- Consumes: `validate_and_canonicalize_transactions(raw_df, batch_date) -> pd.DataFrame` from Task 2.
- Produces:
  - `ClassificationResult(accepted: pd.DataFrame, quarantine: pd.DataFrame)`
  - `classify_duplicates(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`
  - `classify_transactions(raw_df: pd.DataFrame, batch_date: date, classified_at: datetime) -> ClassificationResult`

- [ ] **Step 1: Write the failing exact-replay test**

Append to `tests/pipeline/test_transaction_classification.py`:

```python
from pipeline.transaction_classification import classify_transactions


def test_exact_replay_accepts_lowest_source_lineage():
    frame = pd.DataFrame([
        raw_row(
            txn="TX-R",
            source_row_id="north",
            source_file="transactions/business_date=2025-07-22/sales_north_20250722.csv.gz",
            source_row_number=1,
        ),
        raw_row(
            txn="TX-R",
            source_row_id="bkk-later",
            source_file="transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
            source_row_number=11,
        ),
        raw_row(
            txn=" TX-R ",
            source_row_id="winner",
            source_file="transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
            source_row_number=10,
        ),
    ])
    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )

    assert result.accepted["source_row_id"].tolist() == ["winner"]
    quarantine = result.quarantine.sort_values("source_row_id").reset_index(drop=True)
    assert quarantine["source_row_id"].tolist() == ["bkk-later", "north"]
    assert quarantine["reason_codes"].tolist() == [
        ["DUPLICATE_REPLAY"],
        ["DUPLICATE_REPLAY"],
    ]
```

- [ ] **Step 2: Run the replay test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_exact_replay_accepts_lowest_source_lineage -v
```

Expected: import failure because `classify_transactions` does not exist.

- [ ] **Step 3: Implement result types, duplicate rules, and output projections**

Extend `pipeline/transaction_classification.py`:

```python
from dataclasses import dataclass
from datetime import datetime

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


def classify_duplicates(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    accepted_parts = []
    quarantine_parts = []
    payload_columns = [
        "_transaction_dt",
        "_amount_numeric",
        "_currency_canonical",
        "region",
    ]

    for _txn, group in valid_df.groupby("_txn_canonical", sort=False, dropna=False):
        payloads = set(
            group.loc[:, payload_columns].itertuples(index=False, name=None)
        )
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

    if accepted_parts:
        accepted = pd.concat(accepted_parts, ignore_index=True)
    else:
        accepted = valid_df.iloc[0:0].copy()

    if quarantine_parts:
        duplicate_quarantine = pd.concat(quarantine_parts, ignore_index=True)
    else:
        duplicate_quarantine = valid_df.iloc[0:0].copy()
        duplicate_quarantine["_duplicate_reason"] = pd.Series(dtype="object")

    return accepted, duplicate_quarantine


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

    accepted = pd.DataFrame(index=accepted_valid.index)
    accepted["txn"] = accepted_valid["_txn_canonical"]
    accepted["transaction_dt"] = accepted_valid["_transaction_dt"]
    accepted["amount"] = accepted_valid["_amount_numeric"]
    accepted["currency"] = accepted_valid["_currency_canonical"]
    accepted["region"] = accepted_valid["region"]
    for column in (
        "source_file",
        "source_checksum",
        "source_row_number",
        "source_row_id",
        "batch_date",
        "ingested_at",
    ):
        accepted[column] = accepted_valid[column]
    accepted["classified_at"] = classified_at
    accepted = accepted.loc[:, list(ACCEPTED_COLUMNS)].reset_index(drop=True)

    base_quarantine = base_invalid.loc[:, list(RAW_TRANSACTION_COLUMNS)].copy()
    base_quarantine["reason_codes"] = base_invalid["_base_reason_codes"].map(list)
    base_quarantine["quarantined_at"] = classified_at

    duplicate_output = duplicate_quarantine.loc[:, list(RAW_TRANSACTION_COLUMNS)].copy()
    duplicate_output["reason_codes"] = duplicate_quarantine["_duplicate_reason"].map(
        lambda reason: [reason]
    )
    duplicate_output["quarantined_at"] = classified_at

    quarantine = pd.concat(
        [base_quarantine, duplicate_output],
        ignore_index=True,
    )
    quarantine = quarantine.loc[:, list(QUARANTINE_COLUMNS)]

    if len(raw_df) != len(accepted) + len(quarantine):
        raise TransactionClassificationError(
            "Classification reconciliation failed: "
            f"raw={len(raw_df)} accepted={len(accepted)} quarantine={len(quarantine)}"
        )

    return ClassificationResult(accepted=accepted, quarantine=quarantine)
```

- [ ] **Step 4: Re-run the replay test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_exact_replay_accepts_lowest_source_lineage -v
```

Expected: PASS.

- [ ] **Step 5: Add failing conflict, base-invalid isolation, raw-preservation, and reconciliation tests**

Append:

```python
def test_conflicting_canonical_payloads_quarantine_every_valid_occurrence():
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


def test_base_invalid_row_does_not_poison_valid_row_with_same_txn():
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
    assert invalid["reason_codes"] == ["INVALID_AMOUNT"]


def test_accepted_is_canonical_and_quarantine_preserves_raw_business_values():
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
    assert accepted["txn"] == "TX-OK"
    assert accepted["amount"] == Decimal("100.50")
    assert accepted["currency"] == "USD"
    assert isinstance(accepted["transaction_dt"], pd.Timestamp)

    quarantined = result.quarantine.iloc[0]
    assert quarantined["txn"] == " raw-value "
    assert quarantined["amount"] == "N/A"
    assert quarantined["currency"] == " gbp "
    assert quarantined["reason_codes"] == ["INVALID_AMOUNT", "INVALID_CURRENCY"]


def test_every_raw_row_has_exactly_one_classification_outcome():
    frame = pd.DataFrame([
        raw_row(source_row_id="unique", txn="TX-U"),
        raw_row(source_row_id="replay-a", txn="TX-R", source_row_number=1),
        raw_row(source_row_id="replay-b", txn="TX-R", source_row_number=2),
        raw_row(source_row_id="invalid", txn="TX-I", amount="N/A"),
    ])
    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )
    output_ids = set(result.accepted["source_row_id"]) | set(
        result.quarantine["source_row_id"]
    )
    assert len(result.accepted) + len(result.quarantine) == len(frame)
    assert output_ids == set(frame["source_row_id"])
```

- [ ] **Step 6: Run the full pure-Pandas suite and make it GREEN**

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

### Task 4: Focused BigQuery Partition-Row Reader

**Files:**
- Modify: `pipeline/bigquery_adapter.py`
- Modify: `tests/pipeline/test_bigquery_adapter.py`

**Interfaces:**
- Consumes: existing `_partition_job_config(partition_date)`.
- Produces `BigQueryAdapter.query_partition_rows(dataset_id: str, table_id: str, partition_field: str, partition_date: date, columns: tuple[str, ...]) -> list[dict]`.

- [ ] **Step 1: Write the failing adapter test**

Extend `tests/pipeline/test_bigquery_adapter.py` with a fake row supporting `.items()` and configure the existing fake client query result. Test:

```python
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

- [ ] **Step 2: Run the adapter test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py::test_query_partition_rows_selects_requested_columns_and_returns_dicts -v
```

Expected: `AttributeError` for missing `query_partition_rows`.

- [ ] **Step 3: Implement only the narrow partition reader**

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

- [ ] **Step 4: Run adapter and contract tests and observe GREEN**

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

### Task 5: One-Batch Classification Persistence and Partial-Retry Recovery

**Files:**
- Create: `pipeline/classification_load.py`
- Create: `tests/pipeline/test_classification_load.py`

**Interfaces:**
- Consumes: `query_partition_rows`, `query_source_row_ids`, `append_rows`, `query_partition_row_count`, `anti_filter_existing`, and `classify_transactions`.
- Produces `ClassificationLoadError`, `ClassificationLoadSummary`, and `classify_and_load_batch(batch_date: date, bigquery_adapter, classified_at: datetime | None = None) -> ClassificationLoadSummary`.

- [ ] **Step 1: Write the concrete stateful fake and first-run/rerun test**

Create `tests/pipeline/test_classification_load.py`:

```python
from datetime import date, datetime, timezone

import pytest

from pipeline.classification_load import classify_and_load_batch


def row_matches_date(row, partition_date):
    return row["batch_date"] in (partition_date, partition_date.isoformat())


@pytest.fixture
def raw_rows_fixture():
    common = {
        "dtts": "2025-07-22 09:30:00",
        "amount": "100.00",
        "currency": "THB",
        "region": "bkk",
        "source_checksum": "abc",
        "batch_date": date(2025, 7, 22),
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }
    return [
        {
            **common,
            "txn": "TX-U",
            "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
            "source_row_number": 1,
            "source_row_id": "unique",
        },
        {
            **common,
            "txn": "TX-R",
            "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
            "source_row_number": 2,
            "source_row_id": "replay-winner",
        },
        {
            **common,
            "txn": "TX-R",
            "source_file": "transactions/business_date=2025-07-22/sales_north_20250722.csv.gz",
            "source_row_number": 1,
            "source_row_id": "replay-loser",
        },
        {
            **common,
            "txn": "TX-I",
            "amount": "N/A",
            "source_file": "transactions/business_date=2025-07-22/sales_south_20250722.csv.gz",
            "source_row_number": 1,
            "source_row_id": "invalid",
        },
    ]


class StatefulBigQueryFake:
    def __init__(self, raw_rows):
        self.raw_rows = list(raw_rows)
        self.outputs = {
            ("bahtflow_analytics", "transactions_accepted"): [],
            ("bahtflow_ops", "transactions_quarantine"): [],
        }
        self.fail_next_quarantine_append = False

    def query_partition_rows(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
        columns,
    ):
        assert (dataset_id, table_id, partition_field) == (
            "bahtflow_raw",
            "transactions",
            "batch_date",
        )
        return [
            {column: row[column] for column in columns}
            for row in self.raw_rows
            if row_matches_date(row, partition_date)
        ]

    def query_source_row_ids(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
    ):
        assert partition_field == "batch_date"
        return {
            row["source_row_id"]
            for row in self.outputs[(dataset_id, table_id)]
            if row_matches_date(row, partition_date)
        }

    def append_rows(self, dataset_id, table_id, rows, schema):
        if (
            self.fail_next_quarantine_append
            and (dataset_id, table_id)
            == ("bahtflow_ops", "transactions_quarantine")
        ):
            self.fail_next_quarantine_append = False
            raise RuntimeError("simulated quarantine write failure")
        self.outputs[(dataset_id, table_id)].extend(rows)
        return len(rows)

    def query_partition_row_count(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
    ):
        assert partition_field == "batch_date"
        return sum(
            1
            for row in self.outputs[(dataset_id, table_id)]
            if row_matches_date(row, partition_date)
        )


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

Expected: module import failure because `pipeline.classification_load` does not exist.

- [ ] **Step 3: Implement JSON-safe records and one-batch orchestration**

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
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _frame_to_records(frame: pd.DataFrame) -> list[dict]:
    return [
        {key: _json_safe_value(value) for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]


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
    raw_frame = pd.DataFrame(raw_rows, columns=list(RAW_TRANSACTION_COLUMNS))
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
    reconciled = (
        len(raw_frame) == accepted_partition_rows + quarantine_partition_rows
    )
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

- [ ] **Step 4: Re-run the first-run/rerun test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_classification_load.py::test_first_run_classifies_and_rerun_inserts_zero -v
```

Expected: PASS.

- [ ] **Step 5: Write the partial-retry test**

Append:

```python
def test_partial_retry_skips_persisted_accepted_and_writes_missing_quarantine(
    raw_rows_fixture,
):
    fake = StatefulBigQueryFake(raw_rows_fixture)
    fake.fail_next_quarantine_append = True

    with pytest.raises(RuntimeError, match="simulated quarantine write failure"):
        classify_and_load_batch(
            batch_date=date(2025, 7, 22),
            bigquery_adapter=fake,
            classified_at=datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
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

- [ ] **Step 6: Run the partial-retry test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_classification_load.py::test_partial_retry_skips_persisted_accepted_and_writes_missing_quarantine -v
```

Expected: PASS with the Task 5 implementation. If it fails, change only the idempotency/retry behavior demonstrated by this test; do not add rollback infrastructure.

- [ ] **Step 7: Write the persisted-reconciliation failure test**

Append:

```python
from pipeline.classification_load import ClassificationLoadError


class MismatchedCountFake(StatefulBigQueryFake):
    def query_partition_row_count(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
    ):
        count = super().query_partition_row_count(
            dataset_id,
            table_id,
            partition_field,
            partition_date,
        )
        if (dataset_id, table_id) == (
            "bahtflow_analytics",
            "transactions_accepted",
        ):
            return count + 1
        return count


def test_persisted_reconciliation_mismatch_fails(raw_rows_fixture):
    fake = MismatchedCountFake(raw_rows_fixture)
    with pytest.raises(
        ClassificationLoadError,
        match="Persisted classification reconciliation failed",
    ):
        classify_and_load_batch(
            batch_date=date(2025, 7, 22),
            bigquery_adapter=fake,
            classified_at=datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
        )
```

- [ ] **Step 8: Run all F05 pipeline tests and observe GREEN**

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

### Task 6: Classification CLI

**Files:**
- Create: `scripts/classify_transactions.py`
- Create: `tests/scripts/test_classify_transactions.py`

**Interfaces:**
- Consumes: `load_gcp_settings()`, `BigQueryAdapter`, `classify_and_load_batch`.
- Produces CLI `python -m scripts.classify_transactions --batch-date YYYY-MM-DD`.

- [ ] **Step 1: Write the failing CLI test**

Create `tests/scripts/test_classify_transactions.py`:

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

Expected: module import failure for `scripts.classify_transactions`.

- [ ] **Step 3: Implement the dependency-light CLI**

Create `scripts/classify_transactions.py`:

```python
from __future__ import annotations

import argparse
from datetime import date

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.classification_load import classify_and_load_batch
from pipeline.config import load_gcp_settings


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Classify one raw BahtFlow transaction batch"
    )
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
    fields = (
        "batch_date",
        "raw_rows",
        "accepted_rows",
        "quarantine_rows",
        "accepted_inserted_rows",
        "quarantine_inserted_rows",
        "accepted_partition_rows",
        "quarantine_partition_rows",
        "reconciled",
    )
    for field in fields:
        print(f"{field}={getattr(summary, field)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run CLI and focused F05 tests and observe GREEN**

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

### Task 7: Live Acceptance, README Runbook, and Final Review

**Files:**
- Modify: `README.md`

**Interfaces:**
- Uses the F04 raw partition `2025-07-22`, currently expected to contain 8,978 rows.
- Uses `scripts.bootstrap_classification` and `scripts.classify_transactions` through the `gcp-toolbox` Compose profile.

- [ ] **Step 1: Pull the branch locally and run the complete local gate before cloud writes**

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

Required: `pytest` has zero failures; compile/config/diff/credential checks are quiet; working tree is clean.

- [ ] **Step 2: Build the GCP toolbox**

```powershell
docker compose --profile gcp build gcp-toolbox
```

Expected: successful build with the already pinned F04 dependencies.

- [ ] **Step 3: Bootstrap the two F05 tables twice**

Run twice:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification
```

On a fresh F05 setup the first run should report `created` for both tables and the second `verified`. If the first run already reports `verified`, do not delete tables or data automatically.

- [ ] **Step 4: Record raw and output partition pre-state**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from datetime import date; from pipeline.config import load_gcp_settings; from pipeline.bigquery_adapter import BigQueryAdapter; s=load_gcp_settings(); a=BigQueryAdapter(s.project_id); d=date(2025,7,22); print('raw_before=',a.query_partition_row_count('bahtflow_raw','transactions','batch_date',d)); print('accepted_before=',a.query_partition_row_count('bahtflow_analytics','transactions_accepted','batch_date',d)); print('quarantine_before=',a.query_partition_row_count('bahtflow_ops','transactions_quarantine','batch_date',d))"
```

Fresh acceptance target:

```text
raw_before=8978
accepted_before=0
quarantine_before=0
```

If either output partition is nonzero, stop and choose a fresh acceptance strategy explicitly; do not silently delete or overwrite classified data.

- [ ] **Step 5: Run the first live classification and record measured counts**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Required relationships:

```text
batch_date=2025-07-22
raw_rows=8978
accepted_inserted_rows=accepted_rows
quarantine_inserted_rows=quarantine_rows
accepted_partition_rows=accepted_rows
quarantine_partition_rows=quarantine_rows
reconciled=True
8978 = accepted_rows + quarantine_rows
```

Do not predict accepted/quarantine counts before observing the live output.

- [ ] **Step 6: Run the same classification command again for idempotency evidence**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Required:

```text
raw_rows=8978
accepted_inserted_rows=0
quarantine_inserted_rows=0
accepted_partition_rows=<same measured first-run accepted count>
quarantine_partition_rows=<same measured first-run quarantine count>
reconciled=True
```

- [ ] **Step 7: Query typed accepted samples and raw quarantine evidence**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from pipeline.config import load_gcp_settings; from google.cloud import bigquery; s=load_gcp_settings(); c=bigquery.Client(project=s.project_id); a=list(c.query('SELECT txn, transaction_dt, amount, currency, region, source_row_id FROM `'+s.project_id+'.bahtflow_analytics.transactions_accepted` WHERE batch_date=DATE(\"2025-07-22\") ORDER BY source_file, source_row_number LIMIT 5').result()); q=list(c.query('SELECT txn, dtts, amount, currency, reason_codes, source_row_id FROM `'+s.project_id+'.bahtflow_ops.transactions_quarantine` WHERE batch_date=DATE(\"2025-07-22\") AND ARRAY_LENGTH(reason_codes)>0 ORDER BY source_file, source_row_number LIMIT 5').result()); print('accepted_sample_rows=',len(a)); print('accepted_sample=',[tuple(r) for r in a]); print('quarantine_sample_rows=',len(q)); print('quarantine_sample=',[tuple(r) for r in q])"
```

Evidence required:
- accepted sample exposes timezone-free `DATETIME` values;
- accepted amounts are numeric, not malformed raw strings;
- accepted currencies are only `THB`, `USD`, `EUR`;
- quarantine samples retain raw business values and every sampled reason array is non-empty.

- [ ] **Step 8: Query reason distribution and verify live duplicate examples when present**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "from pipeline.config import load_gcp_settings; from google.cloud import bigquery; s=load_gcp_settings(); c=bigquery.Client(project=s.project_id); q='SELECT reason, COUNT(*) FROM `'+s.project_id+'.bahtflow_ops.transactions_quarantine`, UNNEST(reason_codes) reason WHERE batch_date=DATE(\"2025-07-22\") GROUP BY reason ORDER BY reason'; print([tuple(r) for r in c.query(q).result()])"
```

If `DUPLICATE_REPLAY` is present, select one replay txn and verify exactly one accepted winner in the same batch. If `DUPLICATE_CONFLICT` is present, select one conflict txn and verify zero accepted rows for that txn in the same batch. If a case is absent from the live fixture, state that the live batch lacks the case and use the unit-test evidence for that rule.

- [ ] **Step 9: Add the Feature 05 README runbook**

Add section:

```markdown
## Feature 05: Pandas data-quality classification
```

Include these commands:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Document explicitly:
- accepted is typed/canonical in `bahtflow_analytics.transactions_accepted`;
- quarantine preserves raw business values and ordered reason codes in `bahtflow_ops.transactions_quarantine`;
- duplicate scope is batch-local in v1;
- base-invalid rows do not enter duplicate comparison;
- unchanged reruns insert zero rows;
- F05 stops before FX/F06 and Airflow/F07.

- [ ] **Step 10: Commit the README runbook**

```powershell
git add README.md
git commit -m "docs: add Feature 05 classification runbook"
```

- [ ] **Step 11: Run the fresh final local gate**

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

Required: zero test failures and all non-test checks clean/quiet.

- [ ] **Step 12: Review the final diff before integration**

```powershell
git diff main...feat/05-pandas-dq-classification --stat
git diff main...feat/05-pandas-dq-classification -- `
  pipeline/transaction_classification.py `
  pipeline/classification_load.py `
  pipeline/bigquery_contract.py `
  pipeline/bigquery_adapter.py `
  scripts/bootstrap_classification.py `
  scripts/classify_transactions.py `
  README.md
```

Review specifically for:
- business classification accidentally implemented in BigQuery SQL;
- global cross-batch duplicate behavior;
- timezone assumptions applied to `transaction_dt`;
- float conversion of monetary values;
- destructive partition replacement or `MERGE`;
- canonical values replacing raw business values in quarantine;
- FX/F06 or Airflow/F07 feature creep.

Only after fresh live evidence, a fresh full local gate, and Review Diff are clean should F05 enter the finishing/integration workflow.

## Final F05 Acceptance Gate

```text
raw partition rows = accepted partition rows + quarantine partition rows
first live classification writes every classified row exactly once
unchanged rerun writes 0 accepted and 0 quarantine rows
accepted values are typed/canonical
quarantine preserves raw evidence with ordered reason_codes
unit tests prove deterministic replay/conflict semantics
partial-retry test proves recovery after accepted-only persistence
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
git status --short = empty
credential-file search = empty
```
