# Feature 05 Pandas Data Quality Classification Implementation Plan

> Execute task-by-task with TDD. Do not write production behavior before the corresponding RED test has been observed.

**Goal:** Classify one raw transaction batch with Pandas into typed accepted rows and auditable quarantine rows, persist both outputs idempotently in BigQuery, and prove deterministic batch-local duplicate semantics and retry recovery.

**Architecture:** Read exactly one `bahtflow_raw.transactions` partition, run all business validation and duplicate classification in Pandas, then append unseen rows to `bahtflow_analytics.transactions_accepted` and `bahtflow_ops.transactions_quarantine` using `source_row_id` as the partition-scoped idempotency key. BigQuery performs storage/query mechanics only. Duplicate semantics are batch-local and append-only in v1.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-05-pandas-dq-classification-design.md`

## Locked Constraints

- Accepted: typed/canonical; quarantine: raw business values + lineage + ordered `reason_codes`.
- `transaction_dt` is BigQuery `DATETIME`; do not invent timezone semantics.
- `amount` uses `decimal.Decimal` / BigQuery `NUMERIC`; no float business representation.
- `txn`: trim only; blank is `MISSING_TXN`; no regex and no case conversion.
- `currency`: trim + uppercase; only `THB`, `USD`, `EUR`.
- `region`: exactly `bkk`, `central`, `north`, `northeast`, `south`.
- Base-invalid rows never participate in duplicate comparison.
- Duplicate business key is `txn`; canonical payload is `(transaction_dt, amount, currency, region)`.
- Conflicting canonical payloads quarantine every base-valid occurrence as `DUPLICATE_CONFLICT`.
- Exact replay keeps the lowest `(source_file, source_row_number)` and quarantines extras as `DUPLICATE_REPLAY`.
- Duplicate scope is one `batch_date`, not global history.
- Persistence uses query existing IDs -> Pandas anti-filter -> `WRITE_APPEND`; single-writer v1.
- Partial retry does not roll back a successful first output write.
- No FX, currency conversion, facts, marts, production Airflow wiring, dbt, Spark, `MERGE`, or concurrent-writer behavior in F05.

## Files

Create:
- `pipeline/transaction_classification.py`
- `pipeline/classification_load.py`
- `scripts/bootstrap_classification.py`
- `scripts/classify_transactions.py`
- `tests/pipeline/test_transaction_classification.py`
- `tests/pipeline/test_classification_load.py`
- `tests/scripts/test_bootstrap_classification.py`
- `tests/scripts/test_classify_transactions.py`

Modify:
- `pipeline/bigquery_contract.py`
- `pipeline/bigquery_adapter.py`
- `tests/pipeline/test_bigquery_contract.py`
- `tests/pipeline/test_bigquery_adapter.py`
- `README.md`

---

## Task 1 — BigQuery accepted/quarantine contracts and bootstrap

**Files:** `pipeline/bigquery_contract.py`, `scripts/bootstrap_classification.py`, `tests/pipeline/test_bigquery_contract.py`, `tests/scripts/test_bootstrap_classification.py`.

### 1A. Contract RED

Add exact schema tests importing these new constants:

```python
ACCEPTED_DATASET_ID = "bahtflow_analytics"
ACCEPTED_TABLE_ID = "transactions_accepted"
ACCEPTED_TRANSACTIONS_PARTITION_FIELD = "batch_date"
QUARANTINE_DATASET_ID = "bahtflow_ops"
QUARANTINE_TABLE_ID = "transactions_quarantine"
QUARANTINE_TRANSACTIONS_PARTITION_FIELD = "batch_date"
```

Accepted schema expected shape:

```text
txn STRING REQUIRED
transaction_dt DATETIME REQUIRED
amount NUMERIC REQUIRED
currency STRING REQUIRED
region STRING REQUIRED
source_file STRING REQUIRED
source_checksum STRING REQUIRED
source_row_number INTEGER REQUIRED
source_row_id STRING REQUIRED
batch_date DATE REQUIRED
ingested_at TIMESTAMP REQUIRED
classified_at TIMESTAMP REQUIRED
```

Quarantine schema expected shape:

```text
txn STRING NULLABLE
dtts STRING NULLABLE
amount STRING NULLABLE
currency STRING NULLABLE
region STRING REQUIRED
source_file STRING REQUIRED
source_checksum STRING REQUIRED
source_row_number INTEGER REQUIRED
source_row_id STRING REQUIRED
batch_date DATE REQUIRED
ingested_at TIMESTAMP REQUIRED
reason_codes STRING REPEATED
quarantined_at TIMESTAMP REQUIRED
```

Run and observe RED:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected RED: import failure for missing F05 contract constants.

### 1B. Contract GREEN

Implement only the constants/schemas above in `pipeline/bigquery_contract.py`. Re-run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py -v
```

Expected: all contract tests pass.

### 1C. Bootstrap RED/GREEN

Write `tests/scripts/test_bootstrap_classification.py` with a recording adapter whose `ensure_partitioned_table(...)` returns `verified`. Assert exactly two calls, in this order:

```text
bahtflow_analytics.transactions_accepted / batch_date
bahtflow_ops.transactions_quarantine / batch_date
```

Run RED:

```powershell
pytest tests/scripts/test_bootstrap_classification.py -v
```

Expected: `ModuleNotFoundError` for `scripts.bootstrap_classification`.

Implement `scripts/bootstrap_classification.py` with:

```python
def bootstrap_classification(adapter) -> list[tuple[str, str]]:
    ...
```

It must call existing `BigQueryAdapter.ensure_partitioned_table` and return `(full_table_name, status)` pairs. `main()` loads GCP settings and prints:

```text
table=<dataset.table> status=<created|verified>
```

Re-run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py tests/scripts/test_bootstrap_classification.py -v
```

Commit:

```powershell
git add pipeline/bigquery_contract.py scripts/bootstrap_classification.py tests/pipeline/test_bigquery_contract.py tests/scripts/test_bootstrap_classification.py
git commit -m "feat: add classification table contracts"
```

---

## Task 2 — Base validation and canonicalization in Pandas

**Files:** `pipeline/transaction_classification.py`, `tests/pipeline/test_transaction_classification.py`.

### 2A. Public contract

Create these constants/types only as tests require them:

```python
RAW_TRANSACTION_COLUMNS = (
    "txn", "dtts", "amount", "currency", "region",
    "source_file", "source_checksum", "source_row_number",
    "source_row_id", "batch_date", "ingested_at",
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
```

Primary function:

```python
def validate_and_canonicalize_transactions(
    raw_df: pd.DataFrame,
    batch_date: date,
) -> pd.DataFrame:
    ...
```

It retains all raw columns and adds internal columns:

```text
_txn_canonical
_transaction_dt
_amount_numeric
_currency_canonical
_base_reason_codes
```

### 2B. First RED

Create a reusable test row:

```python
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
```

Test one valid row. Assert using bracket access, not Series attribute access:

```python
row = result.iloc[0]
assert row["_txn_canonical"] == "TX-1"
assert row["_transaction_dt"] == pd.Timestamp("2025-07-22 09:30:00")
assert row["_amount_numeric"] == Decimal("100.50")
assert row["_currency_canonical"] == "USD"
assert row["_base_reason_codes"] == []
```

Run RED:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_base_validation_canonicalizes_valid_values -v
```

Expected: module import failure.

### 2C. Minimal GREEN

Implement raw-column validation and canonicalization. Always select tuple-defined columns as a list:

```python
frame = raw_df.loc[:, list(RAW_TRANSACTION_COLUMNS)].copy()
```

Parse `dtts` exactly with:

```python
pd.to_datetime(text, format="%Y-%m-%d %H:%M:%S", errors="coerce")
```

Parse money with a helper returning `Decimal | None`. Reject null/blank/non-numeric/non-finite values and values outside BigQuery NUMERIC capacity (precision <= 38, scale <= 9, integer digits <= 29). Do not use float.

Collect all applicable base reasons in `BASE_REASON_ORDER`.

Run the focused valid-row test until GREEN.

### 2D. Expand RED/GREEN coverage

Add tests for:
- missing/blank txn;
- invalid datetime;
- parsed datetime whose date differs from `batch_date`;
- blank, `N/A`, non-finite, scale-overflow, and width-overflow amount;
- zero amount accepted;
- negative amount -> `NEGATIVE_AMOUNT`;
- currency trim/uppercase and unsupported currency;
- invalid region;
- one row with multiple failures preserving exact reason order;
- missing DataFrame column raises `TransactionClassificationError` naming the column.

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py -v
```

Commit:

```powershell
git add pipeline/transaction_classification.py tests/pipeline/test_transaction_classification.py
git commit -m "feat: validate and canonicalize transactions"
```

---

## Task 3 — Deterministic duplicate classification and final projections

**Files:** same Task 2 files.

### 3A. Result interfaces

Add:

```python
@dataclass(frozen=True)
class ClassificationResult:
    accepted: pd.DataFrame
    quarantine: pd.DataFrame


def classify_duplicates(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ...


def classify_transactions(
    raw_df: pd.DataFrame,
    batch_date: date,
    classified_at: datetime,
) -> ClassificationResult:
    ...
```

Accepted output columns exactly:

```text
txn, transaction_dt, amount, currency, region,
source_file, source_checksum, source_row_number, source_row_id,
batch_date, ingested_at, classified_at
```

Quarantine output columns exactly:

```text
txn, dtts, amount, currency, region,
source_file, source_checksum, source_row_number, source_row_id,
batch_date, ingested_at, reason_codes, quarantined_at
```

### 3B. Replay RED

Write a test with three base-valid rows sharing canonical `txn` and payload but deliberately shuffled source lineage. The winner must be the lowest `(source_file, source_row_number)` regardless of input DataFrame order. Losers must have exactly `["DUPLICATE_REPLAY"]`.

Run RED:

```powershell
pytest tests/pipeline/test_transaction_classification.py::test_exact_replay_accepts_lowest_source_lineage -v
```

Expected: missing `classify_transactions`.

### 3C. Minimal duplicate GREEN

Do not use namedtuple attributes for internal columns beginning with `_`; Pandas may rename them. Build the canonical payload set safely:

```python
payload_columns = [
    "_transaction_dt",
    "_amount_numeric",
    "_currency_canonical",
    "region",
]
payloads = set(
    group.loc[:, payload_columns].itertuples(index=False, name=None)
)
```

Within each canonical txn group:
- `len(payloads) > 1` -> every base-valid row gets `_duplicate_reason = "DUPLICATE_CONFLICT"`;
- one payload -> stable sort by `source_file`, `source_row_number`; first accepted, remaining `DUPLICATE_REPLAY`.

Do not make final quarantine DataFrame order a business contract; tests may sort by `source_row_id` before comparing sets/reasons.

### 3D. Expand duplicate/projected-output tests

Add tests for:
- canonical conflict quarantines all valid occurrences and accepts none;
- invalid amount row with same txn as a valid row does not poison valid row;
- accepted output contains canonical txn/currency, Decimal amount, parsed timezone-free datetime;
- quarantine preserves original raw txn/dtts/amount/currency strings;
- all raw source_row_ids appear in exactly one of accepted/quarantine;
- `len(raw) == len(accepted) + len(quarantine)`.

When projecting raw columns use:

```python
base_invalid.loc[:, list(RAW_TRANSACTION_COLUMNS)]
duplicate_quarantine.loc[:, list(RAW_TRANSACTION_COLUMNS)]
```

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py -v
```

Commit:

```powershell
git add pipeline/transaction_classification.py tests/pipeline/test_transaction_classification.py
git commit -m "feat: classify transaction data quality"
```

---

## Task 4 — Focused BigQuery partition-row read helper

**Files:** `pipeline/bigquery_adapter.py`, `tests/pipeline/test_bigquery_adapter.py`.

### 4A. RED

Add a fake mapping row and test:

```python
rows = adapter.query_partition_rows(
    "bahtflow_raw",
    "transactions",
    "batch_date",
    date(2025, 7, 22),
    ("txn", "source_row_id"),
)
```

Assert returned list of dicts and SQL contains:

```text
SELECT txn, source_row_id
FROM `proj.bahtflow_raw.transactions`
WHERE batch_date = @partition_date
```

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py::test_query_partition_rows_selects_requested_columns_and_returns_dicts -v
```

Expected RED: missing method.

### 4B. GREEN

Add only:

```python
def query_partition_rows(
    self,
    dataset_id: str,
    table_id: str,
    partition_field: str,
    partition_date: date,
    columns: tuple[str, ...],
) -> list[dict]:
    ...
```

Require non-empty columns, reuse `_partition_job_config`, execute the partition-filtered select, return `[dict(row.items()) ...]`. Do not add a generic query builder.

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_bigquery_contract.py -v
```

Commit:

```powershell
git add pipeline/bigquery_adapter.py tests/pipeline/test_bigquery_adapter.py
git commit -m "feat: read partition rows from bigquery"
```

---

## Task 5 — One-batch classification persistence and retry recovery

**Files:** `pipeline/classification_load.py`, `tests/pipeline/test_classification_load.py`.

### 5A. Concrete stateful fake fixture

The test file must define the fixture, not merely refer to it:

```python
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
```

Define `StatefulBigQueryFake` with exactly the boundary methods F05 needs:
- `query_partition_rows`
- `query_source_row_ids`
- `append_rows`
- `query_partition_row_count`

Store output rows separately for accepted/quarantine. Add `fail_next_quarantine_append` to simulate the partial-write case.

### 5B. First-run/rerun RED

Test expected classification for the four concrete rows:

```text
raw=4
accepted=2
quarantine=2
first accepted insert=2
first quarantine insert=2
rerun accepted insert=0
rerun quarantine insert=0
partition accepted=2
partition quarantine=2
reconciled=True
```

Run RED:

```powershell
pytest tests/pipeline/test_classification_load.py::test_first_run_classifies_and_rerun_inserts_zero -v
```

Expected: missing `pipeline.classification_load`.

### 5C. Minimal GREEN orchestration

Create:

```python
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


def classify_and_load_batch(
    *,
    batch_date: date,
    bigquery_adapter,
    classified_at: datetime | None = None,
) -> ClassificationLoadSummary:
    ...
```

Flow:
1. query target raw partition using `RAW_TRANSACTION_COLUMNS`;
2. build Pandas DataFrame;
3. call `classify_transactions`;
4. assert in-memory reconciliation;
5. query accepted existing IDs -> reuse `anti_filter_existing` -> append unseen;
6. query quarantine existing IDs -> anti-filter -> append unseen;
7. query both persisted partition counts;
8. require `raw_rows == accepted_partition_rows + quarantine_partition_rows`;
9. return summary.

Serialize BigQuery JSON rows safely:
- `Decimal` -> decimal string;
- Pandas/native datetime/date -> ISO text acceptable to BigQuery JSON loading;
- `reason_codes` remains list;
- Pandas missing values -> `None`.

### 5D. Partial retry and reconciliation tests

Add test:
- first call persists accepted then fake throws on quarantine append;
- retry with a later `classified_at` must insert accepted=0, quarantine=2, reconcile true.

Add test where fake returns a deliberately incorrect persisted count and assert `ClassificationLoadError` containing `Persisted classification reconciliation failed`.

Run:

```powershell
pytest tests/pipeline/test_transaction_classification.py tests/pipeline/test_classification_load.py tests/pipeline/test_bigquery_adapter.py tests/pipeline/test_bigquery_contract.py -v
```

Commit:

```powershell
git add pipeline/classification_load.py tests/pipeline/test_classification_load.py
git commit -m "feat: persist classified transaction batches"
```

---

## Task 6 — Classification CLI

**Files:** `scripts/classify_transactions.py`, `tests/scripts/test_classify_transactions.py`.

### 6A. RED

Test `main(["--batch-date", "2025-07-22"])` with monkeypatched `run_classification`. Expected output keys, one per line in this order:

```text
batch_date
raw_rows
accepted_rows
quarantine_rows
accepted_inserted_rows
quarantine_inserted_rows
accepted_partition_rows
quarantine_partition_rows
reconciled
```

Run:

```powershell
pytest tests/scripts/test_classify_transactions.py -v
```

Expected: module import failure.

### 6B. GREEN

Implement:

```python
def parse_args(argv=None): ...
def run_classification(batch_date: date): ...
def main(argv=None) -> None: ...
```

`run_classification` loads settings, creates `BigQueryAdapter`, and calls `classify_and_load_batch`.

Run:

```powershell
pytest tests/scripts/test_classify_transactions.py tests/scripts/test_bootstrap_classification.py tests/pipeline/test_transaction_classification.py tests/pipeline/test_classification_load.py -v
```

Commit:

```powershell
git add scripts/classify_transactions.py tests/scripts/test_classify_transactions.py
git commit -m "feat: add transaction classification CLI"
```

---

## Task 7 — Local gate, live BigQuery acceptance, README, final review

### 7A. Fresh local verification before cloud writes

On `feat/05-pandas-dq-classification`:

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

Required: zero test failures; compile/config/diff/credential output quiet; clean working tree.

Build:

```powershell
docker compose --profile gcp build gcp-toolbox
```

### 7B. Bootstrap twice

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification
```

Fresh first run should create both F05 tables; second run should verify both. If first run already says `verified`, do not delete anything automatically.

### 7C. Pre-state for live acceptance

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

If either output partition is nonzero, stop and explicitly choose another fresh acceptance strategy; do not silently delete/rewrite.

### 7D. First classification + rerun

First run:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

Record measured counts; do not guess them. Must satisfy:

```text
raw_rows=8978
8978 = accepted_rows + quarantine_rows
accepted_inserted_rows = accepted_rows
quarantine_inserted_rows = quarantine_rows
reconciled=True
```

Run the same command again. Required:

```text
accepted_inserted_rows=0
quarantine_inserted_rows=0
accepted_partition_rows and quarantine_partition_rows unchanged
reconciled=True
```

### 7E. Typed/raw evidence

Query samples from both output tables. Demonstrate:
- accepted `transaction_dt` comes from BigQuery `DATETIME`;
- accepted amount is numeric;
- accepted currency is one of THB/USD/EUR;
- quarantine raw business strings are preserved;
- every sampled quarantine `reason_codes` array is non-empty.

Query reason distribution with `UNNEST(reason_codes)`. If live data contains `DUPLICATE_REPLAY`, verify one replay txn has exactly one accepted winner in the same batch. If it contains `DUPLICATE_CONFLICT`, verify one conflict txn has zero accepted rows in the same batch. If either case is absent live, state that and use unit-test evidence rather than fabricating a live example.

### 7F. README runbook

Add `## Feature 05: Pandas data-quality classification` documenting:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_classification

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

State explicitly:
- accepted is typed/canonical;
- quarantine preserves raw values + ordered reasons;
- duplicate scope is batch-local v1;
- base-invalid rows do not enter duplicate comparison;
- reruns append zero when unchanged;
- F05 stops before FX/F06 and Airflow/F07.

Commit:

```powershell
git add README.md
git commit -m "docs: add Feature 05 classification runbook"
```

### 7G. Final gate and Review Diff

Run the full local gate from 7A again after the README commit.

Then inspect:

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

Review for:
- business classification accidentally moved into BigQuery SQL;
- global cross-batch duplicate behavior;
- timezone assumption on `transaction_dt`;
- float money conversion;
- destructive partition replacement or `MERGE`;
- quarantine raw fields overwritten by canonical values;
- FX/F06 or Airflow/F07 feature creep.

Only after fresh live evidence, fresh full local gate, and Review Diff are clean should F05 enter the finishing/integration workflow.

## Final F05 Acceptance Checklist

```text
raw partition rows = accepted partition rows + quarantine partition rows
first live classification writes every classified row exactly once
unchanged rerun writes 0 accepted and 0 quarantine rows
accepted values are typed/canonical
quarantine preserves raw evidence with ordered reason_codes
unit tests prove deterministic replay/conflict semantics
partial retry test proves accepted-only partial write recovers
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
git status --short = empty
credential-file search = empty
```
