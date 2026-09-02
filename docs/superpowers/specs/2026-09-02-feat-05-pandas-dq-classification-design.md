# Feature 05: Pandas Data Quality Classification Design

## Status

Approved in chat on 2026-09-02.

## Goal

Classify one raw transaction batch into trusted accepted rows and auditable quarantine rows using plain Pandas business logic, then persist both outputs idempotently in BigQuery.

Feature 05 stops at classification. It does not resolve FX, convert currencies, build facts or marts, or wire the production Airflow DAG.

## Context

Feature 04 already loads one transaction batch idempotently into `bahtflow_raw.transactions` while preserving source business fields as raw strings and retaining immutable source lineage.

The existing Databricks version provides the same business intent: validate source rows, quarantine blocking defects, reject conflicting duplicates, and keep one exact replay. Feature 05 reimplements that intent in Pandas but deliberately makes the classification semantics more explicit and deterministic instead of porting the previous implementation byte-for-byte.

The v1 design remains thin:

- Pandas owns business validation and classification.
- BigQuery owns storage, partition-scoped reads, and append/idempotency mechanics.
- Duplicate semantics are batch-local in v1 so append-only persistence does not require historical retractions.
- A future v2 may add global cross-batch transaction uniqueness with an explicit reclassification strategy.

## Architecture

```text
bahtflow_raw.transactions
        |
        | read target batch_date
        v
validate_and_canonicalize_transactions()
        |
        |-- base-invalid rows --------------------+
        |                                         |
        `-- base-valid rows                       |
                |                                 |
                v                                 |
         classify_duplicates()                   |
          |                 |                     |
          |                 |                     |
          v                 v                     |
       accepted       duplicate quarantine        |
          |                 |                     |
          |                 +---------------------+
          v                                       v
bahtflow_analytics.                     bahtflow_ops.
transactions_accepted                   transactions_quarantine
```

Classification operates on exactly one `batch_date` at a time.

## BigQuery Outputs

Feature 05 adds exactly two output tables.

### `bahtflow_analytics.transactions_accepted`

Trusted, typed transaction representation for downstream FX enrichment.

Schema:

| Column | BigQuery type | Mode | Meaning |
| --- | --- | --- | --- |
| `txn` | STRING | REQUIRED | Canonical transaction business key |
| `transaction_dt` | DATETIME | REQUIRED | Parsed source `dtts` without invented timezone semantics |
| `amount` | NUMERIC | REQUIRED | Parsed monetary amount |
| `currency` | STRING | REQUIRED | Canonical `THB`, `USD`, or `EUR` |
| `region` | STRING | REQUIRED | Canonical lowercase region code |
| `source_file` | STRING | REQUIRED | Immutable source lineage |
| `source_checksum` | STRING | REQUIRED | Immutable source object checksum |
| `source_row_number` | INTEGER | REQUIRED | Row position in the immutable source file |
| `source_row_id` | STRING | REQUIRED | Stable idempotency key inherited from raw |
| `batch_date` | DATE | REQUIRED | Logical transaction batch date |
| `ingested_at` | TIMESTAMP | REQUIRED | Raw ingestion timestamp |
| `classified_at` | TIMESTAMP | REQUIRED | Classification timestamp |

Partition field: `batch_date`, DAY partitioning.

### `bahtflow_ops.transactions_quarantine`

Operational evidence for rows rejected by business classification. Raw business fields are preserved instead of partially typed values.

Schema:

| Column | BigQuery type | Mode | Meaning |
| --- | --- | --- | --- |
| `txn` | STRING | NULLABLE | Raw source value |
| `dtts` | STRING | NULLABLE | Raw source value |
| `amount` | STRING | NULLABLE | Raw source value |
| `currency` | STRING | NULLABLE | Raw source value |
| `region` | STRING | REQUIRED | Derived raw region code |
| `source_file` | STRING | REQUIRED | Immutable source lineage |
| `source_checksum` | STRING | REQUIRED | Immutable source object checksum |
| `source_row_number` | INTEGER | REQUIRED | Row position in source file |
| `source_row_id` | STRING | REQUIRED | Stable idempotency key inherited from raw |
| `batch_date` | DATE | REQUIRED | Logical transaction batch date |
| `ingested_at` | TIMESTAMP | REQUIRED | Raw ingestion timestamp |
| `reason_codes` | STRING | REPEATED | Ordered deterministic quarantine reasons |
| `quarantined_at` | TIMESTAMP | REQUIRED | Classification timestamp |

Partition field: `batch_date`, DAY partitioning.

## Classification Pipeline

Classification has two explicit phases.

### Phase 1: Base validation and canonicalization

Each raw row is evaluated independently. The classifier derives canonical values for valid rows while collecting every applicable base validation failure in deterministic order.

Canonicalization rules:

- `txn`: trim surrounding whitespace only. Do not uppercase, apply a regex, or otherwise change identifier semantics. Blank after trimming is invalid.
- `dtts`: parse exact source timestamp semantics into timezone-free `DATETIME`. No timezone is invented.
- `amount`: trim and parse with decimal semantics suitable for BigQuery `NUMERIC`. Do not use binary floating-point as the business representation.
- `currency`: trim and uppercase; only `THB`, `USD`, and `EUR` are valid.
- `region`: valid values are exactly `bkk`, `central`, `north`, `northeast`, and `south`; accepted rows retain these lowercase codes.
- `batch_date`: parsed transaction calendar date must equal the row's `batch_date`.

Amount zero is valid. Negative amounts are blocking defects.

Base validation reason order is fixed as:

```text
MISSING_TXN
INVALID_DTTS
DTTS_BATCH_DATE_MISMATCH
INVALID_AMOUNT
NEGATIVE_AMOUNT
INVALID_CURRENCY
INVALID_REGION
```

Reason semantics:

- `MISSING_TXN`: source `txn` is null, empty, or blank after trimming.
- `INVALID_DTTS`: source `dtts` is null, blank, or cannot be parsed according to the source datetime contract.
- `DTTS_BATCH_DATE_MISMATCH`: parsed transaction date differs from `batch_date`. This reason is evaluated only when datetime parsing succeeds.
- `INVALID_AMOUNT`: source amount is null, blank, or cannot be parsed as a decimal value compatible with the accepted numeric contract.
- `NEGATIVE_AMOUNT`: parsed amount is less than zero. It is evaluated only when amount parsing succeeds.
- `INVALID_CURRENCY`: canonicalized currency is blank or not one of `THB`, `USD`, or `EUR`.
- `INVALID_REGION`: region is not one of the five canonical source-domain codes.

A row may receive multiple base reasons. Any row with one or more base reasons is quarantined and does not participate in duplicate comparison.

### Phase 2: Batch-local duplicate classification

Duplicate logic applies only to base-valid rows in the target `batch_date`.

Business key:

```text
txn
```

Canonical duplicate payload:

```text
(transaction_dt, amount, currency, region)
```

Source lineage fields such as `source_file`, `source_row_id`, and `ingested_at` are not part of the business payload.

Rules:

1. If one `txn` has more than one distinct canonical payload in the target batch, every base-valid occurrence of that `txn` is quarantined with `DUPLICATE_CONFLICT`.
2. If one `txn` has exactly one canonical payload but multiple occurrences, exactly one row is accepted and every extra occurrence is quarantined with `DUPLICATE_REPLAY`.
3. The replay winner is deterministic: choose the lowest `(source_file ASC, source_row_number ASC)` lineage tuple.
4. A base-invalid row never causes an otherwise valid row with the same `txn` to become a duplicate conflict.

Duplicate reasons are appended after all base reasons in the global reason ordering:

```text
DUPLICATE_CONFLICT
DUPLICATE_REPLAY
```

Because duplicate classification runs only on base-valid rows, duplicate quarantine rows contain exactly one duplicate reason in v1.

## Why Duplicate Scope Is Batch-Local in v1

A global business key appears attractive, but it conflicts with Feature 05's append-only persistence contract. If a transaction accepted in an earlier batch later receives a conflicting payload, a truly global rule would require retracting the historical accepted row and reclassifying historical data.

Feature 05 intentionally avoids that state-retraction subsystem. Batch-local duplicate semantics keep classification deterministic, rerun-safe, and consistent with thin v1 scope. Global cross-batch uniqueness is deferred until the project has an explicit historical reclassification or `MERGE` strategy.

## Persistence and Idempotency

Persistence follows the proven Feature 04 single-writer v1 pattern.

For each output table:

1. Query existing `source_row_id` values for the target `batch_date` partition.
2. Anti-filter the classified Pandas frame against already persisted IDs.
3. Append only unseen rows with BigQuery `WRITE_APPEND`.
4. Never silently overwrite or replace the partition.

An unchanged rerun therefore inserts zero accepted rows and zero quarantine rows.

This idempotency protects operational retries, not rule-version migrations. If classification rules change later, historical reclassification must use an explicit migration/rebuild path rather than silently rerunning the ordinary F05 CLI.

## Partial Failure and Retry

Feature 05 does not attempt a distributed transaction across the two output tables.

If accepted rows are appended successfully and the quarantine append fails, the batch run fails. On retry:

- already written accepted `source_row_id` values are anti-filtered out;
- missing quarantine rows are appended;
- final reconciliation determines whether the partition is complete.

This keeps recovery deterministic without adding rollback or staging infrastructure to thin v1.

## Reconciliation

Before persistence, pure classification must satisfy:

```text
raw_batch_rows
= accepted_classified_rows + quarantine_classified_rows
```

After persistence, the target partition must satisfy:

```text
raw_partition_rows
= accepted_partition_rows + quarantine_partition_rows
```

A mismatch is a Feature 05 failure even if the BigQuery writes themselves succeeded.

Reconciliation is based on row counts because every raw source row has exactly one classification outcome in v1. `source_row_id` remains the row identity and idempotency key.

## Components

### `pipeline/transaction_classification.py`

Pure-ish Pandas business logic. It has no BigQuery or GCS dependency.

Expected public responsibilities:

- `validate_and_canonicalize_transactions(...)`
- `classify_duplicates(...)`
- `classify_transactions(...)`

The module should return accepted and quarantine frames through a small explicit result type rather than a generic framework abstraction.

### `pipeline/classification_load.py`

Feature-level orchestration for one batch:

- read one raw BigQuery partition;
- call Pandas classification;
- verify in-memory reconciliation;
- anti-filter accepted/quarantine by existing `source_row_id` values;
- append unseen rows;
- verify persisted partition reconciliation;
- return a concise classification summary.

### `pipeline/bigquery_contract.py`

Add the two table schemas and their `batch_date` partition contracts. Existing raw schemas remain unchanged.

### BigQuery adapter

Reuse the narrow existing adapter where possible. Add only focused read helpers needed to retrieve the target raw partition and to serialize the two output shapes. Do not introduce a generic repository/service layer.

### `scripts/bootstrap_classification.py`

Create or verify exactly the two Feature 05 output tables idempotently.

### `scripts/classify_transactions.py`

CLI entry point:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.classify_transactions --batch-date 2025-07-22
```

The CLI should print a concise summary containing at least:

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

## Testing Strategy

### Pure Pandas unit tests

Tests must cover:

- txn trimming and blank detection;
- datetime parsing and batch-date mismatch;
- decimal amount parsing;
- zero accepted and negative quarantined;
- currency trim/uppercase and unsupported currency rejection;
- region validation;
- multiple base reasons on one row with deterministic reason order;
- accepted typed output shape;
- quarantine preservation of raw business fields and lineage;
- exact duplicate replay winner selected by `(source_file, source_row_number)`;
- replay losers receive `DUPLICATE_REPLAY`;
- canonical payload conflicts quarantine every base-valid occurrence;
- base-invalid rows do not participate in duplicate comparison;
- row-count reconciliation.

### Persistence/orchestration tests

Use a stateful fake BigQuery boundary to prove:

- first run inserts accepted and quarantine rows;
- unchanged rerun inserts zero rows;
- partial retry after accepted-only persistence appends only the missing quarantine rows;
- persisted reconciliation mismatch fails the run.

### Contract/bootstrap tests

Tests must verify:

- exact accepted schema;
- exact quarantine schema, including repeated `reason_codes`;
- DAY partitioning by `batch_date`;
- bootstrap creates missing tables and verifies matching existing tables;
- bootstrap rejects incompatible existing schemas or partition contracts.

## Live Acceptance

Use the already loaded Feature 04 raw partition for `2025-07-22`.

Feature 05 live acceptance must prove:

```text
raw_rows = accepted_rows + quarantine_rows
```

First run:

```text
accepted_inserted_rows > 0
quarantine_inserted_rows >= 0
reconciled = True
```

Unchanged rerun:

```text
accepted_inserted_rows = 0
quarantine_inserted_rows = 0
reconciled = True
```

Live evidence should also demonstrate that:

- accepted rows use typed `DATETIME` and `NUMERIC` values;
- accepted currency is canonical and limited to `THB`, `USD`, `EUR`;
- exact replays have one deterministic accepted winner;
- duplicate conflicts have no accepted winner within the batch;
- quarantine preserves raw business fields and has non-empty `reason_codes`.

The exact accepted/quarantine row counts are evidence to measure during implementation and must not be guessed in the design.

## Final Feature 05 Gate

Feature 05 is complete only when all of the following are demonstrated with fresh evidence:

```text
raw partition rows = accepted partition rows + quarantine partition rows
first classification run writes the expected classified rows
unchanged rerun writes 0 accepted and 0 quarantine rows
accepted rows are typed/canonical
quarantine rows preserve raw evidence with ordered reason_codes
unit tests cover deterministic duplicate semantics
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
credential-file search = empty
```

## Non-Goals

Feature 05 does not implement:

- cross-batch/global transaction reclassification;
- FX rate lookup, carry-forward, or currency conversion;
- transaction fact tables;
- marts or dashboards;
- Airflow production task wiring;
- dbt;
- Great Expectations;
- Spark/PySpark;
- `MERGE`, staging-table, or concurrent-writer semantics;
- automatic historical rebuild when classification rules change.

Those remain later-feature or v2 concerns.