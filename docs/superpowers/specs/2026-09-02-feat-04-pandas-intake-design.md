# Feature 04 Pandas Intake + Raw Load Design

## Status

Approved in chat on 2026-09-02.

## Goal

Prove one logical-date intake path from the immutable GCS landing boundary into the existing BigQuery raw tables using Pandas, while preserving source evidence and making reruns insert zero additional rows.

Feature 04 is intentionally thin. It does not classify business-quality failures, resolve effective FX, convert currencies, wire Airflow tasks, or process the full 360-day corpus.

## Scope

For one requested `batch_date`:

1. Discover exactly five regional transaction objects in GCS.
2. Treat same-day FX as optional.
3. Download and checksum-verify source bytes against immutable GCS metadata.
4. Read CSV data with Pandas without silently converting dirty source strings.
5. Add stable source metadata and partition columns.
6. Query BigQuery only for already-loaded `source_row_id` values in the relevant partition.
7. Anti-filter already-loaded rows in Pandas.
8. Append only unseen rows to the existing raw tables.
9. Prove a second run inserts zero rows.

## Data Flow

```text
GCS immutable landing
      |
      +-- transactions/business_date=YYYY-MM-DD/*.csv.gz
      |        require exactly 5 expected regions
      |
      +-- fx/YYYY/MM/fx_YYYYMMDD.csv
               optional: missing => NO_NEW_RATE
      |
      v
checksum verification
      |
      v
Pandas read_csv
      |
      v
transport validation only
      |
      v
add source metadata + partition date
      |
      v
query existing source_row_id for target partition
      |
      v
Pandas anti-filter
      |
      v
BigQuery raw append
```

## Transaction Discovery Contract

The logical date must contain exactly these five regional files:

- `bkk`
- `central`
- `north`
- `northeast`
- `south`

Canonical object layout:

```text
transactions/business_date=YYYY-MM-DD/sales_{region}_YYYYMMDD.csv.gz
```

Discovery fails when any expected regional file is missing, when an unexpected transaction file is present in that date prefix, or when duplicate region identities are discovered.

The transaction CSV header must be exactly:

```text
txn,dtts,amount,currency
```

The gzip must be readable. Feature 04 does not parse or business-clean these four fields.

## Preserve Dirty Source Values

Pandas reads transaction source columns as strings with default NA interpretation disabled. Literal source values such as `N/A`, blanks, malformed timestamps, malformed amounts, and duplicate transaction IDs remain raw evidence for Feature 05.

Feature 04 must not silently:

- cast `txn` to a numeric type,
- parse `dtts`,
- parse `amount`,
- normalize `currency`,
- drop duplicate business rows,
- convert source `N/A` text into missing values.

The raw table is an evidence boundary, not a cleaned business table.

## Source Integrity Verification

For every discovered object:

1. Read GCS custom metadata key `bahtflow-source-sha256`.
2. Download the exact object bytes.
3. Compute SHA-256 locally from those bytes.
4. Fail if checksum metadata is missing or does not equal the computed checksum.

This reuses the immutable landing guarantee from Feature 02 and prevents the Pandas intake from trusting object names alone.

## Transaction Raw Metadata

Every transaction row receives:

```text
region
source_file
source_checksum
source_row_number
source_row_id
batch_date
ingested_at
```

Semantics:

- `region`: region derived from the canonical object name.
- `source_file`: canonical GCS object name, not only the basename.
- `source_checksum`: verified SHA-256 stored on the source object.
- `source_row_number`: 1-based data-row number, excluding the CSV header.
- `source_row_id`: deterministic transport identity.
- `batch_date`: requested logical date, stored as the BigQuery partition date.
- `ingested_at`: one UTC timestamp generated for the current load invocation; it is not part of idempotency identity.

## Deterministic `source_row_id`

Transaction source identity is:

```text
SHA256(source_file | source_checksum | source_row_number)
```

The same immutable file and row always produce the same ID. Business columns are deliberately excluded from deduplication semantics at this layer.

Consequences:

- Two identical business rows at different source row numbers remain two raw source rows.
- A rerun of the same immutable object generates the same IDs.
- Business duplicate handling remains deferred to Feature 05.

FX uses the same source identity rule.

## FX Intake Contract

Canonical same-day FX object:

```text
fx/YYYY/MM/fx_YYYYMMDD.csv
```

Same-day FX is optional. If the object does not exist, Feature 04 returns an explicit `NO_NEW_RATE` result and does not fail the transaction batch.

When present, the exact source header is:

```text
rate_date,currency,mid_rate,rate_unit,source_provider,source_url
```

Feature 04 preserves the source `rate_date` text as `rate_date_raw` and uses the requested/published object date as the required BigQuery `rate_date` partition column. Effective-date carry-forward is not performed here.

Every FX row also receives:

```text
source_file
source_checksum
source_row_number
source_row_id
rate_date
ingested_at
```

Feature 04 does not resolve the latest prior rate, add THB identity rates, or perform currency conversion. Those belong to Feature 06.

## Idempotency Decision

### Chosen approach: partition-scoped existing-ID query + Pandas anti-filter + append

Before appending each prepared DataFrame, query only the relevant BigQuery partition for existing `source_row_id` values.

Conceptually:

```text
prepared_df
    |
    +-- existing_ids = BigQuery query for target partition
    |
    +-- new_df = prepared_df[~prepared_df.source_row_id.isin(existing_ids)]
    |
    +-- append new_df only
```

First run inserts unseen rows. A second run over unchanged immutable GCS objects produces the same IDs, so `new_df` is empty and zero rows are appended.

This is a deliberate thin-v1 trade-off. It assumes one writer for a logical batch. A future version may use a staging table plus atomic BigQuery `MERGE` if concurrent writers become a real requirement.

## BigQuery Loading Boundary

Reuse the Feature 03 raw tables:

```text
bahtflow_raw.transactions
bahtflow_raw.fx_rates
```

The BigQuery adapter gains only narrow operations required by Feature 04:

- query existing `source_row_id` values for a partition,
- append prepared raw rows,
- optionally query partition row counts for verification.

Do not add a generic repository/service abstraction.

The append path may convert the prepared Pandas DataFrame to row records for the BigQuery client. Pandas owns intake shaping and anti-filter logic; BigQuery remains the durable raw warehouse boundary.

## Proposed Code Shape

```text
pipeline/pandas_intake.py
  - discover/validate expected transaction object names
  - verify source bytes/checksum
  - read transaction gzip CSV into Pandas
  - read optional FX CSV into Pandas
  - preserve source strings
  - add deterministic metadata
  - anti-filter existing source_row_id values

pipeline/bigquery_adapter.py
  - partition-scoped source_row_id query
  - append raw row records

scripts/load_raw_batch.py
  - one logical-date CLI
  - orchestrates transaction and optional FX intake using existing settings/adapters

tests/pipeline/test_pandas_intake.py
  - credential-free Pandas/source-contract/idempotency tests

tests/pipeline/test_bigquery_adapter.py
  - credential-free adapter tests for new narrow operations
```

`requirements-gcp.txt` adds a pinned Pandas dependency. No dbt, Airflow wiring, Spark, Great Expectations, or new cloud service is introduced.

## Error Behavior

Hard failures:

- transaction prefix does not resolve to exactly five canonical region files,
- transaction header mismatch,
- gzip/CSV cannot be read,
- missing source checksum metadata,
- downloaded checksum mismatch,
- present FX file has the wrong header,
- BigQuery append/query failure.

Non-failure condition:

- no same-day FX object => `NO_NEW_RATE`.

Business-invalid transaction values are not Feature 04 hard failures if the transport contract is intact; they remain raw evidence for Feature 05.

## CLI Output

A one-date command should report concise, non-secret evidence such as:

```text
batch_date=2025-07-22
tx_files=5
tx_source_rows=<N>
tx_inserted_rows=<N>
fx_status=LOADED|NO_NEW_RATE
fx_source_rows=<N-or-0>
fx_inserted_rows=<N-or-0>
```

On an unchanged rerun:

```text
tx_inserted_rows=0
fx_inserted_rows=0
```

If same-day FX is absent, its rerun remains `fx_status=NO_NEW_RATE` with zero FX source/inserted rows.

## Testing Strategy

Use TDD with credential-free unit tests before live GCP execution.

Minimum tests:

1. five canonical transaction objects accepted;
2. missing region rejected;
3. unexpected/duplicate region rejected;
4. exact transaction header enforced;
5. literal dirty values such as `N/A` remain strings;
6. source row numbers are deterministic and 1-based;
7. `source_row_id` is stable for the same file/checksum/row;
8. different source row numbers remain distinct even when business payloads match;
9. checksum mismatch fails;
10. missing same-day FX returns `NO_NEW_RATE`;
11. present FX header is enforced and raw date text preserved;
12. anti-filter removes IDs already present in the target partition;
13. BigQuery adapter queries only the requested partition and appends prepared rows.

## Live Acceptance Gate

Run exactly one logical date before any broader execution.

Required evidence:

```text
transaction files = 5/5
transaction source rows > 0
first transaction insert > 0
second transaction insert = 0

same-day FX = LOADED or NO_NEW_RATE
if LOADED:
  first FX insert > 0
  second FX insert = 0

raw transaction partition row count unchanged on rerun
raw FX partition row count unchanged on rerun when FX exists

dirty transaction source strings preserved in raw
pytest = 0 failures
py_compile = clean
docker compose config = clean
git diff --check = clean
credential-file search = empty
```

## Non-Goals

Feature 04 does not include:

- business-quality classification,
- accepted/quarantine tables,
- duplicate transaction business rules,
- parsing/standardizing transaction timestamps or amounts,
- effective FX carry-forward,
- currency conversion,
- fact or mart tables,
- Airflow task replacement,
- backfill execution,
- full 360-day load,
- dbt,
- streaming/concurrent-writer guarantees.

## Exit Condition

Feature 04 is complete when one logical-date transaction batch reaches BigQuery raw through Pandas with exactly five required source files, same-day FX behaves as optional, dirty source values are preserved, and an unchanged rerun inserts zero additional rows.