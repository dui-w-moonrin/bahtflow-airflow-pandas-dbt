# Feature 06: FX + Currency Fact Design

## Status

Approved in chat on 2026-09-02.

## Goal

Feature 06 turns Feature 05 accepted transactions into an analytics fact by resolving the latest published FX snapshot at or before each logical batch date and converting every accepted transaction into THB, USD, and EUR amounts.

The feature preserves the v1 Pandas-first boundary:

- Pandas owns FX validation, effective-date resolution, conversion, and derived fields.
- BigQuery owns partition reads, append-only persistence, partition counts, and idempotency mechanics only.
- No future FX, fallback constant, silent default, or SQL business transformation is allowed.
- Every fact row retains the exact FX date and rates used.
- Feature 05 remains the sole owner of transaction-quality quarantine decisions.

## Scope

Feature 06 includes:

1. Resolve one effective USD/EUR FX snapshot for a logical batch date.
2. Validate the selected raw FX snapshot.
3. Convert accepted THB/USD/EUR transactions into all three currencies with `Decimal` arithmetic.
4. Persist one fact row per accepted transaction.
5. Prove batch-local idempotency, partial-write recovery, and persisted count reconciliation.
6. Produce live acceptance evidence for the already-proven Feature 05 batch.

Feature 06 does not include:

- Airflow DAG wiring or historical backfill orchestration; Feature 07 owns that.
- Marts, report rounding, publication, or broader recovery gates; Feature 08 owns those.
- dbt, Spark, Kafka, streaming, or additional transformation layers.
- A persisted `fx_effective_daily` table.
- A second transaction quarantine stage.

## Architecture Decision

Use a batch-local Pandas resolver and persist only the transaction fact.

```text
bahtflow_analytics.transactions_accepted
        WHERE batch_date = D
                    |
                    v
          Pandas accepted batch
                    |
                    |
bahtflow_raw.fx_rates
WHERE rate_date <= D
        |
        v
find latest published rate_date <= D
        |
        v
validate selected USD/EUR snapshot
        |
        v
EffectiveFxSnapshot
  fx_rate_date
  usd_thb_rate
  eur_thb_rate
  is_carried_forward
  staleness_days
        |
        v
build_transaction_fact()
        |
        v
bahtflow_analytics.fct_transactions
        partitioned by batch_date
```

Feature 06 assumes the raw FX history required for the target date is already present in `bahtflow_raw.fx_rates`. Feature 07 owns orchestration/backfill guarantees around FX availability and logical-date ordering.

## Inputs

### Accepted transactions

Read only the target partition from:

```text
bahtflow_analytics.transactions_accepted
```

Authoritative fields are:

```text
txn
transaction_dt
amount
currency
region
source_file
source_checksum
source_row_number
source_row_id
batch_date
ingested_at
classified_at
```

`amount` is BigQuery `NUMERIC`, represented as `Decimal`. Currency is canonical `THB`, `USD`, or `EUR`.

### Raw FX

Read raw FX rows where:

```text
rate_date <= batch_date
```

The raw contract is:

```text
rate_date_raw
currency
mid_rate
rate_unit
source_provider
source_url
source_file
source_checksum
source_row_number
source_row_id
rate_date
ingested_at
```

For v1 conversion semantics, `mid_rate` is THB per one unit of foreign currency. `rate_unit`, provider, URL, and raw-source lineage remain available in the raw table; the fact copies the effective date and numeric rates actually used.

## Effective FX Resolution

For logical batch date `D`:

1. Query raw FX rows with `rate_date <= D`.
2. If no such rows exist, fail the batch.
3. Determine the latest published `rate_date` present in those rows.
4. Select only rows from that latest date.
5. Validate that date as one complete USD/EUR snapshot.
6. If the latest published snapshot is malformed or incomplete, fail. Do not silently fall back to an older valid snapshot.

This intentionally means **latest published snapshot**, not **latest valid snapshot**.

### Required selected-snapshot shape

The selected snapshot must contain exactly:

```text
USD: exactly 1 row
EUR: exactly 1 row
```

Fail if:

- USD or EUR is missing;
- either appears more than once;
- an unsupported extra currency is present;
- `rate_date_raw` is blank, unparseable, or differs from canonical `rate_date`;
- `mid_rate` is blank, non-numeric, non-finite, zero, negative, or not BigQuery-`NUMERIC` representable;
- the selected rows do not share the same canonical `rate_date`.

There is no maximum staleness threshold in v1.

### Effective snapshot value

The resolver returns:

```text
fx_rate_date
usd_thb_rate
eur_thb_rate
is_carried_forward
staleness_days
```

with:

```text
is_carried_forward = fx_rate_date < batch_date
staleness_days = (batch_date - fx_rate_date).days
```

`staleness_days` must be an integer >= 0. Same-day FX has `False/0`; prior FX has `True/>0`.

## Currency Conversion Semantics

All money and FX arithmetic uses Python `Decimal`. `float` is forbidden in the conversion path.

For accepted amount `A`:

```text
THB: amount_thb = A
USD: amount_thb = A * usd_thb_rate
EUR: amount_thb = A * eur_thb_rate
```

Then:

```text
amount_usd = amount_thb / usd_thb_rate
amount_eur = amount_thb / eur_thb_rate
```

No direct USD/EUR cross-rate is required.

### Precision policy

The fact stores detailed values rather than report-rounded two-decimal values.

Before persistence, every derived monetary value and stored FX rate must be normalized to a BigQuery-`NUMERIC`-representable `Decimal`:

- maximum fractional scale: 9;
- maximum integer digits: 29;
- when scale reduction is required, use deterministic `ROUND_HALF_EVEN` to 9 fractional digits;
- non-finite or non-representable results fail the batch.

This is storage normalization, not presentation rounding. Feature 08 may round mart/report output separately.

## Fact Table Contract

Create:

```text
bahtflow_analytics.fct_transactions
```

with DAY partitioning on `batch_date`.

Each accepted transaction produces exactly one fact row.

```text
txn                 STRING     REQUIRED
transaction_dt      DATETIME   REQUIRED
amount              NUMERIC    REQUIRED
currency            STRING     REQUIRED
region              STRING     REQUIRED
source_file         STRING     REQUIRED
source_checksum     STRING     REQUIRED
source_row_number   INTEGER    REQUIRED
source_row_id       STRING     REQUIRED
batch_date          DATE       REQUIRED
ingested_at         TIMESTAMP  REQUIRED
classified_at       TIMESTAMP  REQUIRED
amount_thb          NUMERIC    REQUIRED
amount_usd          NUMERIC    REQUIRED
amount_eur          NUMERIC    REQUIRED
fx_rate_date        DATE       REQUIRED
usd_thb_rate        NUMERIC    REQUIRED
eur_thb_rate        NUMERIC    REQUIRED
is_carried_forward  BOOLEAN    REQUIRED
staleness_days      INTEGER    REQUIRED
fact_created_at     TIMESTAMP  REQUIRED
```

Original `amount` and `currency` remain unchanged alongside the converted analytical amounts.

## Idempotency

Reuse `source_row_id` as the fact idempotency key.

For one target partition:

1. Read accepted rows for `batch_date = D`.
2. Resolve the effective FX snapshot.
3. Build the complete in-memory fact DataFrame.
4. Query existing fact `source_row_id` values only for partition `D`.
5. Pandas anti-filters existing IDs.
6. Append unseen rows with `WRITE_APPEND`.

No `MERGE`, truncate, replace, or delete is used. Single-writer execution remains the v1 assumption.

An unchanged rerun must report:

```text
fact_inserted_rows = 0
```

### Source/reference changes after persistence

Feature 06 is append-only. It does not silently rewrite an already-persisted fact row if transformation rules or immutable source/reference inputs are later changed outside the v1 contract. Such a change requires an explicit rebuild/migration rather than an idempotent rerun.

## Partial-Write Recovery

If fact rows were persisted and a later workflow step fails, retrying the same logical date must:

- rebuild the deterministic fact batch;
- query existing fact IDs;
- skip already-persisted rows;
- append only missing rows;
- reconcile the final partition count.

Feature 06 does not roll back valid rows already written.

## Reconciliation

Before persistence:

```text
accepted_rows = generated_fact_rows
accepted source_row_id set = generated fact source_row_id set
```

After persistence:

```text
accepted_partition_rows = fact_partition_rows
```

Persisted count mismatch fails the batch. Set equality is mandatory in transformation tests and may also be inspected during live acceptance without creating a permanent reconciliation table.

## Failure Semantics

Fail the batch when a complete trustworthy fact cannot be produced, including:

- no raw FX at or before the batch date;
- malformed/incomplete latest published USD/EUR snapshot;
- invalid, zero, negative, non-finite, or non-representable FX rate;
- future FX would be required;
- an impossible post-F05 accepted transaction state prevents conversion;
- a converted value is not BigQuery-`NUMERIC` representable;
- generated fact count differs from accepted count;
- persisted fact partition count differs from accepted partition count.

These failures do not create new transaction quarantine rows. Feature 05 owns transaction-quality classification; Feature 06 treats them as reference-data or transformation failures.

## Component Boundaries

### FX resolution module

Owns raw FX selection/validation and `EffectiveFxSnapshot`. It performs no persistence.

### Currency fact transformation module

Owns Decimal formulas, NUMERIC normalization, fact projection, source-row identity preservation, and in-memory reconciliation.

### Fact load orchestration module

Owns target-partition reads, raw FX history read, anti-filtering, append-only fact write, persisted count reconciliation, and run summary.

### BigQuery contract/adapter extensions

Own fact schema/bootstrap and narrow warehouse mechanics needed by the modules above. BigQuery SQL must not resolve effective FX or perform currency conversion.

## CLI and Bootstrap

Follow existing Feature 05 script conventions:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_currency_fact

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.build_currency_fact --batch-date 2025-07-22
```

The one-batch summary exposes at least:

```text
batch_date
accepted_rows
fx_rate_date
is_carried_forward
staleness_days
fact_rows
fact_inserted_rows
fact_partition_rows
reconciled
```

Exact live rates and converted values are measured from the corpus, never guessed in documentation.

## Testing Strategy

### FX resolver

Cover:

- same-day FX => carry-forward false, staleness 0;
- prior FX => carry-forward true, positive staleness;
- future FX never selected;
- no prior publication fails;
- missing USD/EUR fails;
- duplicate USD/EUR fails;
- unsupported extra currency fails;
- invalid/non-finite/zero/negative rate fails;
- `rate_date_raw` mismatch fails;
- malformed latest publication does not fall back to an older valid date.

### Conversion

Cover:

- THB identity conversion;
- USD and EUR conversion to all three output currencies;
- Decimal-only arithmetic;
- deterministic scale-9 normalization;
- NUMERIC overflow failure;
- original amount/currency preserved;
- fact count equals accepted count;
- source-row identity set preserved exactly.

### Persistence

Cover:

- first run writes unseen rows;
- unchanged rerun inserts zero;
- partial-write retry appends only missing rows;
- persisted count mismatch fails;
- fact bootstrap is create/verify rerun-safe.

## Live Acceptance

Use the already-proven Feature 05 batch `2025-07-22` as the primary acceptance date.

Feature 05 established:

```text
accepted_rows = 8803
```

Feature 06 must prove:

```text
fact_rows = 8803
accepted_partition_rows = fact_partition_rows
```

The first live run inserts the measured fact rows. The unchanged rerun must report:

```text
fact_inserted_rows = 0
```

Inspect at least one THB, one USD, and one EUR fact row and show:

```text
original amount/currency
amount_thb
amount_usd
amount_eur
fx_rate_date
usd_thb_rate
eur_thb_rate
is_carried_forward
staleness_days
```

Carry-forward behavior is mandatory in unit tests. A second live carry-forward demonstration is optional in Feature 06: use it only if the current raw/accepted acceptance state already contains a suitable logical date. Otherwise Feature 07 backfill is the natural place to demonstrate historical carry-forward end to end.

## Acceptance Criteria

Feature 06 is complete only when evidence shows:

1. The fact table has the exact schema and DAY partition on `batch_date`.
2. The resolver never uses future FX.
3. The latest published prior/same-day snapshot is validated as an exact USD/EUR pair.
4. An invalid latest publication fails rather than falling back.
5. Conversion uses `Decimal` and BigQuery-`NUMERIC`-representable values.
6. Every fact row contains effective FX date/rates/staleness lineage.
7. One accepted row produces one fact row with the same `source_row_id`.
8. Primary live acceptance for `2025-07-22` proves 8,803 accepted and 8,803 fact rows.
9. An unchanged rerun inserts zero fact rows.
10. Partial-write retry is proven by tests.
11. Persisted count mismatch fails.
12. No business conversion logic is implemented in BigQuery SQL.
13. No Feature 07 Airflow or Feature 08 mart scope creeps into Feature 06.

## Deferred Work

Feature 07 wraps the proven Feature 04-06 Python functions into the Airflow logical-date/backfill path and guarantees the FX availability/order needed by historical runs.

Feature 08 adds marts, broader reconciliation, publication gating, recovery demonstrations, and presentation rounding.
