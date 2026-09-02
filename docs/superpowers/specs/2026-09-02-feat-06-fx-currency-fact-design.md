# Feature 06: FX + Currency Fact Design

## Status

Approved in chat on 2026-09-02.

## Goal

Feature 06 turns Feature 05 accepted transactions into an analytics fact by resolving the latest published FX snapshot at or before each logical batch date and converting every accepted transaction into THB, USD, and EUR amounts.

The feature must preserve auditability and the v1 Pandas-first architecture:

- Pandas owns FX validation, effective-date resolution, currency conversion, and derived fields.
- BigQuery provides partition reads, append-only persistence, partition counts, and idempotency mechanics only.
- No future FX rate, fallback constant, silent default, or SQL business transformation is allowed.
- Every fact row retains the exact FX date and rates used for conversion.
- Feature 05 remains the sole owner of transaction-quality quarantine decisions.

## Scope

Feature 06 includes:

1. Resolving one effective USD/EUR FX snapshot for a logical transaction batch date.
2. Validating the selected raw FX snapshot.
3. Converting accepted THB/USD/EUR transactions into all three currencies with `Decimal` arithmetic.
4. Persisting one fact row per accepted transaction into BigQuery.
5. Proving batch-local idempotency, partial-write recovery, and persisted count reconciliation.
6. Live acceptance evidence for same-day and carry-forward FX behavior where the existing source corpus provides those cases.

Feature 06 does not include:

- Airflow DAG wiring or historical backfill orchestration; Feature 07 owns that.
- Marts, reporting rounding, public publishing, or recovery gates; Feature 08 owns those.
- dbt, Spark, Kafka, streaming, or additional warehouse transformation layers.
- A persisted `fx_effective_daily` table. Effective FX remains a Pandas runtime object in v1.
- A second quarantine stage for accepted transactions.

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

This keeps Feature 06 thin. It avoids an extra effective-FX warehouse table and keeps business logic out of BigQuery SQL.

## Inputs

### Accepted transactions

Read only the target partition from:

```text
bahtflow_analytics.transactions_accepted
```

The Feature 05 accepted schema remains authoritative for source fields:

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

Accepted transaction `amount` is BigQuery `NUMERIC`, represented as `Decimal` in Pandas/Python. Accepted currencies are canonical `THB`, `USD`, or `EUR`.

### Raw FX

Read raw FX rows where:

```text
rate_date <= batch_date
```

The raw FX contract remains:

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

`mid_rate` means THB per one unit of foreign currency.

## Effective FX Resolution

### Selection rule

For logical batch date `D`:

1. Query raw FX rows with `rate_date <= D`.
2. If no such rows exist, fail the batch.
3. Determine the latest published `rate_date` present in those rows.
4. Select only rows from that latest date.
5. Validate that selected date as one complete snapshot.
6. If the selected latest snapshot is malformed or incomplete, fail the batch. Do not silently fall back to an older valid date.

This rule deliberately distinguishes "latest published snapshot" from "latest valid snapshot". A malformed newest publication is a source/reference-data defect and must remain visible.

### Required snapshot shape

The selected snapshot must contain exactly:

```text
USD: exactly 1 row
EUR: exactly 1 row
```

The snapshot fails if:

- USD is missing.
- EUR is missing.
- either currency appears more than once.
- another unsupported currency appears in the selected snapshot.
- `rate_date_raw` is blank, unparseable, or does not equal canonical `rate_date`.
- `mid_rate` is blank, non-numeric, non-finite, zero, negative, or outside BigQuery `NUMERIC` representability.
- USD and EUR do not share the same canonical `rate_date`.

The feature does not impose a maximum staleness threshold in v1.

### Effective snapshot fields

The resolver returns one immutable runtime value with:

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

`staleness_days` must be an integer >= 0.

A same-day rate has:

```text
is_carried_forward = false
staleness_days = 0
```

A prior published rate has:

```text
is_carried_forward = true
staleness_days > 0
```

## Currency Conversion Semantics

All money and FX arithmetic uses Python `Decimal`. `float` is not permitted in the conversion path.

For an accepted transaction with original amount `A`:

```text
if currency == THB:
    amount_thb = A

if currency == USD:
    amount_thb = A * usd_thb_rate

if currency == EUR:
    amount_thb = A * eur_thb_rate
```

All three analytical amounts are then available through the THB bridge:

```text
amount_usd = amount_thb / usd_thb_rate
amount_eur = amount_thb / eur_thb_rate
```

No direct USD/EUR source cross-rate is required.

### Precision policy

The fact stores detailed values, not presentation-rounded two-decimal values.

BigQuery `NUMERIC` supports at most 9 fractional decimal places. Every derived monetary value and stored FX rate must therefore be normalized deterministically to a BigQuery-`NUMERIC`-representable `Decimal` before persistence.

The persistence normalization policy is:

- maximum scale: 9 fractional digits;
- deterministic rounding: `ROUND_HALF_EVEN` when reduction to scale 9 is required;
- maximum integer digits: 29;
- non-finite or non-representable results fail the batch.

This normalization is a storage-representation rule, not business/reporting rounding. Feature 08 may round displayed mart metrics to two decimals where appropriate.

## Fact Table Contract

Create:

```text
bahtflow_analytics.fct_transactions
```

partitioned by:

```text
batch_date
```

Each accepted transaction produces exactly one fact row.

### Schema

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
ingested_at          TIMESTAMP  REQUIRED
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

The original accepted `amount` and `currency` remain unchanged so the fact preserves the source transaction amount alongside converted analytical amounts.

## Idempotency

Feature 06 reuses the stable Feature 04/05 source-row identity:

```text
source_row_id
```

For one target batch partition:

1. Read accepted rows for `batch_date = D`.
2. Resolve one effective FX snapshot for `D`.
3. Build the complete in-memory fact DataFrame.
4. Query existing fact `source_row_id` values only for partition `D`.
5. Pandas anti-filters already persisted fact rows.
6. Append only unseen rows with `WRITE_APPEND`.

No `MERGE`, destructive replace, truncate, or delete is used in v1.

Single-writer execution remains the v1 assumption.

An unchanged rerun must report:

```text
fact_inserted_rows = 0
```

while the persisted fact partition count remains unchanged.

## Partial-Write Recovery

If a fact append succeeds partially at the workflow level and a later operation fails, retrying the same logical date must:

- rebuild the deterministic fact batch from accepted + effective FX;
- query existing fact `source_row_id` values;
- skip already persisted rows;
- append only missing rows;
- reconcile final partition counts.

Feature 06 does not roll back or delete already-written valid fact rows.

## Reconciliation Invariants

Before persistence:

```text
accepted_rows = generated_fact_rows
```

After persistence:

```text
accepted_partition_rows = fact_partition_rows
```

The stronger identity invariant is:

```text
accepted source_row_id set = fact source_row_id set
```

The implementation should verify count reconciliation on every run. Set equality is tested at the transformation/unit level and may be used in live acceptance evidence without requiring a separate permanent reconciliation table.

Any persisted count mismatch fails the batch.

## Failure Semantics

Feature 06 fails the batch when reference data or conversion correctness is not sufficient to produce a complete trustworthy fact.

Fail conditions include:

- no FX rows at or before the batch date;
- incomplete or malformed latest published USD/EUR snapshot;
- invalid, zero, negative, non-finite, or non-representable FX rate;
- future FX would be required;
- accepted transaction contains an impossible post-F05 state that prevents conversion;
- derived converted amount cannot be represented safely as BigQuery `NUMERIC`;
- generated fact row count differs from accepted row count;
- persisted fact partition count differs from accepted partition count.

These failures do not create new transaction quarantine records. Feature 05 owns transaction-quality classification; Feature 06 treats these cases as reference-data or transformation failures.

## Component Boundaries

The implementation should keep responsibilities narrow.

### FX resolution module

Owns:

- raw FX validation for the selected snapshot;
- latest-prior publication selection;
- `EffectiveFxSnapshot` creation;
- no BigQuery persistence.

### Currency fact transformation module

Owns:

- accepted transaction validation assumptions needed for conversion;
- Decimal conversion formulas;
- BigQuery NUMERIC normalization;
- fact DataFrame projection;
- in-memory row-count reconciliation.

### Fact load orchestration module

Owns:

- target-partition reads;
- raw FX history read for `rate_date <= batch_date`;
- fact anti-filtering;
- append-only write;
- persisted row-count reconciliation;
- run summary.

### BigQuery contract/adapter extensions

Own only warehouse mechanics needed by the above modules:

- fact schema/bootstrap;
- querying FX rows up to a target date;
- existing `source_row_id` lookup;
- append rows;
- partition counts.

BigQuery SQL must not implement effective FX resolution or currency-conversion business rules.

## CLI and Bootstrap

Provide a narrow bootstrap command for the fact table and a one-batch execution command, following existing Feature 05 script conventions.

Expected execution shape:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_currency_fact

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.build_currency_fact --batch-date 2025-07-22
```

The one-batch CLI summary should expose at least:

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

Exact live rates and derived amount values must be measured from the source corpus rather than documented as guessed constants.

## Testing Strategy

### FX resolver unit tests

Cover:

- same-day FX resolves with `is_carried_forward=False` and staleness 0;
- earlier FX resolves with `is_carried_forward=True` and positive staleness;
- future FX is never selected;
- no prior publication fails;
- latest publication missing USD fails;
- latest publication missing EUR fails;
- duplicate USD or EUR fails;
- unsupported extra currency in selected snapshot fails;
- blank/non-numeric/non-finite/zero/negative rate fails;
- `rate_date_raw` mismatch fails;
- malformed latest publication does not silently fall back to an older valid date.

### Conversion unit tests

Cover:

- THB identity conversion;
- USD to THB, USD, and EUR;
- EUR to THB, USD, and EUR;
- Decimal-only arithmetic;
- deterministic scale-9 normalization;
- integer/scale overflow failure;
- original accepted amount/currency preserved;
- generated fact row count equals accepted row count;
- source-row identity preserved exactly.

### Persistence tests

Cover:

- first run writes unseen fact rows;
- unchanged rerun inserts zero rows;
- partial-write retry appends only missing fact rows;
- persisted count mismatch fails;
- fact bootstrap is create/verify rerun-safe.

## Live Acceptance

Use the already-proven Feature 05 batch `2025-07-22` as the primary Feature 06 acceptance date.

Feature 05 established:

```text
accepted_rows = 8803
```

Feature 06 must measure and prove for that same batch:

```text
fact_rows = 8803
accepted_partition_rows = fact_partition_rows
```

The first live run must insert the measured fact rows. The unchanged rerun must report:

```text
fact_inserted_rows = 0
```

Live evidence must inspect at least one THB, one USD, and one EUR transaction and show:

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

To prove carry-forward behavior, select an existing transaction batch date from the source corpus that has no same-day FX publication but does have a prior published complete USD/EUR snapshot. Do not fabricate a new production acceptance source case solely for the demonstration. If the corpus unexpectedly contains no such batch, unit-test evidence remains the fallback proof.

## Acceptance Criteria

Feature 06 is complete only when all of the following are evidenced:

1. The fact table exists with the exact schema and `batch_date` DAY partitioning.
2. The FX resolver never uses a future rate.
3. The latest published prior/same-day FX snapshot is validated as an exact USD/EUR pair.
4. Invalid latest FX publication fails rather than silently falling back.
5. THB/USD/EUR conversion uses `Decimal` and stores BigQuery-`NUMERIC`-representable values.
6. Every fact row contains FX rate/date/staleness lineage.
7. One accepted transaction produces one fact row.
8. Primary live acceptance for `2025-07-22` proves `8803` accepted rows and `8803` fact rows.
9. An unchanged rerun inserts zero fact rows.
10. Partial-write retry behavior is proven by tests.
11. Persisted count mismatch fails.
12. No business conversion logic is implemented in BigQuery SQL.
13. No Feature 07 Airflow orchestration or Feature 08 mart scope creeps into this feature.

## Deferred Work

Feature 07 will wrap the proven Feature 04-06 Python functions into the Airflow logical-date path and backfill flow.

Feature 08 will add marts, broader reconciliation, publication gating, and recovery demonstrations. Presentation rounding belongs there rather than in this fact.
