# Feature 07 — Airflow E2E + Backfill Design

## Status

Approved in chat on 2026-09-02.

## Goal

Feature 07 turns the already-proven F04, F05, and F06 Python services into one production-minded Airflow 3 orchestration path. The same DAG code path must serve both normal daily scheduling and explicit historical backfill, using Airflow logical date as the single batch-date source of truth.

F07 ends when one logical date can flow through raw ingestion, transaction classification, FX ingestion/resolution, and persisted currency fact creation successfully through Airflow. Marts, publication, final quality gating, persisted run-audit state, and recovery/publish semantics remain F08 responsibilities.

## Architecture Decisions

### 1. Custom Airflow runtime

Airflow services will run from a custom image based on:

```text
apache/airflow:3.3.1-python3.12
```

The image installs the existing `requirements-gcp.txt` dependencies needed by the Pandas/GCS/BigQuery pipeline. The Compose Airflow runtime mounts the repository at `/opt/bahtflow`, makes that repository importable by Python tasks, receives the existing BahtFlow GCP environment variables, and mounts local ADC credentials read-only at the same container credential path used by the GCP toolbox.

Airflow tasks call Python services directly. They do not invoke a sibling Docker container, use the Docker socket, or shell out to pipeline CLI wrappers.

### 2. TaskFlow API

The production DAG uses Airflow TaskFlow (`@dag` and `@task`) with thin wrappers around existing pipeline services. Business transformation logic stays in `pipeline/`; the DAG contains orchestration only.

The intended dependency graph is:

```text
resolve_batch_date
        |
     preflight
     /      \
load_tx_raw  load_fx_raw
     |          |
 classify       |
     \          /
   build_currency_fact
          |
        finish
```

The DAG is daily, uses `catchup=False`, and uses `max_active_runs=1` for v1. Serial active runs are intentional: they preserve deterministic historical ordering and the single-writer assumptions already used by F04-F06, while making sparse FX carry-forward behavior straightforward to prove during backfill.

### 3. Logical date is the only batch-date source of truth

Every task derives `batch_date` from the Airflow logical date in the `Asia/Bangkok` timezone. The DAG exposes no independent `batch_date` override parameter.

Scheduled runs, manual historical execution, and Airflow backfill therefore differ only in how Airflow creates the logical date. F04-F06 services always receive the same `datetime.date` contract.

This prevents a run from having one Airflow logical date and a conflicting application batch date.

### 4. Explicit backfill, not automatic catchup

Normal scheduling keeps `catchup=False`. Historical processing is deliberate and uses Airflow's backfill mechanism over a requested date range.

F07 does not create a second backfill script or a special historical code path. Backfill invokes the same DAG tasks that a normal daily run invokes.

### 5. F07 stops at the persisted fact

The terminal data product for F07 is `bahtflow_analytics.fct_transactions` for the logical batch date.

The following remain out of scope until F08/F09:

- marts
- publication tables/views or publish switching
- final quality gate
- persisted run-audit lifecycle
- rollback behavior
- end-to-end recovery/publish proof
- full 360-day measured execution
- CI polish
- dbt

## Component Boundaries

### Airflow DAG

`airflow/dags/bahtflow_daily.py` becomes the real orchestration DAG.

Its responsibilities are limited to:

- deriving the batch date from Airflow logical date
- ordering tasks
- constructing runtime adapters/settings
- invoking pipeline services
- returning small task summaries suitable for XCom
- allowing task exceptions to fail the Airflow task/run

The DAG must not contain Pandas classification, FX resolution, conversion, BigQuery row-building, or source-validation business rules.

### Raw load split

F04 currently exposes `load_raw_batch()`, which performs transaction and same-day FX raw ingestion in one function. F07 introduces two narrower orchestration boundaries:

```text
load_transaction_raw_batch(...)
load_fx_raw_batch(...)
```

`load_raw_batch()` remains supported and delegates to those two narrower services so the existing F04 CLI/tests remain backward-compatible.

Transaction ingestion and FX ingestion become separate Airflow tasks.

Transaction ingestion requires exactly the five expected regional transaction objects for the batch date and preserves raw source values and source-row identity exactly as F04 already specifies.

FX ingestion checks only the same-day sparse FX object. If it is absent, the task succeeds with `NO_NEW_RATE`. If an object exists, it must pass the existing FX intake validation before being appended idempotently.

### Transaction classification

The classification task calls the existing F05 persistence service:

```text
pipeline.classification_load.classify_and_load_batch()
```

It reads the current raw transaction partition, writes accepted and quarantine partitions idempotently, and requires persisted reconciliation:

```text
raw partition rows = accepted partition rows + quarantine partition rows
```

### Currency fact

The fact task calls the existing F06 persistence service:

```text
pipeline.currency_fact_load.build_and_load_currency_fact()
```

It reads accepted transactions for `batch_date` and all raw FX history where `rate_date <= batch_date`, resolves the effective complete USD/EUR snapshot, builds currency conversions in Pandas, persists unseen fact rows, and requires:

```text
accepted partition rows = fact partition rows
```

The fact task does not receive a rate object from `load_fx_raw` through XCom. The Airflow dependency edge guarantees that same-day FX ingestion has finished first; F06 then resolves the effective snapshot from persisted BigQuery raw history.

## Preflight

F07 adds a read-only `preflight` pipeline boundary. Preflight validates runtime readiness before any batch writes.

It must validate:

1. Required environment settings are present using the existing `load_gcp_settings()` contract:
   - `BAHTFLOW_GCP_PROJECT`
   - `BAHTFLOW_GCS_BUCKET`
   - `BAHTFLOW_GCP_LOCATION`
   - `BAHTFLOW_RUNTIME_SERVICE_ACCOUNT`
2. ADC-backed GCS access succeeds and the configured landing bucket exists in the configured location. Existing `GcsAdapter.ensure_bucket(..., create_if_missing=False)` behavior may be reused because it is non-creating in this mode.
3. BigQuery access succeeds.
4. Required BigQuery datasets exist in the configured location.
5. Required F04-F06 tables exist with the expected schemas and DAY partition fields:
   - `bahtflow_raw.transactions` on `batch_date`
   - `bahtflow_raw.fx_rates` on `rate_date`
   - `bahtflow_analytics.transactions_accepted` on `batch_date`
   - `bahtflow_ops.transactions_quarantine` on `batch_date`
   - `bahtflow_analytics.fct_transactions` on `batch_date`

Preflight must never create or alter datasets/tables. BigQuery bootstrap scripts remain explicit setup/deployment commands outside the DAG. The BigQuery adapter may receive narrow read-only validation methods if needed; existing creating `ensure_*` methods must not be used in preflight when absence would create infrastructure.

## Task Data Exchange

Airflow's metadata database is not a data-processing transport. No Pandas `DataFrame`, source file bytes, accepted rows, quarantine rows, or fact rows may be passed through XCom.

Each task persists its durable state to BigQuery and downstream tasks re-read the partition/history they own.

XCom/task return values are limited to small JSON-serializable summaries, for example:

```text
load_tx_raw
  batch_date
  source_rows
  inserted_rows
  partition_rows

load_fx_raw
  batch_date
  fx_status
  source_rows
  inserted_rows
  partition_rows

classify_transactions
  raw_rows
  accepted_rows
  quarantine_rows
  accepted_inserted_rows
  quarantine_inserted_rows
  reconciled

build_currency_fact
  accepted_rows
  fx_rate_date
  is_carried_forward
  staleness_days
  fact_rows
  fact_inserted_rows
  reconciled
```

The summaries are observability evidence, not inputs required to rebuild the next task's data.

## Failure Semantics

F07 follows `fail fast, retry safe, no silent fallback`.

### Preflight failure

Missing configuration, inaccessible GCS/BigQuery, location mismatch, missing warehouse resources, schema mismatch, or partition mismatch fails the run before batch writes begin.

### Transaction raw failure

A transaction batch must have exactly one expected source file for each of the five regions. Missing, duplicate/unexpected, disappearing, checksum-invalid, or malformed transaction input fails `load_tx_raw`.

There is no partial business success for a missing transaction region.

### FX raw behavior

Same-day FX is sparse by contract.

- file absent: task succeeds with `NO_NEW_RATE`
- file present and valid: task loads it idempotently
- file present but malformed: task fails

A malformed same-day publication is not converted into `NO_NEW_RATE` and is not silently replaced by an older publication at ingestion time.

### Classification failure

Existing F05 validation or persisted reconciliation failure fails the task.

### Currency fact failure

The existing F06 rules remain authoritative. The resolver uses the newest published FX rate date `<= batch_date`. The selected publication must be one complete, valid USD/EUR pair. There is no future FX and no magic default.

If no prior complete USD/EUR snapshot exists, the fact task fails the batch.

### Retry behavior

Transient runtime failures may be retried by Airflow. The exact retry count and delay are operational parameters, not part of the F07 data contract; they must be finite and configured centrally rather than changing business behavior.

Retries rely on the already-proven idempotent persistence semantics in F04-F06: target-partition source-row IDs are queried, previously persisted IDs are anti-filtered, and only unseen rows are appended. Business validation errors are not hidden by fallback logic.

F07 does not add rollback or publish-state transitions. Those are F08 concerns.

## Historical Ordering and FX Carry-Forward

The v1 DAG uses `max_active_runs=1`. Backfill is therefore executed as a serial set of logical-date runs.

This is important for the sparse FX source contract. A Friday publication must be persisted before Saturday/Sunday facts are built so that the downstream F06 resolver sees complete historical state.

The live carry-forward acceptance window is:

```text
2025-07-25 Friday
  -> same-day FX published

2025-07-26 Saturday
  -> NO_NEW_RATE
  -> effective fx_rate_date = 2025-07-25
  -> is_carried_forward = True
  -> staleness_days = 1

2025-07-27 Sunday
  -> NO_NEW_RATE
  -> effective fx_rate_date = 2025-07-25
  -> is_carried_forward = True
  -> staleness_days = 2
```

No special weekend code exists in the DAG. Weekend behavior emerges from sparse ingestion plus F06's `latest rate_date <= batch_date` rule.

## Testing Strategy

### 1. Unit and DAG-structure tests

Tests must cover at least:

- logical date converts to the expected `batch_date` under `Asia/Bangkok`
- DAG schedule remains daily
- `catchup=False`
- `max_active_runs=1`
- expected task IDs exist
- dependency graph matches the approved architecture
- thin task wrappers call the correct pipeline service boundaries
- task summaries remain small/serializable and do not contain DataFrames
- preflight is validate-only
- preflight detects missing/mismatched BigQuery resources
- transaction/FX raw split preserves the F04 behavior
- existing `load_raw_batch()` remains backward-compatible
- absent same-day FX returns `NO_NEW_RATE`
- malformed present FX fails rather than becoming `NO_NEW_RATE`
- all existing F00-F06 regression tests remain green

### 2. Airflow runtime acceptance

The custom Airflow image must:

- import the repository pipeline modules
- import the production DAG without error
- load the required GCP/Pandas dependencies
- authenticate through mounted ADC
- reach the configured GCS bucket
- reach and validate the BigQuery warehouse contracts

### 3. One-day E2E acceptance

Use logical date `2025-07-22`, which already has known F04-F06 persisted evidence.

An Airflow run must traverse the real DAG through raw load, classification, FX resolution, and fact creation successfully.

Because this date was already processed before F07, this acceptance run is intentionally a retry/idempotency proof. It must not create duplicate persisted source rows. The expected inserted-row counts for already-complete target partitions are zero while persisted reconciliation remains true.

### 4. Historical/backfill acceptance

Use explicit Airflow backfill over `2025-07-25` through `2025-07-27`.

All three dates must use the same TaskFlow DAG and pipeline functions as normal daily execution. No special script may process the backfill range.

Evidence must confirm Friday same-day FX and Saturday/Sunday carry-forward lineage as specified above.

A full 360-date execution is intentionally deferred to F09.

## Expected Files Touched

Primary F07 implementation work is expected in:

```text
docker/airflow.Dockerfile
docker-compose.yml
airflow/dags/bahtflow_daily.py
pipeline/raw_load.py
pipeline/preflight.py
pipeline/bigquery_adapter.py          # only if narrow read-only validation support is needed
README.md
```

Tests are expected under the existing `tests/` layout for raw-load regression, preflight, DAG structure/logical date, and runtime-facing boundaries.

`pipeline/classification_load.py`, `pipeline/currency_fact_load.py`, and their business transformation modules should remain unchanged unless a narrowly justified orchestration-interface issue is discovered. Any such discovered change must preserve F05/F06 contracts and receive tests.

## Exit Criteria

F07 is complete only when all of the following are demonstrated with fresh evidence:

1. **Custom Airflow runtime works**
   - Airflow services run from the custom image.
   - DAG imports successfully.
   - Pipeline imports and GCP access work inside Airflow runtime.

2. **One-day E2E succeeds through the same orchestration path**
   - logical date `2025-07-22`
   - raw -> classification -> currency fact
   - DAG run succeeds
   - rerun/idempotency produces no duplicate persisted rows

3. **Explicit historical backfill uses the same code path**
   - backfill `2025-07-25` through `2025-07-27`
   - no special historical pipeline implementation
   - serial run ordering

4. **Carry-forward is proven live**
   - Friday uses published same-day FX
   - Saturday carries Friday with staleness 1
   - Sunday carries Friday with staleness 2

5. **Repository verification is clean before merge**
   - full pytest suite passes
   - DAG import/parse verification passes
   - relevant Python compile checks pass
   - `docker compose config --quiet` passes
   - `git diff --check` passes
   - credential scan remains clean
   - working tree is clean after committed implementation
   - README records only executed F07 evidence

## Non-Goals

F07 does not add:

- dbt
- marts
- a BI layer
- publish switching
- persisted quality-gate state
- run rollback
- new source systems
- streaming
- Spark/PySpark
- Kafka
- Composer
- Kubernetes
- Terraform
- Great Expectations
- full historical performance benchmarking

The design intentionally keeps orchestration thin and delegates business processing to the already tested Pandas pipeline services.
