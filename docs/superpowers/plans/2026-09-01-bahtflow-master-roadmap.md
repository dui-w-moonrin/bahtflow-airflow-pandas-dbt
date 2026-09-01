# BahtFlow Master Feature Roadmap

> **For agentic workers:** Each feature below is a reviewer-sized delivery unit. Before implementing a feature, create a dedicated implementation plan under `docs/superpowers/plans/` and execute it with the project workflow: Ask -> Plan -> Implement -> Review Diff -> Run/Test -> Commit.

**Goal:** Build a production-minded batch ELT portfolio project that can backfill 360 historical transaction days, continue with daily incremental batches, preserve dirty raw data, convert currencies with sparse BOT FX rates, and publish only reconciled, tested analytics outputs.

**Architecture:** Source files are committed as a reproducible corpus, copied to an immutable GCS landing zone, validated at the transport boundary with Python/Pandas, and loaded idempotently into BigQuery raw tables. Local Dockerized Airflow orchestrates historical backfill and steady-state daily runs; dbt Core owns business transformations, quarantine, FX carry-forward, testing, lineage, marts, and publication-ready outputs.

**Tech Stack:** Apache Airflow in Docker, Python, Pandas, Google Cloud Storage, BigQuery, dbt Core with `dbt-bigquery`, pytest, Git/GitHub.

**Spec:** `docs/superpowers/specs/2026-09-01-bahtflow-airflow-pandas-dbt-design.md`

## Global Constraints

- Airflow runs locally in Docker; Cloud Composer is out of scope for v1.
- Pandas validates transport/file contracts and adds ingestion metadata; it does not own business transformations.
- BigQuery raw preserves source values and lineage; intentionally dirty rows are not silently cleaned at ingestion.
- dbt owns SQL-based staging, classification, quarantine, FX enrichment, marts, tests, documentation, and lineage.
- Transaction ingestion requires the complete five-region batch for a date; missing transaction files fail the batch.
- FX is a sparse source. Missing weekend/holiday FX files are expected and must not fail the batch by themselves.
- FX carry-forward uses the most recent published rate on or before the business date and always retains the original `rate_date`; no arbitrary default FX value is allowed.
- Idempotency is based on stable source-file/source-row identity, not on transaction ID.
- Kafka, ksqlDB, Spark, streaming ingestion, Kubernetes, Terraform, Great Expectations, a BI application, and ML workloads are out of scope for v1.
- Credentials, service-account keys, `.env`, tokens, and webhook secrets must never be committed.

---

## Release Shape

The project is divided into **10 features**. Feature 00 is the source-material foundation; Features 01-09 build the executable pipeline.

| Feature | Name | Outcome | Dependency | Status |
| --- | --- | --- | --- | --- |
| F00 | Source Contract & Complete Corpus | Reproducible 360-day backfill + 5 incremental days + sparse FX | none | historical + FX complete; incremental extension remains |
| F01 | Local Airflow Runtime | Reproducible Dockerized Airflow/dev environment | F00 | planned |
| F02 | GCP Landing & Warehouse Bootstrap | Immutable GCS landing + BigQuery datasets/tables | F00-F01 | planned |
| F03 | Pandas Intake & Idempotent Raw Load | Contract validation + raw transaction/FX ingestion | F02 | planned |
| F04 | Airflow Backfill & Daily Orchestration | Same DAG handles 360-day backfill and steady-state dates | F03 | planned |
| F05 | dbt Classification & Quarantine | Safe parsing, DQ classification, duplicate semantics | F03-F04 | planned |
| F06 | FX Carry-Forward & Currency Fact | Complete daily FX dimension + THB/USD/EUR conversion | F05 | planned |
| F07 | Marts, Reconciliation & Safe Publication | Business/DQ marts exposed only after success gates | F05-F06 | planned |
| F08 | Observability & Recovery Proof | Audit trail, failure visibility, rerun/recovery demonstration | F04-F07 | planned |
| F09 | CI, Runbook & Interview Package | Reproducible tests, docs, diagrams, demo narrative | F01-F08 | planned |

---

## F00 - Source Contract & Complete Corpus

**Branch:** `feat/00-source-contract-complete-corpus`

### Purpose

Lock one source contract before building infrastructure so every later feature is developed and tested against the same files.

### Existing material

- Historical transaction corpus: 360 dates from `2025-07-22` through `2026-07-16`.
- Five regional gzip CSV files per date: `bkk`, `central`, `north`, `northeast`, `south`.
- Historical physical files: 1,800.
- Historical rows: 3,672,845.
- Transaction schema: `txn,dtts,amount,currency`; region is derived from the source path.
- Sparse BOT FX source: 251 published-date files from `2025-07-08` through `2026-07-21`, exactly EUR and USD per published date.

### Remaining scope

Bring the five existing incremental transaction dates `2026-07-17` through `2026-07-21` from the earlier BahtFlow source into the same five-region daily contract. Split each combined incremental day into five regional gzip files, add them under `data/daily_regional_sales/business_date=YYYY-MM-DD/`, and regenerate the transaction manifest.

### Acceptance

- 360 dates remain explicitly documented as the historical backfill window.
- 5 later dates are explicitly documented as steady-state/incremental demo dates.
- Every transaction date has exactly five regional files and the exact source header.
- The manifest verifies file path, region, row count, timestamp bounds, compressed size, and SHA-256.
- FX remains sparse; no weekend/holiday files are fabricated.
- Source validation tests pass without changing intentionally dirty transaction values.

### Main files

- Modify: `data/README.md`
- Modify: `data/daily_source_manifest.csv`
- Create/modify: `scripts/split_incremental_to_regional_daily.py`
- Test: `tests/test_incremental_source_splitter.py`

---

## F01 - Local Airflow Runtime

**Branch:** `feat/01-local-airflow-runtime`

### Purpose

Create the reproducible local runtime used for development, tests, backfill, and the interview demo.

### Scope

- Docker Compose stack with Airflow and PostgreSQL metadata DB.
- A custom Airflow image containing the Google providers, Pandas/BigQuery dependencies, and dbt BigQuery adapter needed by later tasks.
- Repository mounts for DAGs, pipeline code, dbt project, logs, and test fixtures.
- `.env.example` containing only non-secret variable names and safe defaults.
- Local test configuration and dependency lock/pins suitable for the selected Airflow version.
- A minimal smoke DAG proving the container can import project code.

### Acceptance

- `docker compose config` succeeds.
- Airflow scheduler/API/UI services start healthy.
- Airflow can list/import the smoke DAG without import errors.
- Project Python tests run inside or against the same dependency environment.
- No credentials are present in Git.

### Main files

- Create: `docker-compose.yml`
- Create: `airflow/Dockerfile`
- Create: `airflow/dags/smoke_test.py`
- Create: `requirements.txt` and/or project dependency configuration selected during the dedicated feature plan
- Create: `.env.example`
- Test: `tests/airflow/test_smoke_dag.py`

---

## F02 - GCP Landing & Warehouse Bootstrap

**Branch:** `feat/02-gcp-landing-bootstrap`

### Purpose

Create a small, explicit cloud boundary without moving orchestration out of local Docker.

### GCP layout

Use environment-configured names with four logical BigQuery datasets:

```text
bahtflow_raw        # immutable/raw source rows
bahtflow_analytics  # dbt staging, intermediate, fact, dimensions, marts
bahtflow_ops        # run/file audit state and load staging tables
bahtflow_public     # consumer-facing successful-only views
```

GCS landing layout:

```text
transactions/business_date=YYYY-MM-DD/sales_{region}_YYYYMMDD.csv.gz
fx/YYYY/MM/fx_YYYYMMDD.csv
manifests/daily_source_manifest.csv
manifests/fx_manifest.csv
```

### Scope

- Bootstrap/check the GCS landing bucket and four datasets in one configured BigQuery location.
- Upload transaction/FX source objects idempotently from the committed corpus.
- Attach or verify checksum metadata so an existing object with different content fails rather than being overwritten silently.
- Create raw/ops table schemas required before Airflow runs.
- BigQuery raw transaction fields remain strings for `txn`, `dtts`, `amount`, and `currency`.
- Partition `raw.transactions` by `batch_date`; partition `raw.fx_rates` by `rate_date`.

### Acceptance

- Re-running the bootstrap/upload does not duplicate or overwrite unchanged source objects.
- A checksum mismatch is surfaced as a hard error.
- The expected transaction, FX, manifest objects are discoverable from GCS.
- BigQuery datasets exist in the configured location.
- Empty raw/ops tables have the documented schema and partitioning.

### Main files

- Create: `pipeline/config.py`
- Create: `scripts/bootstrap_gcp.py`
- Create: `scripts/upload_landing_sources.py`
- Create: `sql/bootstrap_raw_and_ops.sql`
- Test: `tests/pipeline/test_config.py`
- Test: `tests/pipeline/test_landing_manifest.py`

---

## F03 - Pandas Intake & Idempotent Raw Load

**Branch:** `feat/03-pandas-intake-raw-load`

### Purpose

Make the ingestion boundary strict about transport correctness while preserving dirty business data for downstream classification.

### Transaction contract

For each logical `batch_date`:

1. Discover exactly five regional GCS objects.
2. Verify each object against the committed/source manifest checksum and row expectations.
3. Read the gzip CSV with Pandas as strings.
4. Verify the exact header `txn,dtts,amount,currency`.
5. Verify every parseable `dtts` date belongs to the requested batch date; malformed timestamps remain raw evidence and are classified downstream rather than silently discarded.
6. Derive `region` from the object path.
7. Add `source_file`, `source_checksum`, `source_row_number`, `batch_date`, `ingested_at`, and stable `source_row_id` metadata.

### FX contract

- Same-day FX file is optional because the source is sparse.
- When present, it must contain exactly EUR and USD records with the expected columns and its own `rate_date`.
- Missing same-day FX is recorded as `NO_NEW_RATE`, not as an ingestion failure.

### Idempotent load

Load each validated daily DataFrame to a temporary BigQuery staging table, then perform an insert-only merge into raw using stable source-row identity. A rerun of the same unchanged file must insert zero additional raw rows.

### Acceptance

- Missing one of the five transaction regions fails before any batch publication.
- Invalid business values such as `N/A`, negative amounts, and duplicate transaction IDs survive raw ingestion unchanged.
- A holiday/weekend with no FX file can still proceed.
- A same-file retry produces the same raw row count.
- A changed file at an immutable source path is rejected by checksum validation.

### Main files

- Create: `pipeline/source_discovery.py`
- Create: `pipeline/contracts.py`
- Create: `pipeline/transaction_intake.py`
- Create: `pipeline/fx_intake.py`
- Create: `pipeline/bigquery_loader.py`
- Test: `tests/pipeline/test_source_discovery.py`
- Test: `tests/pipeline/test_transaction_contract.py`
- Test: `tests/pipeline/test_fx_contract.py`
- Test: `tests/pipeline/test_raw_idempotency.py`

---

## F04 - Airflow Backfill & Daily Orchestration

**Branch:** `feat/04-airflow-backfill-daily-dag`

### Purpose

Use one logical-date-driven workflow for both historical recovery/backfill and steady-state daily processing.

### DAG contract

Main DAG: `bahtflow_daily`.

Recommended task flow:

```text
start_run_audit
       |
       +--> discover_tx -> validate_tx -> load_tx_raw --+
       |                                                |
       +--> discover_fx -> validate_fx -> load_fx_raw --+
                                                        v
                                                dbt_classify
                                                        |
                                                dbt_quality_gate
                                                        |
                                                 dbt_publish
                                                        |
                                                  reconcile
                                                        |
                                                 mark_success
```

Failure callbacks update the run audit and call the alert adapter.

### Backfill policy

- Historical window: `2025-07-22` through `2026-07-16` (360 logical dates).
- Use an explicit Airflow backfill date range rather than allowing catchup to run beyond the committed fixture window.
- The same DAG then processes `2026-07-17` through `2026-07-21` as individual steady-state/incremental dates.
- Every task derives input from Airflow logical date/data interval; no task hardcodes a source date.

### Acceptance

- DAG imports cleanly.
- Task dependency graph matches the documented flow.
- Backfill produces one logical run per historical date.
- A selected incremental date uses the same code path as a historical date.
- Transaction contract failures stop downstream dbt/publication tasks.
- Missing FX does not block a date when a prior valid rate exists downstream.

### Main files

- Create: `airflow/dags/bahtflow_daily.py`
- Create: `airflow/callbacks.py`
- Create: `airflow/task_wrappers.py`
- Test: `tests/airflow/test_bahtflow_daily_dag.py`
- Test: `tests/airflow/test_logical_date_contract.py`

---

## F05 - dbt Classification & Quarantine

**Branch:** `feat/05-dbt-classification-quarantine`

### Purpose

Move business-quality semantics into warehouse SQL where they are modular, testable, documented, and lineage-visible.

### Model graph

```text
raw.transactions
      |
      v
stg_transactions
      |
      v
int_transactions_classified
      |                    |
      |                    +--> transactions_quarantine
      v
transactions_accepted
```

### Classification rules

Retain the established BahtFlow semantics:

- Missing/blank required fields are rejected.
- Invalid transaction ID, timestamp, region, currency, and amount values are rejected.
- Negative amounts are rejected; zero-value transactions are allowed.
- Exact duplicate replays retain one deterministic row and quarantine the additional replay rows.
- Duplicate conflicts reject every occurrence because the correct payload cannot be inferred.
- Amount outliers may be flagged as warnings but are not automatically quarantined.

Each rejected row keeps an explicit `rejection_reason`/classification evidence and its raw source identity.

### Acceptance

- Safe parsing never crashes a model on malformed raw strings.
- `accepted + quarantine = classified` for every batch.
- Exact replay and conflict cases are deterministic on rerun.
- Quarantine retains the original raw evidence and source metadata.
- dbt schema tests cover required keys and accepted values.

### Main files

- Create: `dbt/dbt_project.yml`
- Create: `dbt/models/sources.yml`
- Create: `dbt/models/staging/stg_transactions.sql`
- Create: `dbt/models/intermediate/int_transactions_classified.sql`
- Create: `dbt/models/intermediate/transactions_accepted.sql`
- Create: `dbt/models/quarantine/transactions_quarantine.sql`
- Create: `dbt/models/**/schema.yml`
- Create: `dbt/tests/assert_batch_classification_reconciles.sql`

---

## F06 - FX Carry-Forward & Currency Fact

**Branch:** `feat/06-fx-carry-forward-conversion`

### Purpose

Turn the sparse BOT feed into a complete daily FX lookup without fabricating source records, then produce one accepted fact table with comparable THB/USD/EUR amounts.

### FX model graph

```text
raw.fx_rates
      |
      v
stg_fx_rates
      |
      v
 dim_fx_daily
      |
      +----------------------+
                             v
transactions_accepted --> fct_transactions
```

### FX behavior

- `dim_fx_daily` has one row per business date and foreign currency required by the fact model.
- If a new BOT rate exists for the date, use it directly.
- Otherwise carry forward the latest published rate whose `rate_date <= business_date`.
- Keep `rate_date`, `is_carried_forward`, and `staleness_days` so consumers can see where the value came from.
- If no prior rate exists, fail the dbt quality gate; do not substitute a magic default.
- THB uses the identity conversion of 1.0.

Because the FX corpus starts on `2025-07-08` while transaction history begins on `2025-07-22`, the source contains a natural pre-history buffer for the first transaction batch.

### Currency conversion

For each accepted transaction, materialize:

- `amount_thb`
- `amount_usd`
- `amount_eur`
- USD rate/value lineage used for the date
- EUR rate/value lineage used for the date

The BOT convention is THB per 1 unit of foreign currency.

### Acceptance

- Every accepted fact row has all three converted amounts.
- Weekend/holiday dates use a prior published FX rate and retain that prior `rate_date`.
- No FX lookup uses a future rate.
- No fact row is published with missing FX coverage.
- Conversion tests verify known example calculations and rate lineage.

### Main files

- Create: `dbt/models/staging/stg_fx_rates.sql`
- Create: `dbt/models/intermediate/dim_fx_daily.sql`
- Create: `dbt/models/marts/fct_transactions.sql`
- Create/modify: dbt schema YAML for FX/fact tests
- Create: `dbt/tests/assert_fx_coverage.sql`
- Create: `dbt/tests/assert_no_future_fx.sql`

---

## F07 - Marts, Reconciliation & Safe Publication

**Branch:** `feat/07-marts-reconciliation-publication`

### Purpose

Separate internal processing tables from consumer-facing data so a technically completed task does not automatically mean trusted data is published.

### Internal marts

Build at least:

- `mart_daily_sales`: daily accepted transaction count and THB/USD/EUR totals.
- `mart_region_sales`: daily metrics by the five source regions.
- `mart_currency_summary`: metrics by original transaction currency.
- `mart_data_quality`: raw, accepted, quarantine, invalid, replay, conflict, and warning metrics by batch.

### Reconciliation gate

For every batch verify:

```text
manifest source rows
    = raw unique source rows
    = classified rows
    = accepted rows + quarantine rows
```

Also verify the published fact count equals the accepted count and no published fact is missing FX coverage.

### Safe publication

Use `bahtflow_ops.pipeline_runs` as the authoritative success gate. `bahtflow_public` views expose only batches whose final audit status is `SUCCESS`; an internal model may exist for a failed run, but it must not become consumer-visible.

### Acceptance

- Failed/incomplete batches do not appear in public views.
- A successful batch becomes visible only after dbt tests and reconciliation pass.
- Public totals reconcile back to accepted fact rows.
- Re-running a successful logical date does not duplicate published metrics.

### Main files

- Create: `dbt/models/marts/mart_daily_sales.sql`
- Create: `dbt/models/marts/mart_region_sales.sql`
- Create: `dbt/models/marts/mart_currency_summary.sql`
- Create: `dbt/models/marts/mart_data_quality.sql`
- Create: `dbt/models/public/published_transactions.sql`
- Create: `dbt/models/public/published_daily_sales.sql`
- Create: `pipeline/reconciliation.py`
- Test: `tests/pipeline/test_reconciliation.py`
- Create: `dbt/tests/assert_fact_reconciles.sql`

---

## F08 - Observability & Recovery Proof

**Branch:** `feat/08-observability-recovery`

### Purpose

Make failure diagnosis and rerun behavior visible enough to answer production-incident interview questions with evidence from the project.

### Ops tables

Use two explicit operational grains:

1. `bahtflow_ops.pipeline_runs` - one row/state record per Airflow logical run, including batch date, DAG run ID, status, counts, start/end timestamps, duration, and error summary.
2. `bahtflow_ops.file_ingestion_audit` - one record per source file attempt, including source kind, path, checksum, expected/loaded rows, and ingestion status.

### Alert adapter

- Default implementation logs structured failure information to Airflow logs/console.
- Optional webhook configuration may be supported through environment/connection secrets, but the public repository contains no destination secret.

### Recovery demonstration

Document and execute a controlled downstream failure after raw ingestion, then fix/remove the failure and rerun the same logical date. Prove:

- raw source rows do not increase on retry;
- accepted/quarantine/fact counts return to the expected state;
- public views remain on the last successful state while the run is failed;
- the corrected rerun reaches `SUCCESS` and publishes once.

### Acceptance

- Every run has a visible success/failure audit state.
- Every source file has checksum/row-count audit evidence.
- Failure logs identify the task and logical batch date.
- Recovery run proves same-input idempotency with before/after count queries saved in the runbook.

### Main files

- Create: `pipeline/audit.py`
- Create: `pipeline/alerts.py`
- Modify: `airflow/callbacks.py`
- Modify: `airflow/dags/bahtflow_daily.py`
- Test: `tests/pipeline/test_audit.py`
- Test: `tests/airflow/test_failure_callback.py`
- Create/modify: `docs/runbook.md`

---

## F09 - CI, Runbook & Interview Package

**Branch:** `feat/09-ci-docs-interview-package`

### Purpose

Turn the codebase into a reproducible portfolio artifact rather than a one-machine demo.

### CI scope

A pull request must be able to run credential-free checks that do not require live GCP access:

- Python unit tests.
- Source-contract tests on small committed fixtures.
- Airflow DAG import/dependency tests.
- dbt parse/project validation that does not issue warehouse queries.
- repository checks that no `.env` or credential/key file is tracked.

Live GCP integration/backfill remains an explicit runbook step rather than a public-CI secret requirement.

### Documentation scope

- Rewrite root README around the final architecture and actual measured results.
- Architecture diagram showing source -> GCS -> Airflow/Pandas -> BigQuery raw -> dbt -> public views.
- Runbook for local startup, GCP bootstrap, source upload, one-day run, historical backfill, incremental dates, recovery, and teardown.
- Data contracts for transaction and sparse FX sources.
- Table/model catalog with raw, quarantine, fact, marts, audit, and public outputs.
- Cost-control/security notes.
- Interview talking points explaining: why batch not streaming, why Pandas not Spark, why Airflow and dbt both exist, FX carry-forward, idempotency, schema/file-contract handling, reconciliation, and recovery.
- Capture evidence such as Airflow graph/run screenshots and dbt lineage/docs after the implementation is working.

### Acceptance

- Fresh clone can run credential-free test suite following README commands.
- Runbook can reproduce one end-to-end GCP batch.
- Full historical window and five later daily dates are documented with measured counts/durations after execution.
- README contains no claim that is not backed by executed pipeline evidence.
- Repository is ready to show directly in a technical interview.

### Main files

- Create: `.github/workflows/ci.yml`
- Modify: `README.md`
- Create: `docs/architecture.md`
- Create: `docs/runbook.md`
- Create: `docs/interview-talking-points.md`
- Create: `docs/model-catalog.md`

---

## Delivery Order and Milestones

### Milestone A - Reproducible Foundation

**F00 -> F01 -> F02**

Exit condition: all source material is under one contract, Dockerized Airflow starts locally, and the immutable corpus is reproducibly present in GCS/BigQuery resource scaffolding.

### Milestone B - First Vertical Slice

**F03 -> F04 -> thin path through F05/F06**

Exit condition: one selected logical date can flow from five GCS transaction files through Pandas validation and idempotent BigQuery raw load, then through dbt classification and FX conversion to a fact output.

This is the highest-priority interview milestone because it demonstrates the architecture with working evidence before every portfolio enhancement is finished.

### Milestone C - Full Data Product

**complete F05 -> F06 -> F07**

Exit condition: dirty rows are classified/quarantined, FX is complete by business date, all business/DQ marts reconcile, and only successful batches are publicly visible.

### Milestone D - Production-Minded Proof

**F08 -> F09**

Exit condition: the project can demonstrate failure, recovery, idempotent rerun, operational audit evidence, CI checks, and a reproducible public runbook.

---

## Definition of Done for v1

v1 is complete only when all of the following are demonstrated with executed evidence:

1. 360 historical transaction dates can be backfilled with the same `bahtflow_daily` DAG.
2. The five later dates `2026-07-17` through `2026-07-21` can be processed individually using the same DAG contract.
3. Each transaction batch requires five regional files; missing region input fails the batch.
4. Sparse FX missing dates are resolved by latest-prior-rate logic, not fabricated source files or magic defaults.
5. Same-date rerun does not duplicate raw, fact, or public rows.
6. Dirty business rows are preserved in raw and explicitly classified into accepted/quarantine outcomes.
7. Exact duplicate replays and duplicate conflicts have deterministic, documented behavior.
8. Accepted rows have THB/USD/EUR converted amounts with FX source-date lineage.
9. Source -> raw -> accepted/quarantine -> fact -> public counts reconcile per batch.
10. A failed batch is visible in audit data and remains hidden from consumer-facing views.
11. A documented recovery rerun returns the failed logical date to the expected final state.
12. Python, Airflow DAG, and dbt project checks run from the repository without exposing credentials.
13. README/runbook claims match actual measured pipeline results.

---

## Deliberate Non-Goals for v1

The following are explicitly deferred because they do not improve the batch use case enough to justify the complexity:

- Kafka or ksqlDB event streaming.
- Spark/PySpark distributed transformation.
- Cloud Composer or another managed Airflow service.
- Kubernetes or Terraform deployment.
- Great Expectations in addition to dbt tests.
- Custom BI/dashboard application.
- ML/AI feature generation.
- Real-time FX ingestion.
- Multi-cloud deployment.

These can be discussed as alternative architectures in interviews without being added to the implementation.

---

## Branch / Review Discipline

For each feature:

```text
Ask
  -> dedicated feature plan
  -> create feat/NN-name branch
  -> implement test-first
  -> review git diff
  -> run focused tests
  -> run full credential-free test suite
  -> commit small logical changes
  -> PR / final diff review
  -> merge to main
```

Do not combine two feature branches merely because they touch the same technology. The feature boundary is defined by an independently testable engineering outcome, not by tool name.
