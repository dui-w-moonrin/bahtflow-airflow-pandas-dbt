# BahtFlow Thin Pandas v1 Roadmap Design

## Status

Approved in chat on 2026-09-02.

## Goal

Finish a production-minded batch ELT portfolio quickly enough to leave time for Pandas interview review. v1 deliberately favors a thin, testable vertical slice over tool breadth.

## Architecture Decision

Use Pandas for all v1 transformation logic. dbt is deferred to v2.

```text
GCS
  -> Pandas intake
  -> BigQuery raw
  -> Pandas classification
  -> accepted / quarantine
  -> effective FX resolution
  -> currency conversion
  -> BigQuery fact
  -> marts
  -> reconciliation
  -> publish
```

Airflow remains the orchestrator. BigQuery remains the warehouse. Pandas owns transport validation, business classification, FX enrichment, derived columns, and batch aggregations. BigQuery SQL may be used for warehouse-native loading/idempotency mechanics, but not as a second business-transformation layer.

## Runtime Dependency

```text
start_run_audit
  -> preflight
  -> [transaction branch || FX branch]

transaction branch:
  discover_tx_files
  -> validate_tx_batch
  -> load_tx_raw
  -> classify_transactions
  -> accepted / quarantine

FX branch:
  discover_fx_file
  -> validate_fx_or_NO_NEW_RATE
  -> load_fx_raw
  -> resolve_effective_fx

accepted + effective_fx
  -> enrich_and_convert_fx
  -> fct_transactions
  -> marts
  -> reconcile
  -> quality_gate
  -> publish
  -> mark_success
```

Transaction batches require exactly five regional files. Same-day FX is optional; missing weekend/holiday FX is `NO_NEW_RATE`, and downstream logic must use the latest published rate where `rate_date <= business_date`.

## Transformation Rules

Raw source values are preserved before business transformation. Classification happens before FX conversion.

Transaction rows are quarantined for missing required values, invalid transaction/timestamp/region/currency/amount, negative amount, or duplicate conflict. Zero amount is allowed. Exact duplicate replays retain one deterministic row and quarantine extra replay rows. Duplicate conflicts quarantine every occurrence. Amount outliers may be warnings but are not automatic quarantine rules in v1.

Accepted transactions are enriched with effective USD/EUR rates. No future rate or magic default is allowed. THB uses identity 1.0. Converted outputs include THB, USD, and EUR amounts plus FX source-date lineage.

Batch reconciliation must prove:

```text
manifest source rows
= raw unique source rows
= classified rows
= accepted + quarantine

accepted
= fact rows
```

## Thin Feature Roadmap

| Feature | Outcome | Minimum measurable exit |
| --- | --- | --- |
| F00 Source Contract | Reproducible transaction + sparse FX corpus | 1,800 transaction files, 251 FX files, manifests/checksums |
| F01 Airflow Runtime | Local Dockerized Airflow 3 runtime | healthy services, DAG import, tests pass |
| F02 Immutable GCS Landing | Complete immutable cloud landing | 1,800 TX + 251 FX + 2 manifests = 2,053 objects |
| F03 BigQuery Bootstrap | Warehouse boundary exists | required datasets/tables, schema/location/partition correct, rerun safe |
| F04 Pandas Intake + Raw Load | One batch reaches raw idempotently | 5/5 TX required, FX optional, dirty values preserved, rerun adds 0 rows |
| F05 Pandas DQ Classification | Accepted/quarantine outputs | raw = accepted + quarantine; duplicate rules deterministic |
| F06 FX + Currency Fact | Effective FX and converted fact | no future FX; THB/USD/EUR values and lineage present |
| F07 Airflow E2E + Backfill | Proven orchestration path | one-day run and historical dates use the same logical-date code path |
| F08 Marts + Reconciliation + Recovery | Trusted outputs and retry proof | counts reconcile; failed run not published; retry does not duplicate |
| F09 CI + Full Run + Interview Package | Reproducible portfolio artifact | credential-free CI/tests, measured full run, concise README/runbook/evidence |

## Thin-Mode Constraints

- Each feature has one primary outcome.
- No dbt in v1.
- No Spark/PySpark, Kafka, streaming, Composer, Kubernetes, Terraform, Great Expectations, BI app, or ML.
- No generic repository/service framework beyond the narrow adapter already needed.
- F03 creates only infrastructure needed by F04-F09; no ingestion or business transformations.
- F04 proves one logical date before any 360-day execution.
- F05-F06 transformation logic is plain, testable Pandas functions.
- F07 wraps proven Python functions in Airflow instead of developing business logic inside DAG code.
- F08 keeps marts minimal: enough to demonstrate business output, DQ output, reconciliation, and recovery.
- F09 documents only executed evidence; no unsupported claims.

## v2 Deferred Work

A future version may replace Pandas warehouse transformations with dbt models/tests while retaining the GCS landing and BigQuery raw contracts. This is intentionally deferred and is not required for v1 completion.
