# BahtFlow Airflow, Pandas, and dbt Design

## Status

Approved for planning on 2026-09-01.

## Purpose

Build a public, production-minded batch data-engineering portfolio project that
pre-learns the upcoming Pandas and dbt bootcamp material while demonstrating
the parts of the Hashed Analytic Data Engineer role that are most relevant to a
historical financial-data workload: Docker, Apache Airflow, Python/Pandas,
BigQuery, data quality, testing, monitoring, recovery, and clean analytics
outputs.

The project is intentionally a new repository. It does not reuse Dataform or
PySpark implementation code from other BahtFlow editions.

## Success Criteria

- A local Docker Compose environment runs Apache Airflow and the project
  dependencies reproducibly.
- Airflow can backfill 360 historical daily batches and run one daily
  incremental batch using the same DAG.
- A successful logical-date rerun produces the same published state without
  duplicating raw source rows.
- Pandas validates an incoming file contract and writes batch metadata without
  owning business transformations.
- BigQuery retains raw source data and lineage, and dbt Core creates tested
  staging, intermediate, and mart models.
- The repository contains automated Python, DAG, and dbt tests, a runbook, an
  architecture diagram, and no credentials or runtime secrets.

## Non-Goals

- Kafka, ksqlDB, Spark, and streaming ingestion are out of scope for the first
  release. The source and SLA are deliberately batch-oriented.
- This project does not replace or alter the existing BahtFlow Dataform and
  Databricks editions.
- Pandas is not the business-transformation engine, and dbt is not used to
  ingest files.
- A managed Airflow service such as Cloud Composer is not required; Airflow
  runs locally in Docker and connects to Google Cloud through standard
  credentials supplied outside Git.

## Architecture

```text
Daily historical and incremental CSV.gz files
                |
                v
      GCS landing zone (immutable objects)
                |
                v
  Apache Airflow, running locally with Docker
      |        |        |        |        |
      |        |        |        |        +-- alert / audit result
      |        |        |        +----------- reconciliation
      |        |        +-------------------- dbt test
      |        +----------------------------- dbt build
      +-------------------------------------- Pandas contract check + load
                |
                v
              BigQuery
 raw source -> staging -> intermediate -> marts
```

### Component Boundaries

| Component | Responsibility |
| --- | --- |
| Daily source files | One business-date batch per file; historical files support catchup and new files model the steady-state feed. |
| GCS landing | Stores immutable incoming objects and allows a reproducible file manifest. |
| Airflow | Creates daily logical runs, coordinates tasks, handles retries, writes run audit records, and calls an alert adapter. |
| Pandas/Python | Checks file readability, header/schema contract, expected business date, checksum, and row-count metadata. It does not reject intentionally dirty business rows. |
| BigQuery raw | Preserves raw strings and ingestion lineage for every source row. |
| dbt Core | Builds SQL models, quality tests, documentation, lineage, and analytics-ready mart tables in BigQuery. |
| Audit and alert adapter | Records batch status, row counts, timing, and failures. Console logging is the default; a secret-configured webhook can be an optional deployment setting. |

## Source and Batch Contract

The repository will include 360 synthetic historical daily files plus a daily
incremental-file pattern. Each input is associated with exactly one
`business_date`, source object path, object checksum, and source row sequence.

Airflow uses the logical date as the batch date. Initial catchup creates one
run for each historical day; steady-state runs process the corresponding daily
file. The source delivery convention, rather than a repeated filter over a
monthly file, is what makes daily backfill credible.

An ingestion audit table records at least:

```text
batch_date, dag_run_id, source_file, source_checksum,
source_rows, loaded_rows, rejected_contract_rows,
status, started_at, completed_at, error_summary
```

## Data Layers

### Raw

`raw.transactions` retains each source row in its original representation,
along with `source_file`, `source_checksum`, `source_row_number`, `batch_date`,
and ingestion timestamps. It is append-only and partitioned by transaction or
business date as appropriate for the model.

Raw data is not deduplicated by `transaction_id`: duplicate replays and
conflicting duplicate transaction IDs are intentional evidence for downstream
quality classification.

### dbt Transformations

```text
raw.transactions
      |
      v
stg_transactions
      |
      v
int_transactions_classified ----> quarantine model
      |
      v
fct_transactions
      |
      +--> mart_daily_sales
      +--> mart_currency_summary
      +--> mart_data_quality
```

The final schema names are configurable, but model responsibilities are fixed:

- staging standardizes names and performs safe type parsing;
- intermediate models assign explicit rejection reasons and duplicate semantics;
- fact models contain accepted, enriched transactions;
- quarantine models retain rejected rows and evidence;
- marts publish business and data-quality metrics.

FX conversion will use a separate daily exchange-rate source and retain the
source FX date used for every converted record.

## Idempotency and Recovery

The idempotency key is the source batch identity, not the business transaction
identifier. A rerun first checks the manifest and audit state using the source
object path and checksum. Loading is staged and committed with a stable source
row identity (`source_file`, `source_checksum`, `source_row_number`), allowing
the same file to be retried without duplicate raw rows.

If a dbt task fails after a successful raw load, Airflow reruns the same logical
date. The manifest prevents an unnecessary duplicate load; dbt rebuilds only
the selected batch or impacted incremental models. Public marts are published
only after dbt tests and reconciliation succeed.

## Data Quality and Observability

There are two deliberately separate quality layers:

1. Pandas validates the transport contract: the file exists, can be read, has
   the expected columns, maps to the requested business date, and has coherent
   metadata. A contract failure stops the batch.
2. dbt validates data and business rules: non-null/unique rules where
   appropriate, accepted currencies, relationships, duplicate classification,
   reconciliation, and published-mart assertions. Dirty records are retained
   in quarantine instead of silently discarded.

Airflow logs every task outcome and writes audit rows. A failure callback uses
an alert interface so that local runs are observable without embedding any
chat, email, or webhook secret in the public repository.

## Testing Strategy

- Python unit tests cover source-file discovery, schema/metadata validation,
  checksum and manifest behavior, and idempotency decisions.
- Airflow tests confirm that the DAG imports, task dependency order is correct,
  and the logical-date contract is passed to tasks.
- dbt tests cover schema constraints, accepted values, relationships, and
  custom reconciliation rules.
- An integration run processes a small fixture date through load, dbt build,
  dbt test, reconciliation, and audit publication.
- A documented recovery test intentionally fails a downstream task, reruns the
  same logical date, and verifies that source and published counts are not
  duplicated.

## Repository Shape

```text
bahtflow-airflow-pandas-dbt/
  airflow/                 # DAGs, operators, callbacks, Docker setup
  pipeline/                # Python/Pandas source-contract and load modules
  dbt/                     # dbt Core project, models, tests, macros, docs
  data/                    # synthetic source generator and small fixtures
  tests/                   # Python and DAG tests
  docs/                    # architecture, runbook, interview talking points
  docker-compose.yml
  .env.example
  README.md
```

## Configuration and Security

Project ID, dataset names, GCS bucket, region, and optional alert settings are
provided through environment variables or Airflow connections. `.env`, service
account keys, and runtime credentials are ignored by Git. The README uses
least-privilege guidance and a budget alert for the Google Cloud project.

The current BigQuery free tier includes 10 GiB of storage and 1 TiB of
on-demand query processing per month. Batch loads use the shared pool at no
charge, but the project must still use a billing-enabled Google Cloud project
for the DML and persistent-resource features required by this design.

## Acceptance Demonstration

The finished README and runbook will demonstrate:

1. Dockerized Airflow starts locally.
2. A historical daily catchup completes.
3. A selected batch has auditable source, raw, accepted, quarantine, and mart
   counts.
4. A deliberate downstream failure can be recovered by rerunning the same
   logical date without duplicated raw or published data.
5. dbt documentation and lineage show how an incoming transaction becomes a
   trusted analytics metric.

## Interview Narrative

> I designed BahtFlow as a batch ELT pipeline around operational reliability,
> not artificial compute scale. Airflow coordinates daily backfills and
> recovery, Pandas validates the source contract, BigQuery stores and computes
> the warehouse layers, and dbt makes the transformations, tests, lineage, and
> published business metrics explicit. The pipeline is idempotent at the file
> batch level, so a rerun reaches the same final state without hiding bad data
> or duplicating it.
