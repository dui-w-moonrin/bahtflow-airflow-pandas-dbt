# Feature 01: Airflow 3 Setup Design

## Status

Approved in chat on 2026-09-01 and ready for implementation planning.

## Purpose

Add a local Apache Airflow 3 runtime on top of the Docker foundation created in Feature 00. This feature proves orchestration only: Airflow must start reliably, display the final BahtFlow DAG shape using empty tasks, accept logical-date execution, and support an explicit multi-day backfill. No GCS, BigQuery, Pandas ingestion, dbt transformation, or real BahtFlow data processing is included yet.

## Version and Runtime Decisions

- Apache Airflow: `3.3.1`.
- Airflow image: `apache/airflow:3.3.1-python3.12`.
- Executor: `LocalExecutor`.
- Metadata backend: the existing PostgreSQL 16 service from Feature 00.
- Metadata database connection: the existing local `bahtflow` PostgreSQL database and credentials supplied through `.env`.
- Auth manager: Airflow 3 default SimpleAuthManager.
- Local authentication mode: `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`; this is explicitly development-only.
- API/UI host URL: `http://localhost:8080`.
- Internal Execution API URL: `http://airflow-api-server:8080/execution/`.
- DAG/business timezone: `Asia/Bangkok`.
- Airflow configuration is supplied through environment variables rather than a committed runtime `airflow.cfg`.

The Airflow image is pinned to Python 3.12 to match the existing Feature 00 toolbox runtime and avoid accidental Python-version drift while later Python/Pandas dependencies are introduced.

## Service Architecture

Feature 00 currently provides:

```text
postgres
   |
   +-- toolbox
```

Feature 01 extends that environment to:

```text
postgres
   |
   +-- toolbox
   |
   +-- airflow-init          # one-shot metadata migration
   +-- airflow-api-server    # UI + Core/Execution APIs on localhost:8080
   +-- airflow-scheduler     # scheduling + LocalExecutor task execution
   +-- airflow-dag-processor # DAG parsing/serialization
```

The Airflow services share the same image, environment block, network, DAG bind mount, and log volume. `airflow-init` must complete the database migration before long-running Airflow components start.

Feature 01 intentionally does not add Redis, Celery workers, triggerer, Kubernetes, or a custom Airflow image.

## Docker Compose Changes

`docker-compose.yml` will retain the existing `postgres` and `toolbox` services and add a reusable Airflow service configuration through YAML anchors where doing so keeps the file readable.

Shared Airflow configuration includes at least:

```text
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://<user>:<password>@postgres:5432/<database>
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
AIRFLOW__API__BASE_URL=http://localhost:8080
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Bangkok
```

`AIRFLOW__API__BASE_URL` is the host/browser-facing URL. The Execution API setting deliberately uses the Docker service hostname because tasks launched by LocalExecutor run inside the Airflow container network; `localhost:8080` from that context would refer to the wrong container/process.

The database connection string is composed from environment values already represented in `.env.example`; no real secret is committed.

Mounts:

```text
./airflow/dags -> /opt/airflow/dags
./airflow/logs -> /opt/airflow/logs
```

Runtime-generated Airflow logs are ignored by Git while the directory itself remains reproducible through a committed `.gitkeep`.

## Initialization Contract

The one-shot `airflow-init` service performs:

```text
airflow db migrate
```

It depends on PostgreSQL becoming healthy and exits successfully after the schema is ready. Long-running Airflow components depend on successful initialization.

A fresh clone must be able to follow the documented order:

```text
Copy .env.example -> .env
start/verify Feature 00 Docker foundation
run airflow-init
start Airflow services
```

Re-running the migration must be safe.

## DAG Skeleton

Create `airflow/dags/bahtflow_daily.py` with DAG ID:

```text
bahtflow_daily
```

All Feature 01 tasks use `airflow.providers.standard.operators.empty.EmptyOperator`.

Target graph:

```text
                         start
                           |
                +----------+----------+
                |                     |
                v                     v
        discover_tx_files        discover_fx
                |                     |
                v                     v
        validate_tx_files         validate_fx
                |                     |
                v                     v
           load_tx_raw           load_fx_raw
                |                     |
                +----------+----------+
                           |
                           v
                     dbt_transform
                           |
                           v
                       dbt_test
                           |
                           v
                       reconcile
                           |
                           v
                         finish
```

Task IDs are part of the orchestration contract and are retained by later features when empty tasks are replaced with real implementations.

## Scheduling and Backfill Contract

The DAG uses:

```text
schedule = "@daily"
catchup = False
start_date = 2025-07-22 00:00:00 Asia/Bangkok
```

`Asia/Bangkok` is intentional: later transaction paths are keyed by Thai business date, so Airflow's data interval must align with that date rather than rely on an implicit UTC boundary.

The scheduler does not automatically create the full historical backlog when first started. Historical execution is explicit through the Airflow 3 backfill command. Feature 01 proves the mechanism with only three dates:

```text
2025-07-22
2025-07-23
2025-07-24
```

The intended CLI flow is:

```text
airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-22 --to-date 2025-07-24
```

The full 360-day historical run remains deferred to Feature 08.

Every future real task must eventually derive its batch identity from Airflow logical-date/data-interval context. Feature 01 establishes the schedule/backfill semantics without reading source files.

## Health and Verification

The API server exposes Airflow's public health endpoint:

```text
GET /api/v2/monitor/health
```

Feature verification covers both container state and Airflow state.

Required evidence:

1. `docker compose config --quiet` succeeds.
2. PostgreSQL remains healthy.
3. `airflow-init` exits successfully.
4. API server, scheduler, and DAG processor remain running.
5. Airflow reports version `3.3.1`.
6. `/api/v2/monitor/health` reports healthy database, scheduler, and DAG processor state.
7. `bahtflow_daily` is visible/imported with no DAG import error.
8. The DAG task dependency graph matches the design.
9. One explicit logical-date execution succeeds.
10. The three-day backfill creates successful runs for 2025-07-22 through 2025-07-24.

## Testing Strategy

Automated tests remain credential-free and local.

### DAG import test

Import the DAG in an Airflow-enabled test environment and assert:

- DAG ID is `bahtflow_daily`.
- expected task IDs are present exactly once;
- no unexpected real operators or external-system dependencies are introduced;
- schedule is `@daily`, automatic catchup is disabled, and timezone-aware start date is Asia/Bangkok.

### Dependency test

Assert direct downstream relationships for both branches and the fan-in path, especially:

```text
start -> discover_tx_files
start -> discover_fx
discover_tx_files -> validate_tx_files -> load_tx_raw
discover_fx -> validate_fx -> load_fx_raw
load_tx_raw -> dbt_transform
load_fx_raw -> dbt_transform
dbt_transform -> dbt_test -> reconcile -> finish
```

### Runtime smoke test

Use Docker Compose commands and Airflow CLI/API checks to prove initialization, service health, DAG discovery, a single run, and the bounded three-day backfill.

## Repository Changes

Expected files:

```text
Modify:
  docker-compose.yml
  .env.example
  .gitignore
  README.md

Create:
  airflow/dags/bahtflow_daily.py
  airflow/logs/.gitkeep
  tests/airflow/test_bahtflow_daily_dag.py
```

No GCP, dbt, Pandas, or pipeline source modules are created in this feature.

## Security and Local-Only Boundaries

`SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` is acceptable only because this feature is a localhost development/portfolio environment. The README must label it as development-only.

`.env`, generated logs, Airflow runtime state, credentials, and tokens remain ignored. Only `.env.example` and non-secret configuration templates are committed.

## Definition of Done

Feature 01 is complete only when all of the following are demonstrated:

```text
Airflow 3.3.1 image/runtime             PASS
PostgreSQL metadata connectivity        PASS
airflow db migrate                      PASS
API server                              PASS
scheduler                               PASS
DAG processor                           PASS
bahtflow_daily import                   PASS
DAG dependency contract                 PASS
single logical-date run                 PASS
3-date explicit backfill                PASS
no GCS/BigQuery/Pandas/dbt scope creep  PASS
```

At that point the branch can be merged and Feature 02 can begin GCS setup.