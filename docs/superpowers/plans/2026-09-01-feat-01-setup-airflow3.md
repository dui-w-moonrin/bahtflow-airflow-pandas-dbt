# Feature 01: Airflow 3 Setup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a local Apache Airflow 3.3.1 runtime that renders the final BahtFlow DAG skeleton and proves bounded logical-date backfill without introducing GCP, Pandas, BigQuery, or dbt behavior.

**Architecture:** Reuse the Feature 00 PostgreSQL service and Docker network. Add official Airflow 3.3.1 API server, scheduler, DAG processor, and one-shot migration services using LocalExecutor; mount a single EmptyOperator DAG that encodes the future pipeline dependency contract.

**Tech Stack:** Docker Compose, PostgreSQL 16, Apache Airflow 3.3.1 on Python 3.12, LocalExecutor, SimpleAuthManager, pytest.

**Spec:** `docs/superpowers/specs/2026-09-01-feat-01-setup-airflow3-design.md`

## Global Constraints

- Pin `apache/airflow:3.3.1-python3.12`.
- Use `LocalExecutor`; do not add Redis or Celery.
- Reuse PostgreSQL 16 from Feature 00.
- Use `http://airflow-api-server:8080/execution/` as the internal Execution API URL.
- Use `Asia/Bangkok` as Airflow default timezone and DAG timezone.
- Use SimpleAuthManager all-admin mode only for this local development feature.
- Set `catchup=False`; historical runs are explicit backfills.
- Do not add GCS, BigQuery, Pandas, dbt, transaction ingestion, FX ingestion, or data-quality implementation.

---

### Task 1: Encode the DAG contract test-first

**Files:**
- Create: `tests/airflow/test_bahtflow_daily_dag.py`
- Create: `airflow/dags/bahtflow_daily.py`

**Interfaces:**
- Produces DAG ID `bahtflow_daily`.
- Produces task IDs `start`, `discover_tx_files`, `validate_tx_files`, `load_tx_raw`, `discover_fx`, `validate_fx`, `load_fx_raw`, `dbt_transform`, `dbt_test`, `reconcile`, `finish`.
- Produces daily schedule, `catchup=False`, `Asia/Bangkok` start date, and only `EmptyOperator` tasks.

- [ ] **Step 1: Write the failing DAG tests** that import `airflow/dags/bahtflow_daily.py`, assert the exact task-id set, daily schedule/catchup contract, and direct downstream dependency map.
- [ ] **Step 2: Run `python -m pytest tests/airflow/test_bahtflow_daily_dag.py -v --basetemp .pytest_tmp` in an Airflow-enabled environment and verify RED** because the DAG file does not exist yet.
- [ ] **Step 3: Implement the minimal DAG** using `airflow.sdk.DAG`, `airflow.providers.standard.operators.empty.EmptyOperator`, and `pendulum.datetime(2025, 7, 22, tz="Asia/Bangkok")`.
- [ ] **Step 4: Re-run the focused test and verify GREEN.**
- [ ] **Step 5: Commit** with message `Add Airflow DAG skeleton contract`.

### Task 2: Add Airflow 3 Compose services

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env.example`
- Modify: `.gitignore`
- Create: `airflow/logs/.gitkeep`

**Interfaces:**
- Adds services `airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`.
- All Airflow services mount `./airflow/dags:/opt/airflow/dags` and `./airflow/logs:/opt/airflow/logs`.
- API server publishes `${AIRFLOW_API_PORT}:8080`.

- [ ] **Step 1: Extend `.env.example`** with `AIRFLOW_API_PORT=8080`, `AIRFLOW_UID=50000`, and a non-secret development-only Fernet key placeholder only if required by runtime validation; do not commit a generated private credential.
- [ ] **Step 2: Extend `.gitignore`** with `airflow/logs/*` and `!airflow/logs/.gitkeep`.
- [ ] **Step 3: Add a shared Compose Airflow environment** containing LocalExecutor, PostgreSQL SQLAlchemy connection, `LOAD_EXAMPLES=False`, SimpleAuth all-admin mode, internal Execution API URL, API base URL, and Bangkok timezone.
- [ ] **Step 4: Add `airflow-init` with `airflow db migrate` and `restart: "no"`; make long-running Airflow services depend on successful init and healthy PostgreSQL.**
- [ ] **Step 5: Add API server health check against `/api/v2/monitor/health`; add scheduler and DAG processor commands.**
- [ ] **Step 6: Run `docker compose config --quiet` and verify exit code 0.**
- [ ] **Step 7: Commit** with message `Add Airflow 3 Docker services`.

### Task 3: Prove runtime and bounded backfill

**Files:**
- Runtime verification only; no production-code change unless evidence reveals a defect.

**Interfaces:**
- Uses the Compose services and DAG from Tasks 1-2.

- [ ] **Step 1: Run `docker compose up airflow-init` and verify migration exits 0.**
- [ ] **Step 2: Run `docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor` and verify all long-running Airflow services remain up.**
- [ ] **Step 3: Run `docker compose exec airflow-scheduler airflow version` and verify `3.3.1`.**
- [ ] **Step 4: Run `docker compose exec airflow-scheduler airflow dags list` and verify `bahtflow_daily` is present with no import errors.**
- [ ] **Step 5: Trigger one logical-date run using Airflow CLI/API and verify all EmptyOperator tasks finish successfully.**
- [ ] **Step 6: Run `airflow backfill create --dag-id bahtflow_daily --from-date 2025-07-22 --to-date 2025-07-24` and verify exactly the intended three partition dates succeed.**
- [ ] **Step 7: Verify the API health endpoint reports healthy metadata database, scheduler, and DAG processor.**

### Task 4: Document the developer workflow

**Files:**
- Modify: `README.md`

**Interfaces:**
- Documents fresh-clone startup, Airflow UI URL, service checks, one-day execution, three-day backfill, and shutdown commands.

- [ ] **Step 1: Add an Airflow 3 section** after the Feature 00 Docker section with exact PowerShell commands for `.env`, init, startup, health, DAG listing, bounded backfill, and shutdown.
- [ ] **Step 2: State explicitly that SimpleAuth all-admin mode is development-only and that F01 has no GCP/Pandas/dbt behavior.**
- [ ] **Step 3: Re-run the credential-free Python tests and `docker compose config --quiet`.**
- [ ] **Step 4: Review `git diff` for scope creep and secrets.**
- [ ] **Step 5: Commit** with message `Document Airflow 3 local workflow`.

## Final Verification

Run and record evidence for:

```powershell
docker compose config --quiet
docker compose up airflow-init
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow dags list
python -m pytest tests -v --basetemp .pytest_tmp
```

Then create and inspect the bounded three-day backfill for `2025-07-22` through `2025-07-24`. Do not merge until runtime evidence confirms the API server, scheduler, DAG processor, DAG import, and backfill behavior.