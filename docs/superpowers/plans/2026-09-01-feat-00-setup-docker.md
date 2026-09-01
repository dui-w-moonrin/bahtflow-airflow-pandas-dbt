# Feature 00: Reproducible Local Docker Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a reproducible, local-only Docker Compose environment with a healthy PostgreSQL service and a Python developer toolbox, ready for Airflow to be added in Feature 01.

**Architecture:** Docker Compose defines a dedicated `bahtflow` network and the persistent `postgres_data` volume. `postgres` provides PostgreSQL 16 with a health check. `toolbox` is a Python 3.12 container built from the repository and bind-mounts the project at `/opt/bahtflow`; it exists only to prove image building, project mounts, and environment wiring before Airflow is introduced.

**Tech Stack:** Docker Desktop (Linux containers), Docker Compose, PostgreSQL 16, Python 3.12-slim, PowerShell.

**Spec:** `docs/superpowers/specs/2026-09-01-feat-00-setup-docker-design.md`

## Global Constraints

- Work only on `feat/00-setup-docker`; merge into `main` only after the feature's verification passes.
- Do not add Apache Airflow, GCP/GCS, BigQuery, dbt, a business DAG, or cloud credentials in this feature.
- Keep real `.env` files untracked; publish only `.env.example` with local development defaults.
- Use `postgres:16` with named volume `postgres_data`, network `bahtflow`, and a `pg_isready` health check.
- Use a Python 3.12 toolbox service that mounts the repository at `/opt/bahtflow`.
- Preserve the existing data corpus and Python test suite unchanged.

---

## File Structure

```text
docker-compose.yml                 # Compose services, network, named volume, and health checks
docker/toolbox.Dockerfile          # Minimal Python 3.12 developer toolbox image
.dockerignore                      # Excludes build-irrelevant and sensitive local material
.env.example                       # Non-secret local Compose configuration contract
README.md                          # Local Docker start, verify, and stop instructions
docs/superpowers/specs/...         # Approved F00 design
docs/superpowers/plans/...         # This implementation plan
```

## Task 1: Create the Local Docker Compose Contract

**Files:**
- Create: `docker-compose.yml`
- Create: `docker/toolbox.Dockerfile`
- Create: `.dockerignore`
- Create: `.env.example`

**Interfaces:**
- Consumes: a local `.env` compatible with `.env.example` and the repository root as Docker build context.
- Produces: `postgres` and `toolbox` Compose services on network `bahtflow`, plus named volume `postgres_data`.
- Configuration keys: `COMPOSE_PROJECT_NAME`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`.

- [ ] **Step 1: Verify the Compose contract does not yet exist**

Run:

```powershell
docker compose --env-file .env.example config
```

Expected: FAIL because `docker-compose.yml` and `.env.example` do not yet exist.

- [ ] **Step 2: Create the local environment contract**

Create `.env.example` with exactly:

```dotenv
COMPOSE_PROJECT_NAME=bahtflow
POSTGRES_DB=bahtflow
POSTGRES_USER=bahtflow
POSTGRES_PASSWORD=bahtflow_dev_password
POSTGRES_PORT=5432
```

Create `docker/toolbox.Dockerfile` with a Python 3.12-slim base image, a working directory of `/opt/bahtflow`, and a long-running default command:

```dockerfile
FROM python:3.12-slim

WORKDIR /opt/bahtflow

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["sleep", "infinity"]
```

Create `docker-compose.yml` with these exact service contracts:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - bahtflow
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 5s
      timeout: 5s
      retries: 10

  toolbox:
    build:
      context: .
      dockerfile: docker/toolbox.Dockerfile
    working_dir: /opt/bahtflow
    volumes:
      - ./:/opt/bahtflow
    networks:
      - bahtflow
    depends_on:
      postgres:
        condition: service_healthy

networks:
  bahtflow:
    name: ${COMPOSE_PROJECT_NAME}_network

volumes:
  postgres_data:
    name: ${COMPOSE_PROJECT_NAME}_postgres_data
```

Create `.dockerignore` with:

```text
.git
.env
.env.*
!.env.example
.venv
__pycache__
.pytest_cache
.pytest_tmp
data
airflow
fx
```

- [ ] **Step 3: Verify Compose rendering and image build**

Run:

```powershell
docker compose --env-file .env.example config
docker compose --env-file .env.example build toolbox
```

Expected: both commands exit 0; rendered configuration contains exactly `postgres` and `toolbox`, network `bahtflow`, and volume `postgres_data`.

- [ ] **Step 4: Review and commit the Compose contract**

Run:

```powershell
git diff --check
git diff -- docker-compose.yml docker/toolbox.Dockerfile .dockerignore .env.example
```

Commit:

```powershell
git add docker-compose.yml docker/toolbox.Dockerfile .dockerignore .env.example
git commit -m "Add local Docker Compose environment"
```

## Task 2: Verify Runtime Behaviour and Document Local Use

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: the Compose contract from Task 1 and a real `.env` copied from `.env.example`.
- Produces: documented commands that start, verify, and stop the local environment without leaving containers running.

- [ ] **Step 1: Verify runtime starts without a current Docker section**

Run:

```powershell
docker compose --env-file .env.example up -d --build
docker compose --env-file .env.example ps
```

Expected: services start; the repository README has no Local Docker Environment section yet.

- [ ] **Step 2: Add concise operational documentation**

Append a `## Local Docker environment` section to `README.md` containing these exact command groups:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
docker compose exec postgres pg_isready -U bahtflow -d bahtflow
docker compose exec toolbox python --version
docker compose down
```

State that F00 is local-only and that Airflow, GCP, BigQuery, and dbt are intentionally deferred to later features.

- [ ] **Step 3: Run the Definition of Done checks**

Run:

```powershell
docker compose --env-file .env.example config
docker compose --env-file .env.example up -d --build
docker compose --env-file .env.example ps
docker compose --env-file .env.example exec postgres pg_isready -U bahtflow -d bahtflow
docker compose --env-file .env.example exec toolbox python --version
& 'C:\workspace\projects\bahtflow-airflow-pandas-dbt\.venv\Scripts\python.exe' -m pytest tests -v --basetemp .pytest_tmp
docker compose --env-file .env.example down
```

Expected: PostgreSQL reports it is accepting connections; toolbox reports Python 3.12; all existing pytest tests pass; `docker compose down` exits 0.

- [ ] **Step 4: Review, clean temporary files, and commit documentation**

Remove only the test temporary directory after confirming it resolves to `<worktree>/.pytest_tmp`:

```powershell
Remove-Item -LiteralPath .pytest_tmp -Recurse -Force
```

Then run:

```powershell
git diff --check
git status --short
```

Commit:

```powershell
git add README.md
git commit -m "Document local Docker environment"
```

## Task 3: Merge the Verified Feature

**Files:**
- Modify: Git history only.

**Interfaces:**
- Consumes: two committed F00 tasks with passing Docker and Python verification.
- Produces: F00 merged into `main`; the `feat/00-setup-docker` branch can be removed only after the merged result is green.

- [ ] **Step 1: Review the complete feature diff**

Run:

```powershell
git diff main...HEAD --check
git diff --stat main...HEAD
git log --oneline main..HEAD
```

Expected: only F00 Docker contract, documentation, and F00 design/plan files differ from `main`.

- [ ] **Step 2: Merge locally into main**

Run from the primary checkout:

```powershell
git checkout main
git merge --no-ff feat/00-setup-docker -m "Merge feature local Docker environment"
```

Expected: a merge commit is created without conflicts.

- [ ] **Step 3: Re-run verification on merged main**

Run the Task 2 Definition of Done command set from `main`.

Expected: Docker and pytest verification remain green after the merge.

- [ ] **Step 4: Remove the feature branch and push main**

Run:

```powershell
git branch -d feat/00-setup-docker
git push origin main
git ls-remote --heads origin main
```

Expected: remote `main` resolves to the merged commit and the feature branch is deleted locally.

