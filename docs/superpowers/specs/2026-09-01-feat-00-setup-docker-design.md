# Feature 00: Reproducible Local Docker Environment

## Purpose

Provide the reproducible Docker plumbing that later features will use for Airflow. This feature deliberately does **not** introduce Airflow, Google Cloud access, dbt, or a business DAG.

## Scope

The Docker Compose project will start two services on a dedicated `bahtflow` network:

- `postgres`: PostgreSQL 16 for future Airflow metadata, backed by the named `postgres_data` volume and protected by a health check using `pg_isready`.
- `toolbox`: a small Python 3.12 development container built from the repository. It mounts the repository at `/opt/bahtflow`, loads non-secret development configuration from `.env`, and remains running for diagnostic commands.

`toolbox` is intentionally a generic developer container. Airflow is added only in Feature 01, where it will reuse the network, PostgreSQL connection settings, and bind-mount convention defined here.

## Files

- `docker-compose.yml`: service, network, volume, health-check, bind-mount, and environment wiring.
- `docker/toolbox.Dockerfile`: minimal Python 3.12 image for the toolbox service.
- `.dockerignore`: keeps local virtual environments, Git metadata, caches, secrets, and large source fixtures out of image build context.
- `.env.example`: documented, non-secret defaults for Compose project name, PostgreSQL database/user/password, and host port.
- `README.md`: a concise Local Docker Environment section with start, verify, and stop commands.

The real `.env` remains untracked under the existing `.gitignore` rule.

## Configuration Contract

The default environment is local-only:

```text
POSTGRES_DB=bahtflow
POSTGRES_USER=bahtflow
POSTGRES_PASSWORD=bahtflow_dev_password
POSTGRES_PORT=5432
```

These values are development defaults, never production credentials. Feature 01 may consume them through a PostgreSQL connection string but must not change their names without an explicit migration.

## Runtime Behaviour

```text
Docker Desktop
  |
  +-- postgres (named volume, health check)
  |
  +-- toolbox (bind-mounted repository)
```

All services must start without a crash loop. `postgres` must become healthy; `toolbox` must be able to see the mounted repository and execute Python.

## Verification

The Definition of Done is demonstrated with:

```powershell
Copy-Item .env.example .env
docker compose config
docker compose up -d --build
docker compose ps
docker compose exec postgres pg_isready -U bahtflow -d bahtflow
docker compose exec toolbox python --version
docker compose down
```

The repository's existing Python tests must continue to pass. No cloud account, service account, bucket, BigQuery dataset, Airflow package, or dbt package is created in this feature.
