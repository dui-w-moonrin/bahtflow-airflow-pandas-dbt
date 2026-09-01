# Feature 02: GCS Setup Design

## Status

Approved in chat on 2026-09-01 and ready for implementation planning after user review of this written spec.

## Purpose

Add the first real Google Cloud boundary to BahtFlow while keeping orchestration local. Feature 02 proves that local Docker/Airflow code can authenticate to Google Cloud Storage through Application Default Credentials (ADC) with service-account impersonation, address a configured landing bucket, and upload the committed source corpus with immutable checksum protection.

This feature is intentionally GCS-only. It does not create BigQuery datasets/tables, dbt resources, Pandas ingestion logic, Airflow business tasks, retries, alerts, or the full historical execution path. Those concerns remain in later features so the cloud-storage boundary can be tested and debugged independently.

## Scope Decision Relative to the Older Roadmap

An earlier roadmap version grouped GCS landing and BigQuery warehouse bootstrap into one feature. The approved Feature 02 scope is narrower:

```text
F02: GCS auth + bucket + landing layout + immutable upload
F03+: BigQuery/dbt/raw-load concerns
```

The dedicated implementation plan may update roadmap wording so repository documentation matches this approved split, but it must not add BigQuery implementation to Feature 02.

## Authentication Decision

Normal BahtFlow runtime access uses:

```text
Application Default Credentials
        +
Service Account Impersonation
```

The local developer authenticates with Google Cloud CLI and creates ADC capable of impersonating the configured BahtFlow runtime service account. Python code then uses the normal Google authentication chain; it does not read a committed service-account JSON key.

Conceptually, normal runtime access is:

```text
Dui / developer Google identity
            |
            | Service Account Token Creator
            v
BahtFlow runtime service account
            |
            | short-lived impersonated credentials
            v
Application Default Credentials
            |
            v
Docker / Airflow / Python
            |
            v
Google Cloud Storage objects
```

The exact local `gcloud` login command is operational documentation, not application logic. The application must rely on ADC rather than shelling out to `gcloud`.

Bucket creation is a separate bootstrap concern. If the bucket does not exist, it is created using developer/admin ADC with the project-level permission needed to create a bucket. The runtime service account is not granted project-wide bucket-creation permission merely to satisfy bootstrap.

The application does not switch identities internally. The operator chooses the appropriate ADC context before running a bootstrap or runtime verification command.

## Credential Boundary

Two roles are deliberately separated.

### Bootstrap identity

The developer/admin identity may create or inspect the bucket and configure IAM during initial setup. This identity is used only for bootstrap operations that require project-level administrative permission.

### Runtime identity

The configured runtime service account is the identity used by Python/Airflow through impersonated ADC for normal object operations after the bucket exists.

The runtime identity should receive only the permissions required for the approved object-level workflow. It must not require bucket IAM administration, project-wide bucket creation, owner, or editor permissions.

Feature 02 must not commit:

- service-account JSON keys;
- ADC files;
- access tokens;
- refresh tokens;
- private keys;
- real `.env` values.

## Environment Contract

Configuration is environment-driven. At minimum Feature 02 introduces these logical settings:

```text
BAHTFLOW_GCP_PROJECT
BAHTFLOW_GCS_BUCKET
BAHTFLOW_GCP_LOCATION
BAHTFLOW_RUNTIME_SERVICE_ACCOUNT
```

`.env.example` may contain safe example values/placeholders. The real `.env` remains ignored.

`pipeline/config.py` owns parsing and validation of this configuration. Other modules consume a validated configuration object rather than repeatedly reading `os.environ` throughout the codebase.

Validation failures must be explicit and early. Missing required values, blank values, or obviously invalid configuration must fail before a network operation is attempted.

`BAHTFLOW_RUNTIME_SERVICE_ACCOUNT` documents and validates the identity expected for runtime impersonation. Python client construction still follows ADC; the application does not load a key for that account and does not perform a second in-process impersonation layer.

## GCS Landing Layout

Feature 02 uses one configured project bucket for the BahtFlow landing zone. Bucket name is never hardcoded in source code.

Object layout:

```text
gs://<BAHTFLOW_GCS_BUCKET>/
|
+-- transactions/
|   +-- business_date=YYYY-MM-DD/
|       +-- sales_bkk_YYYYMMDD.csv.gz
|       +-- sales_central_YYYYMMDD.csv.gz
|       +-- sales_north_YYYYMMDD.csv.gz
|       +-- sales_northeast_YYYYMMDD.csv.gz
|       +-- sales_south_YYYYMMDD.csv.gz
|
+-- fx/
|   +-- YYYY/
|       +-- MM/
|           +-- fx_YYYYMMDD.csv
|
+-- manifests/
    +-- daily_source_manifest.csv
    +-- fx_manifest.csv
```

The object layout mirrors the already approved local source contracts closely enough that a transaction business date and FX published date remain visible from the object path.

Object-path generation is implemented as pure logic and covered by credential-free tests.

## Immutable Landing Rule

The landing bucket is treated as source evidence, not as scratch storage. Uploading a source path follows exactly three outcomes:

```text
object absent
    -> upload object
    -> attach source SHA-256 metadata

object present + stored SHA-256 == local SHA-256
    -> skip
    -> report unchanged/idempotent

object present + stored SHA-256 != local SHA-256
    -> hard fail
    -> never overwrite silently
```

The comparison uses a SHA-256 digest calculated from the exact local file bytes. The digest is stored in object metadata under one documented metadata key chosen during implementation and used consistently by upload and verification code.

The design does not depend on provider-specific ETag semantics for source identity. The committed/source SHA-256 is the authoritative content identity.

A missing checksum metadata field on an already-existing target object is treated as unsafe ambiguity and fails rather than assuming the object matches.

## Components

Feature 02 follows the approved option: thin GCS adapter plus pure logic. It deliberately avoids both extremes of scattering Google SDK calls throughout scripts and creating a large generic repository/service abstraction.

### `pipeline/config.py`

Responsibilities:

- read Feature 02 environment configuration;
- validate required values;
- expose one small validated settings object;
- contain no GCS network calls.

### Pure landing/path/checksum logic

A small focused module may be introduced if needed by the implementation plan. Responsibilities:

- map a local transaction source path to its GCS object path;
- map a local FX source path to its GCS object path;
- map committed manifests to their `manifests/` object paths;
- compute local SHA-256;
- decide upload action from local checksum versus remote object metadata.

This logic must be testable without Google credentials or network access.

### Thin GCS adapter

A small adapter wraps only the Google Cloud Storage operations Feature 02 needs, such as:

- get/check bucket;
- inspect one object and its metadata;
- upload one object with metadata;
- list objects for verification;
- optionally download/read a small smoke-test object for live verification.

The adapter should expose a narrow interface so tests can substitute a fake/stub implementation without mocking internal Google SDK behavior at many layers.

### `scripts/bootstrap_gcs.py`

Responsibilities:

- load validated configuration;
- authenticate through the currently selected ADC context;
- check whether the configured bucket exists;
- create the bucket only when absent and when the executing identity is the developer/admin bootstrap identity with the required permission;
- verify that an existing bucket uses the configured location;
- surface an explicit error when the existing bucket location does not match configuration.

The script does not grant project-wide bucket-creation permission to the runtime service account. After one-time creation/IAM setup, normal verification and uploads use impersonated runtime ADC.

Bootstrap does not change bucket IAM automatically unless the implementation plan identifies a minimal, testable reason to do so. IAM grant/setup steps may remain documented operator actions.

### `scripts/upload_landing_sources.py`

Responsibilities:

- enumerate the committed transaction, FX, and manifest sources approved for landing;
- map each source file to its canonical object path;
- calculate SHA-256;
- inspect the target object;
- apply the immutable landing rule;
- upload only absent objects;
- produce a clear uploaded/skipped/failed summary.

It must not parse or clean transaction business values. Source content is copied byte-for-byte.

## Data Flow

Landing upload:

```text
committed source file
        |
        v
canonical object-path mapping
        |
        v
local SHA-256
        |
        v
thin GCS adapter -> inspect target object
        |
        +-- absent ----------------------> upload + SHA-256 metadata
        |
        +-- same checksum --------------> skip
        |
        +-- different/missing checksum -> hard fail
```

Bootstrap identity flow:

```text
developer/admin ADC
      |
      v
bucket lookup
      |
      +-- absent -> create in configured location
      |
      +-- present + location matches -> keep
      |
      +-- present + location differs -> fail
```

Normal runtime identity flow after bootstrap:

```text
developer login
      |
      v
ADC impersonating runtime service account
      |
      v
Docker / Python Storage client
      |
      v
list/read/upload approved landing objects
```

## Error Handling

Feature 02 favors explicit failure over silent repair.

Hard failures include:

- missing/blank required configuration;
- ADC unavailable;
- service-account impersonation denied or invalid for runtime verification;
- bootstrap identity lacks bucket-creation permission when creation is required;
- target bucket inaccessible;
- existing bucket location differs from configured location;
- expected local source file missing;
- local path cannot be mapped to the approved object contract;
- existing object lacks the expected SHA-256 metadata;
- existing object checksum differs from local content;
- upload/read/list permission denied.

Transient Google/network errors are allowed to surface from the Google client library in Feature 02. A general retry framework is intentionally deferred because later Airflow orchestration owns retry policy.

No error handler may automatically overwrite a conflicting landing object.

## Testing Strategy

Testing is intentionally split between credential-free automated verification and live GCP integration verification.

### Credential-free tests: assistant/CI/local pytest

Automated tests must not require a Google account, ADC file, service-account key, or live bucket.

They cover at least:

- required config validation;
- transaction object-path mapping;
- FX object-path mapping;
- manifest object-path mapping;
- SHA-256 calculation from file bytes;
- absent object -> upload decision;
- same checksum -> skip decision;
- different checksum -> fail decision;
- missing remote checksum metadata -> fail decision;
- source files are not modified/cleaned during landing logic;
- secrets/key files remain excluded by repository ignore rules where applicable.

Tests use pure functions and a small fake/stub storage interface rather than deep mocks of Google SDK internals.

### Live GCP verification: Dui/local environment

Live verification is performed from Dui's environment because it owns the GCP project and credentials.

Required evidence is split into bootstrap and runtime phases.

Bootstrap phase, using developer/admin ADC when needed:

1. The configured bucket can be created if absent, or verified if already present.
2. The resulting bucket location matches `BAHTFLOW_GCP_LOCATION`.
3. Required runtime IAM is configured without giving the runtime service account project-wide owner/editor or bucket-creation privileges.

Runtime phase, using ADC that impersonates `BAHTFLOW_RUNTIME_SERVICE_ACCOUNT`:

4. ADC with service-account impersonation is configured successfully.
5. The Docker/runtime environment can resolve ADC without copying a JSON key into the repository or image.
6. The Python Storage client authenticates to GCS as the runtime path.
7. A sample source object can be uploaded to its canonical path.
8. The object can be listed/read back.
9. SHA-256 metadata can be read back and matches the local source file.
10. Re-running the same upload skips the unchanged object.
11. A deliberate checksum mismatch test fails without overwriting the remote object.

The mismatch proof should use a disposable test object/path or controlled test fixture. It must not corrupt a real committed landing object merely to demonstrate failure behavior.

## Docker / ADC Integration Boundary

The committed repository must not assume a developer-specific home directory. The implementation plan must choose a portable way for the local Docker services that need GCP access to see ADC, while keeping credentials outside the repository and mounting them read-only where practical.

The design requirement is behavioral rather than path-specific:

```text
host ADC exists
    -> selected Docker service can resolve ADC
    -> Python Google client authenticates
    -> no credential copied into image/repo
```

Only services that need GCP access should receive the credential mount/configuration. Feature 02 does not need to grant GCP credentials to PostgreSQL.

## Security Principles

- Prefer short-lived impersonated credentials to service-account JSON keys for runtime access.
- Use developer/admin ADC only for one-time bootstrap operations that genuinely require project-level permission.
- Keep runtime permissions least-privilege.
- Keep bootstrap/admin permissions separate from normal pipeline runtime permissions.
- Never commit ADC or service-account credentials.
- Never print access tokens or credential file contents in tests/docs.
- Do not ask users to paste secret/token values into issue logs or chat.
- Treat landing objects as immutable evidence.

## Repository Changes

Expected implementation footprint is intentionally small. The implementation plan may refine exact filenames while preserving responsibilities.

```text
Modify:
  .env.example
  .gitignore                 # only if additional credential patterns are needed
  docker-compose.yml         # only services that need ADC/GCP access
  README.md
  roadmap documentation      # if needed to reflect GCS-only F02 split

Create:
  pipeline/__init__.py       # if package does not already exist
  pipeline/config.py
  pipeline/gcs_landing.py    # pure path/checksum/upload-decision logic, name may be refined
  pipeline/gcs_adapter.py    # thin Google Storage boundary, name may be refined
  scripts/bootstrap_gcs.py
  scripts/upload_landing_sources.py
  tests/pipeline/test_config.py
  tests/pipeline/test_gcs_landing.py
```

Feature 02 must not create BigQuery schemas, dbt models, real Pandas intake code, or Airflow business operators.

## Definition of Done

Feature 02 is complete only when all of the following have fresh verification evidence:

```text
[ ] credential-free pytest suite passes
[ ] docker compose config succeeds
[ ] no secret/key/credential file is committed
[ ] required GCP/GCS config is validated centrally
[ ] transaction, FX, and manifest paths map to the approved GCS layout
[ ] SHA-256 is stored and used as immutable source identity
[ ] developer/admin bootstrap can create or verify the configured bucket without expanding runtime privileges
[ ] ADC + service-account impersonation works for runtime access in Dui's local environment
[ ] required Docker/Python runtime can authenticate to GCS without a JSON key in repo/image
[ ] configured bucket exists in the configured location
[ ] absent source object uploads successfully
[ ] unchanged existing object is skipped on rerun
[ ] conflicting or checksum-ambiguous existing object hard-fails without overwrite
[ ] live upload/list/read/checksum smoke test succeeds
```

Passing credential-free tests alone is not enough to complete Feature 02; the live GCP boundary must also be demonstrated from Dui's environment.

## Explicit Non-Goals

Feature 02 does not implement:

- BigQuery dataset/table bootstrap;
- dbt Core configuration or models;
- Pandas transaction/FX intake;
- Airflow replacement of EmptyOperators with GCP work;
- retry/backoff orchestration;
- alerting;
- Cloud Composer;
- Terraform;
- Kubernetes;
- GCS lifecycle/retention-policy architecture beyond the immutable behavior enforced by application logic;
- the full 360-day historical pipeline run.

The result of Feature 02 is one verified, secure, idempotent local-to-GCS landing boundary that later ingestion features can depend on.
