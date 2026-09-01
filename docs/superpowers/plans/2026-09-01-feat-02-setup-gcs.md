# Feature 02 GCS Setup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a credential-safe, idempotent local-to-GCS landing boundary that uses ADC with service-account impersonation and rejects conflicting source objects by SHA-256.

**Architecture:** Keep source-path/checksum decisions as pure Python, place Google Cloud Storage calls behind one thin adapter, and keep CLI scripts as orchestration wrappers. Automated tests remain credential-free; live GCP verification runs from Dui's machine with ADC mounted read-only into a dedicated Docker `gcp-toolbox` service.

**Tech Stack:** Python 3.12, `google-cloud-storage==3.13.1`, Google Application Default Credentials, Google Cloud Storage, Docker Compose, pytest.

**Spec:** `docs/superpowers/specs/2026-09-01-feat-02-setup-gcs-design.md`

## Global Constraints

- Feature 02 is GCS-only; do not add BigQuery, dbt, Pandas intake, Airflow business operators, retries, alerts, Terraform, Kubernetes, or the full 360-day run.
- Authentication is Application Default Credentials with service-account impersonation for runtime object access; application code must not shell out to `gcloud`.
- Bootstrap/admin identity and runtime service-account permissions stay separate.
- Never commit service-account JSON keys, ADC files, access tokens, refresh tokens, private keys, or real `.env` values.
- Bucket name, project, location, runtime service account, and host ADC path are environment-configured; no developer-specific path is hardcoded in Python.
- Landing objects are immutable evidence: absent -> upload, same source SHA-256 -> skip, different or missing remote SHA-256 -> hard fail.
- SHA-256 is calculated from exact file bytes and stored in GCS custom metadata key `bahtflow-source-sha256`.
- Existing bucket location mismatch is a hard failure.
- Google/network transient failures surface directly in F02; do not add a retry framework.
- Credential-free pytest must run without Google credentials or a live bucket.
- Live GCP verification must use a disposable mismatch object/path; never corrupt a committed landing object for a negative test.

---

## File Structure

```text
Modify:
  .env.example
  .gitignore
  docker-compose.yml
  docker/toolbox.Dockerfile
  README.md

Create:
  requirements-gcp.txt
  pipeline/__init__.py
  pipeline/config.py
  pipeline/gcs_landing.py
  pipeline/gcs_adapter.py
  pipeline/gcs_workflows.py
  scripts/bootstrap_gcs.py
  scripts/upload_landing_sources.py
  scripts/verify_gcs_live.py
  tests/pipeline/test_config.py
  tests/pipeline/test_gcs_landing.py
  tests/pipeline/test_gcs_workflows.py
```

Responsibilities:

- `pipeline/config.py`: parse and validate F02 environment settings only.
- `pipeline/gcs_landing.py`: pure source discovery, canonical object names, SHA-256, immutable upload decision.
- `pipeline/gcs_adapter.py`: narrow Google SDK wrapper; no source-contract decisions.
- `pipeline/gcs_workflows.py`: combine pure landing rules with the adapter; unit-test with a fake adapter.
- `scripts/bootstrap_gcs.py`: admin/bootstrap bucket create-or-verify CLI.
- `scripts/upload_landing_sources.py`: runtime landing upload CLI with bounded `--smoke` mode.
- `scripts/verify_gcs_live.py`: live read/checksum/conflict proof using a disposable object.
- `gcp-toolbox`: the only F02 Compose service that receives the ADC bind mount.

---

### Task 1: GCP Dependency and Central Configuration

**Files:**
- Create: `requirements-gcp.txt`
- Create: `pipeline/__init__.py`
- Create: `pipeline/config.py`
- Create: `tests/pipeline/test_config.py`
- Modify: `docker/toolbox.Dockerfile`
- Modify: `.env.example`

**Interfaces:**
- Produces `GcpSettings(project_id: str, bucket_name: str, location: str, runtime_service_account: str)`.
- Produces `load_gcp_settings(env: Mapping[str, str] | None = None) -> GcpSettings`.

- [ ] **Step 1: Write failing config tests**

Create `tests/pipeline/test_config.py`:

```python
import pytest

from pipeline.config import GcpConfigError, GcpSettings, load_gcp_settings


VALID_ENV = {
    "BAHTFLOW_GCP_PROJECT": "bahtflow-dev",
    "BAHTFLOW_GCS_BUCKET": "bahtflow-dev-landing",
    "BAHTFLOW_GCP_LOCATION": "asia-southeast1",
    "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT": "bahtflow-runtime@bahtflow-dev.iam.gserviceaccount.com",
}


def test_load_gcp_settings_returns_validated_settings():
    assert load_gcp_settings(VALID_ENV) == GcpSettings(
        project_id="bahtflow-dev",
        bucket_name="bahtflow-dev-landing",
        location="asia-southeast1",
        runtime_service_account="bahtflow-runtime@bahtflow-dev.iam.gserviceaccount.com",
    )


@pytest.mark.parametrize("missing_key", VALID_ENV)
def test_load_gcp_settings_rejects_missing_required_values(missing_key):
    env = VALID_ENV.copy()
    env.pop(missing_key)
    with pytest.raises(GcpConfigError, match=missing_key):
        load_gcp_settings(env)


def test_load_gcp_settings_rejects_blank_values():
    with pytest.raises(GcpConfigError, match="BAHTFLOW_GCS_BUCKET"):
        load_gcp_settings(VALID_ENV | {"BAHTFLOW_GCS_BUCKET": "   "})
```

- [ ] **Step 2: Run RED**

```powershell
python -m pytest tests/pipeline/test_config.py -v
```

Expected: import/collection failure because `pipeline.config` does not exist.

- [ ] **Step 3: Add reproducible GCS dependency and toolbox install**

Create `requirements-gcp.txt`:

```text
google-cloud-storage==3.13.1
```

Update `docker/toolbox.Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /opt/bahtflow

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt

CMD ["sleep", "infinity"]
```

Create empty `pipeline/__init__.py`.

- [ ] **Step 4: Implement config**

Create `pipeline/config.py`:

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class GcpConfigError(ValueError):
    pass


@dataclass(frozen=True)
class GcpSettings:
    project_id: str
    bucket_name: str
    location: str
    runtime_service_account: str


_REQUIRED_ENV = {
    "BAHTFLOW_GCP_PROJECT": "project_id",
    "BAHTFLOW_GCS_BUCKET": "bucket_name",
    "BAHTFLOW_GCP_LOCATION": "location",
    "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT": "runtime_service_account",
}


def load_gcp_settings(env: Mapping[str, str] | None = None) -> GcpSettings:
    source = os.environ if env is None else env
    values: dict[str, str] = {}
    for env_name, field_name in _REQUIRED_ENV.items():
        raw = source.get(env_name)
        if raw is None or not raw.strip():
            raise GcpConfigError(f"Missing or blank required setting: {env_name}")
        values[field_name] = raw.strip()
    return GcpSettings(**values)
```

- [ ] **Step 5: Extend `.env.example`**

Append only safe examples:

```text
BAHTFLOW_GCP_PROJECT=your-gcp-project-id
BAHTFLOW_GCS_BUCKET=your-globally-unique-bahtflow-bucket
BAHTFLOW_GCP_LOCATION=asia-southeast1
BAHTFLOW_RUNTIME_SERVICE_ACCOUNT=bahtflow-runtime@your-gcp-project-id.iam.gserviceaccount.com
GOOGLE_ADC_HOST_PATH=C:/Users/YOUR_USER/AppData/Roaming/gcloud/application_default_credentials.json
```

- [ ] **Step 6: Verify GREEN**

```powershell
python -m pytest tests/pipeline/test_config.py -v
python -m py_compile pipeline/config.py
```

Expected: all tests pass; compile exits 0.

- [ ] **Step 7: Commit**

```powershell
git add requirements-gcp.txt pipeline/__init__.py pipeline/config.py tests/pipeline/test_config.py docker/toolbox.Dockerfile .env.example
git commit -m "feat: add GCP runtime configuration"
```

---

### Task 2: Pure Landing Contract

**Files:**
- Create: `pipeline/gcs_landing.py`
- Create: `tests/pipeline/test_gcs_landing.py`

**Interfaces:**
- `SOURCE_SHA256_METADATA_KEY = "bahtflow-source-sha256"`
- `LandingConflictError`
- `UploadAction.UPLOAD`, `UploadAction.SKIP`
- `LandingSource(local_path: Path, object_name: str)`
- `sha256_file(path: Path) -> str`
- `transaction_object_name(relative_path: str) -> str`
- `fx_object_name(relative_path: str) -> str`
- `manifest_object_name(local_path: Path) -> str`
- `iter_landing_sources(repo_root: Path) -> list[LandingSource]`
- `decide_upload(local_sha256: str, *, remote_exists: bool, remote_metadata: Mapping[str, str] | None) -> UploadAction`

- [ ] **Step 1: Write failing path/checksum/action tests**

Create `tests/pipeline/test_gcs_landing.py` with tests asserting:

```python
assert transaction_object_name(
    "business_date=2025-07-22/sales_bkk_20250722.csv.gz"
) == "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz"

assert fx_object_name(
    "fx_daily/2025/07/fx_20250708.csv"
) == "fx/2025/07/fx_20250708.csv"

assert manifest_object_name(
    Path("data/daily_source_manifest.csv")
) == "manifests/daily_source_manifest.csv"

assert manifest_object_name(
    Path("fx/manifest.csv")
) == "manifests/fx_manifest.csv"
```

Also test:

```python
source.write_bytes(b"raw-bytes\x00\xff")
assert sha256_file(source) == hashlib.sha256(b"raw-bytes\x00\xff").hexdigest()
```

and the exact immutable cases:

```python
assert decide_upload("abc", remote_exists=False, remote_metadata=None) is UploadAction.UPLOAD
assert decide_upload(
    "abc",
    remote_exists=True,
    remote_metadata={SOURCE_SHA256_METADATA_KEY: "abc"},
) is UploadAction.SKIP

with pytest.raises(LandingConflictError, match="checksum mismatch"):
    decide_upload(
        "abc",
        remote_exists=True,
        remote_metadata={SOURCE_SHA256_METADATA_KEY: "different"},
    )

with pytest.raises(LandingConflictError, match="missing"):
    decide_upload("abc", remote_exists=True, remote_metadata={})
```

- [ ] **Step 2: Run RED**

```powershell
python -m pytest tests/pipeline/test_gcs_landing.py -v
```

Expected: FAIL because module/functions do not exist.

- [ ] **Step 3: Implement pure path/checksum/action functions**

Create `pipeline/gcs_landing.py` with:

```python
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Mapping


SOURCE_SHA256_METADATA_KEY = "bahtflow-source-sha256"


class LandingConflictError(RuntimeError):
    pass


class UploadAction(str, Enum):
    UPLOAD = "upload"
    SKIP = "skip"


@dataclass(frozen=True)
class LandingSource:
    local_path: Path
    object_name: str


_TRANSACTION_PATTERN = re.compile(
    r"^business_date=\d{4}-\d{2}-\d{2}/sales_(bkk|central|north|northeast|south)_\d{8}\.csv\.gz$"
)
_FX_PATTERN = re.compile(r"^fx_daily/\d{4}/\d{2}/fx_\d{8}\.csv$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def transaction_object_name(relative_path: str) -> str:
    normalized = PurePosixPath(relative_path).as_posix()
    if not _TRANSACTION_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported transaction source path: {relative_path}")
    return f"transactions/{normalized}"


def fx_object_name(relative_path: str) -> str:
    normalized = PurePosixPath(relative_path).as_posix()
    if not _FX_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported FX source path: {relative_path}")
    return f"fx/{normalized.removeprefix('fx_daily/')}"


def manifest_object_name(local_path: Path) -> str:
    mapping = {
        "data/daily_source_manifest.csv": "manifests/daily_source_manifest.csv",
        "fx/manifest.csv": "manifests/fx_manifest.csv",
    }
    normalized = local_path.as_posix()
    if normalized not in mapping:
        raise ValueError(f"Unsupported manifest path: {normalized}")
    return mapping[normalized]


def decide_upload(
    local_sha256: str,
    *,
    remote_exists: bool,
    remote_metadata: Mapping[str, str] | None,
) -> UploadAction:
    if not remote_exists:
        return UploadAction.UPLOAD
    metadata = remote_metadata or {}
    remote_sha256 = metadata.get(SOURCE_SHA256_METADATA_KEY)
    if not remote_sha256:
        raise LandingConflictError("Existing object is missing source checksum metadata")
    if remote_sha256 != local_sha256:
        raise LandingConflictError("Existing object checksum mismatch")
    return UploadAction.SKIP
```

- [ ] **Step 4: Add deterministic source-discovery tests**

Build a tiny repo fixture containing exactly:

```text
data/daily_regional_sales/business_date=2025-07-22/sales_bkk_20250722.csv.gz
data/daily_source_manifest.csv
fx/fx_daily/2025/07/fx_20250708.csv
fx/manifest.csv
```

Assert sorted object names are:

```python
[
    "fx/2025/07/fx_20250708.csv",
    "manifests/daily_source_manifest.csv",
    "manifests/fx_manifest.csv",
    "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
]
```

Add two negative tests: missing `data/daily_regional_sales` raises `FileNotFoundError`; missing `fx/fx_daily` raises `FileNotFoundError`.

- [ ] **Step 5: Implement source discovery including explicit root checks**

Add:

```python
def iter_landing_sources(repo_root: Path) -> list[LandingSource]:
    transaction_root = repo_root / "data" / "daily_regional_sales"
    fx_daily_root = repo_root / "fx" / "fx_daily"
    if not transaction_root.is_dir():
        raise FileNotFoundError(transaction_root)
    if not fx_daily_root.is_dir():
        raise FileNotFoundError(fx_daily_root)

    sources: list[LandingSource] = []

    for path in transaction_root.glob("business_date=*/*.csv.gz"):
        relative = path.relative_to(transaction_root).as_posix()
        sources.append(LandingSource(path, transaction_object_name(relative)))

    fx_root = repo_root / "fx"
    for path in fx_daily_root.glob("*/*/fx_*.csv"):
        relative = path.relative_to(fx_root).as_posix()
        sources.append(LandingSource(path, fx_object_name(relative)))

    manifest_paths = [
        repo_root / "data" / "daily_source_manifest.csv",
        repo_root / "fx" / "manifest.csv",
    ]
    for path in manifest_paths:
        if not path.is_file():
            raise FileNotFoundError(path)
        sources.append(
            LandingSource(
                path,
                manifest_object_name(path.relative_to(repo_root)),
            )
        )

    return sorted(sources, key=lambda item: item.object_name)
```

- [ ] **Step 6: Verify GREEN and commit**

```powershell
python -m pytest tests/pipeline/test_gcs_landing.py -v
python -m py_compile pipeline/gcs_landing.py
git add pipeline/gcs_landing.py tests/pipeline/test_gcs_landing.py
git commit -m "feat: define immutable GCS landing contract"
```

---

### Task 3: Thin GCS Adapter and Landing Workflow

**Files:**
- Create: `pipeline/gcs_adapter.py`
- Create: `pipeline/gcs_workflows.py`
- Create: `tests/pipeline/test_gcs_workflows.py`

**Interfaces:**
- `ObjectMetadata(exists: bool, metadata: Mapping[str, str])`
- `GcsAdapter.ensure_bucket(...) -> str`
- `GcsAdapter.get_object_metadata(...) -> ObjectMetadata`
- `GcsAdapter.upload_file(...) -> None`
- `GcsAdapter.list_object_names(...) -> list[str]`
- `GcsAdapter.download_bytes(...) -> bytes`
- `GcsAdapter.delete_object(...) -> None` only for disposable live verification cleanup.
- `UploadSummary(uploaded: int, skipped: int)`
- `upload_sources(repo_root: Path, bucket_name: str, adapter, *, smoke: bool = False) -> UploadSummary`

- [ ] **Step 1: Write workflow tests with a fake storage adapter**

The fake stores object bytes + metadata in memory and implements `get_object_metadata` and `upload_file` only. Test:

```python
first = upload_sources(repo, "bucket", storage)
second = upload_sources(repo, "bucket", storage)
assert first == UploadSummary(uploaded=4, skipped=0)
assert second == UploadSummary(uploaded=0, skipped=4)
```

Assert every uploaded object has a 64-character `bahtflow-source-sha256` metadata value. Preload one transaction object with metadata `"0" * 64` and assert `LandingConflictError` is raised and that object is not uploaded again.

Add a `smoke=True` test asserting exactly four selected objects: one transaction, one FX, and both manifests.

- [ ] **Step 2: Run RED**

```powershell
python -m pytest tests/pipeline/test_gcs_workflows.py -v
```

Expected: FAIL because adapter/workflow modules do not exist.

- [ ] **Step 3: Implement the thin Google SDK adapter**

Create `pipeline/gcs_adapter.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from google.api_core.exceptions import NotFound
from google.cloud import storage


@dataclass(frozen=True)
class ObjectMetadata:
    exists: bool
    metadata: Mapping[str, str]


class GcsAdapter:
    def __init__(self, project_id: str):
        self._client = storage.Client(project=project_id)

    def ensure_bucket(self, bucket_name: str, location: str, *, create_if_missing: bool) -> str:
        try:
            bucket = self._client.get_bucket(bucket_name)
        except NotFound:
            if not create_if_missing:
                raise RuntimeError(f"GCS bucket does not exist: {bucket_name}")
            bucket = self._client.create_bucket(bucket_name, location=location)

        actual = (bucket.location or "").lower()
        expected = location.lower()
        if actual != expected:
            raise RuntimeError(
                f"GCS bucket location mismatch: expected={expected} actual={actual}"
            )
        return actual

    def get_object_metadata(self, bucket_name: str, object_name: str) -> ObjectMetadata:
        blob = self._client.bucket(bucket_name).blob(object_name)
        try:
            blob.reload()
        except NotFound:
            return ObjectMetadata(exists=False, metadata={})
        return ObjectMetadata(exists=True, metadata=dict(blob.metadata or {}))

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        local_path: Path,
        metadata: Mapping[str, str],
    ) -> None:
        blob = self._client.bucket(bucket_name).blob(object_name)
        blob.metadata = dict(metadata)
        blob.upload_from_filename(str(local_path), if_generation_match=0)

    def list_object_names(self, bucket_name: str, prefix: str = "") -> list[str]:
        return sorted(blob.name for blob in self._client.list_blobs(bucket_name, prefix=prefix))

    def download_bytes(self, bucket_name: str, object_name: str) -> bytes:
        return self._client.bucket(bucket_name).blob(object_name).download_as_bytes()

    def delete_object(self, bucket_name: str, object_name: str) -> None:
        self._client.bucket(bucket_name).blob(object_name).delete()
```

`if_generation_match=0` is mandatory race protection: GCS must reject an upload if the supposedly absent object appears after the pre-check.

- [ ] **Step 4: Implement the credential-free workflow**

Create `pipeline/gcs_workflows.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipeline.gcs_landing import (
    SOURCE_SHA256_METADATA_KEY,
    UploadAction,
    decide_upload,
    iter_landing_sources,
    sha256_file,
)


@dataclass(frozen=True)
class UploadSummary:
    uploaded: int
    skipped: int


def _smoke_subset(sources):
    one_txn = next(x for x in sources if x.object_name.startswith("transactions/"))
    one_fx = next(x for x in sources if x.object_name.startswith("fx/"))
    manifests = [x for x in sources if x.object_name.startswith("manifests/")]
    return sorted([one_txn, one_fx, *manifests], key=lambda x: x.object_name)


def upload_sources(repo_root: Path, bucket_name: str, adapter, *, smoke: bool = False) -> UploadSummary:
    sources = iter_landing_sources(repo_root)
    if smoke:
        sources = _smoke_subset(sources)

    uploaded = 0
    skipped = 0
    for source in sources:
        local_sha256 = sha256_file(source.local_path)
        remote = adapter.get_object_metadata(bucket_name, source.object_name)
        action = decide_upload(
            local_sha256,
            remote_exists=remote.exists,
            remote_metadata=remote.metadata,
        )
        if action is UploadAction.SKIP:
            skipped += 1
            continue
        adapter.upload_file(
            bucket_name,
            source.object_name,
            source.local_path,
            {SOURCE_SHA256_METADATA_KEY: local_sha256},
        )
        uploaded += 1

    return UploadSummary(uploaded=uploaded, skipped=skipped)
```

- [ ] **Step 5: Verify GREEN and commit**

```powershell
python -m pip install -r requirements-gcp.txt
python -m pytest tests/pipeline/test_gcs_workflows.py -v
python -m py_compile pipeline/gcs_adapter.py pipeline/gcs_workflows.py
git add pipeline/gcs_adapter.py pipeline/gcs_workflows.py tests/pipeline/test_gcs_workflows.py
git commit -m "feat: add GCS landing adapter and workflow"
```

---

### Task 4: CLI and Read-Only ADC Docker Boundary

**Files:**
- Create: `scripts/bootstrap_gcs.py`
- Create: `scripts/upload_landing_sources.py`
- Create: `scripts/verify_gcs_live.py`
- Modify: `docker-compose.yml`
- Modify: `.gitignore` only if a newly introduced credential filename pattern is not already ignored.

**Interfaces:**
- `python scripts/bootstrap_gcs.py --create-if-missing`
- `python scripts/bootstrap_gcs.py --check-only`
- `python scripts/upload_landing_sources.py --smoke`
- `python scripts/upload_landing_sources.py`
- `python scripts/verify_gcs_live.py`
- Compose profile/service: `--profile gcp`, `gcp-toolbox`.

- [ ] **Step 1: Implement bootstrap CLI**

```python
# scripts/bootstrap_gcs.py
import argparse

from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--create-if-missing", action="store_true")
    mode.add_argument("--check-only", action="store_true")
    args = parser.parse_args()

    settings = load_gcp_settings()
    location = GcsAdapter(settings.project_id).ensure_bucket(
        settings.bucket_name,
        settings.location,
        create_if_missing=args.create_if_missing,
    )
    print(f"GCS bucket verified: {settings.bucket_name} location={location}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Implement upload CLI**

```python
# scripts/upload_landing_sources.py
import argparse
from pathlib import Path

from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.gcs_workflows import upload_sources


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()

    settings = load_gcp_settings()
    summary = upload_sources(
        Path(__file__).resolve().parents[1],
        settings.bucket_name,
        GcsAdapter(settings.project_id),
        smoke=args.smoke,
    )
    print(f"landing upload complete: uploaded={summary.uploaded} skipped={summary.skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Implement exact live verification script**

Create `scripts/verify_gcs_live.py` so live proof is repeatable and does not rely on ad-hoc shell snippets:

```python
from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.gcs_landing import (
    SOURCE_SHA256_METADATA_KEY,
    LandingConflictError,
    decide_upload,
    iter_landing_sources,
    sha256_file,
)


def main() -> int:
    settings = load_gcp_settings()
    adapter = GcsAdapter(settings.project_id)
    repo_root = Path(__file__).resolve().parents[1]

    source = next(
        item
        for item in iter_landing_sources(repo_root)
        if item.object_name.startswith("transactions/")
    )
    remote = adapter.get_object_metadata(settings.bucket_name, source.object_name)
    if not remote.exists:
        raise RuntimeError(f"Smoke object missing: {source.object_name}")

    stored_sha = remote.metadata.get(SOURCE_SHA256_METADATA_KEY)
    local_sha = sha256_file(source.local_path)
    downloaded_sha = hashlib.sha256(
        adapter.download_bytes(settings.bucket_name, source.object_name)
    ).hexdigest()
    if stored_sha != local_sha or downloaded_sha != local_sha:
        raise RuntimeError("Live read/checksum verification failed")

    disposable_name = "_smoke_conflict/disposable.txt"
    with tempfile.TemporaryDirectory() as tmp_dir:
        disposable = Path(tmp_dir) / "disposable.txt"
        disposable.write_bytes(b"local-version")
        adapter.upload_file(
            settings.bucket_name,
            disposable_name,
            disposable,
            {SOURCE_SHA256_METADATA_KEY: "0" * 64},
        )
        try:
            conflict = adapter.get_object_metadata(settings.bucket_name, disposable_name)
            try:
                decide_upload(
                    sha256_file(disposable),
                    remote_exists=conflict.exists,
                    remote_metadata=conflict.metadata,
                )
            except LandingConflictError:
                pass
            else:
                raise RuntimeError("Expected checksum conflict was not raised")
        finally:
            adapter.delete_object(settings.bucket_name, disposable_name)

    print("live GCS verification passed: read/checksum/conflict")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

The disposable path is outside canonical landing prefixes and is deleted in `finally`.

- [ ] **Step 4: Add dedicated GCP Compose service**

Add:

```yaml
  gcp-toolbox:
    build:
      context: .
      dockerfile: docker/toolbox.Dockerfile
    profiles: ["gcp"]
    working_dir: /opt/bahtflow
    environment:
      BAHTFLOW_GCP_PROJECT: ${BAHTFLOW_GCP_PROJECT}
      BAHTFLOW_GCS_BUCKET: ${BAHTFLOW_GCS_BUCKET}
      BAHTFLOW_GCP_LOCATION: ${BAHTFLOW_GCP_LOCATION}
      BAHTFLOW_RUNTIME_SERVICE_ACCOUNT: ${BAHTFLOW_RUNTIME_SERVICE_ACCOUNT}
      GOOGLE_APPLICATION_CREDENTIALS: /var/secrets/google/application_default_credentials.json
    volumes:
      - ./:/opt/bahtflow
      - type: bind
        source: ${GOOGLE_ADC_HOST_PATH}
        target: /var/secrets/google/application_default_credentials.json
        read_only: true
    networks:
      - bahtflow
```

Do not mount ADC into PostgreSQL or existing Airflow services in F02.

- [ ] **Step 5: Verify container contract and full tests**

```powershell
docker compose config --quiet
docker compose --profile gcp config --services
docker compose --profile gcp build gcp-toolbox
docker compose --profile gcp run --rm gcp-toolbox python -c "from google.cloud import storage; print(storage.__name__)"
python -m pytest tests -v --basetemp .pytest_tmp
```

Expected: Compose config passes, `gcp-toolbox` is listed, Google Storage imports in container, pytest has zero failures.

- [ ] **Step 6: Commit**

```powershell
git add scripts/bootstrap_gcs.py scripts/upload_landing_sources.py scripts/verify_gcs_live.py docker-compose.yml .gitignore
git commit -m "feat: add local GCS CLI and ADC container boundary"
```

---

### Task 5: README, Live GCP Gate, and Final Review

**Files:**
- Modify: `README.md`

**Interfaces:**
- Windows PowerShell + Docker Desktop runbook.
- No command prints credential/token file contents.

- [ ] **Step 1: Document IAM separation**

README must state:

```text
Developer/admin identity:
- creates/verifies the bucket when needed
- can impersonate the runtime service account

Runtime service account:
- gets bucket-scoped roles/storage.objectAdmin for F02 object create/get/list/delete
- does not administer bucket IAM
- does not create/delete the bucket
```

The developer identity needs `roles/iam.serviceAccountTokenCreator` on the runtime service account before impersonation.

- [ ] **Step 2: Bootstrap bucket under developer/admin ADC**

```powershell
gcloud auth application-default login
docker compose --profile gcp run --rm gcp-toolbox `
  python scripts/bootstrap_gcs.py --create-if-missing
```

Expected: bucket is created in `BAHTFLOW_GCP_LOCATION` or existing matching bucket is verified. A location mismatch must fail.

- [ ] **Step 3: Switch ADC to runtime service-account impersonation**

Use the actual service-account email from `.env` in this command; do not print the ADC file:

```powershell
gcloud auth application-default login `
  --impersonate-service-account=bahtflow-runtime@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

- [ ] **Step 4: Verify Docker resolves ADC without revealing secrets**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "import google.auth; c,p=google.auth.default(); print('project=', p); print('credential_type=', type(c).__name__)"
```

Expected: credential resolution succeeds; no token value is printed.

- [ ] **Step 5: Run bounded landing smoke twice**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python scripts/upload_landing_sources.py --smoke
```

Run the exact command again.

Required evidence: the second run reports all four smoke selections skipped and uploads zero new objects. If the first run encounters already-existing identical smoke objects, its uploaded/skipped split may differ; that is acceptable.

- [ ] **Step 6: Run exact live read/checksum/conflict proof**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python scripts/verify_gcs_live.py
```

Expected:

```text
live GCS verification passed: read/checksum/conflict
```

This proves one canonical object can be read back byte-for-byte, stored SHA-256 matches, a disposable mismatched object triggers the same hard-fail decision, and the disposable object is cleaned up.

- [ ] **Step 7: Final credential-free regression and scope review**

```powershell
python -m pytest tests -v --basetemp .pytest_tmp
docker compose config --quiet
git diff main...HEAD --check
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
git status
git diff --stat main...HEAD
```

Expected:

- pytest zero failures;
- Compose config exit 0;
- `git diff --check` clean;
- no tracked credential/key file;
- no BigQuery/dbt/Pandas/Airflow business implementation;
- working tree clean after final docs commit.

- [ ] **Step 8: Commit README**

```powershell
git add README.md
git commit -m "docs: add GCS bootstrap and verification runbook"
```

---

## Feature 02 Completion Gate

Do not merge until fresh evidence covers every item:

```text
[ ] google-cloud-storage==3.13.1 is reproducible in gcp-toolbox
[ ] central GCP config validation passes
[ ] canonical transaction/FX/manifest mapping passes
[ ] exact-byte SHA-256 tests pass
[ ] absent -> upload passes
[ ] same checksum -> skip passes
[ ] different checksum -> hard fail passes
[ ] missing remote checksum -> hard fail passes
[ ] upload uses if_generation_match=0
[ ] docker compose config succeeds
[ ] ADC bind mount is read-only and limited to gcp-toolbox in F02
[ ] no credential file is tracked
[ ] admin/bootstrap bucket create-or-check succeeds live
[ ] impersonated runtime ADC resolves in Docker live
[ ] live smoke upload/read/checksum succeeds
[ ] rerun smoke is idempotent
[ ] disposable mismatch proof fails safely and cleans up
[ ] full pytest suite has zero failures
[ ] final diff contains no BigQuery/dbt/Pandas/Airflow business code
```

Only after all gates pass: Review Diff -> final verification -> merge to `main`.
