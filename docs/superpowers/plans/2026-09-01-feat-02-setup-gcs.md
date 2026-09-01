# Feature 02 GCS Setup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a credential-safe, idempotent local-to-GCS landing boundary that uses ADC with service-account impersonation and rejects conflicting source objects by SHA-256.

**Architecture:** Keep source-path/checksum decisions as pure Python, place Google Cloud Storage calls behind one thin adapter, and keep CLI scripts as orchestration wrappers. Automated tests remain credential-free; live GCP verification is performed separately from Dui's machine with ADC mounted read-only into a dedicated Docker `gcp-toolbox` service.

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

Create or modify these files only as needed by this plan:

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
  tests/pipeline/test_config.py
  tests/pipeline/test_gcs_landing.py
  tests/pipeline/test_gcs_workflows.py
```

Responsibilities:

- `pipeline/config.py`: parse and validate F02 environment settings only.
- `pipeline/gcs_landing.py`: pure source discovery, object-name mapping, SHA-256, immutable upload decision.
- `pipeline/gcs_adapter.py`: thin Google SDK wrapper; no source-contract decisions.
- `pipeline/gcs_workflows.py`: compose pure landing rules with a storage adapter; test with a fake adapter.
- `scripts/bootstrap_gcs.py`: CLI for admin/bootstrap bucket create-or-verify.
- `scripts/upload_landing_sources.py`: CLI for runtime upload; supports bounded `--smoke` mode and full mode.
- `gcp-toolbox` Compose service: only container in F02 that receives the ADC bind mount.

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
- Consumes: process environment variables.
- Produces: `GcpSettings(project_id: str, bucket_name: str, location: str, runtime_service_account: str)` and `load_gcp_settings(env: Mapping[str, str] | None = None) -> GcpSettings`.

- [ ] **Step 1: Write failing configuration tests**

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
    settings = load_gcp_settings(VALID_ENV)

    assert settings == GcpSettings(
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
    env = VALID_ENV | {"BAHTFLOW_GCS_BUCKET": "   "}

    with pytest.raises(GcpConfigError, match="BAHTFLOW_GCS_BUCKET"):
        load_gcp_settings(env)
```

- [ ] **Step 2: Run the focused tests to prove RED**

Run:

```powershell
python -m pytest tests/pipeline/test_config.py -v
```

Expected: collection/import failure because `pipeline.config` does not exist yet.

- [ ] **Step 3: Add the pinned GCS dependency and toolbox installation**

Create `requirements-gcp.txt`:

```text
google-cloud-storage==3.13.1
```

Update `docker/toolbox.Dockerfile` to:

```dockerfile
FROM python:3.12-slim

WORKDIR /opt/bahtflow

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt

CMD ["sleep", "infinity"]
```

Create an empty `pipeline/__init__.py`.

- [ ] **Step 4: Implement minimal central config**

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

- [ ] **Step 5: Extend `.env.example` with safe F02 configuration names**

Append:

```text
BAHTFLOW_GCP_PROJECT=your-gcp-project-id
BAHTFLOW_GCS_BUCKET=your-globally-unique-bahtflow-bucket
BAHTFLOW_GCP_LOCATION=asia-southeast1
BAHTFLOW_RUNTIME_SERVICE_ACCOUNT=bahtflow-runtime@your-gcp-project-id.iam.gserviceaccount.com
GOOGLE_ADC_HOST_PATH=C:/Users/YOUR_USER/AppData/Roaming/gcloud/application_default_credentials.json
```

Do not put any actual user/project credential into the committed file.

- [ ] **Step 6: Run tests and static config checks**

Run:

```powershell
python -m pytest tests/pipeline/test_config.py -v
python -m py_compile pipeline/config.py
```

Expected: all configuration tests PASS and compile exits 0.

- [ ] **Step 7: Commit Task 1**

```powershell
git add requirements-gcp.txt pipeline/__init__.py pipeline/config.py tests/pipeline/test_config.py docker/toolbox.Dockerfile .env.example
git commit -m "feat: add GCP runtime configuration"
```

---

### Task 2: Pure Landing Path, Source Discovery, and SHA-256 Contract

**Files:**
- Create: `pipeline/gcs_landing.py`
- Create: `tests/pipeline/test_gcs_landing.py`

**Interfaces:**
- Consumes: repository root `Path`, local source `Path`, and remote metadata mappings.
- Produces:
  - `SOURCE_SHA256_METADATA_KEY = "bahtflow-source-sha256"`
  - `LandingConflictError`
  - `UploadAction` enum with `UPLOAD` and `SKIP`
  - `LandingSource(local_path: Path, object_name: str)`
  - `sha256_file(path: Path) -> str`
  - `transaction_object_name(relative_path: str) -> str`
  - `fx_object_name(relative_path: str) -> str`
  - `manifest_object_name(local_path: Path) -> str`
  - `iter_landing_sources(repo_root: Path) -> list[LandingSource]`
  - `decide_upload(local_sha256: str, *, remote_exists: bool, remote_metadata: Mapping[str, str] | None) -> UploadAction`

- [ ] **Step 1: Write failing pure-logic tests**

Create `tests/pipeline/test_gcs_landing.py`:

```python
import hashlib
from pathlib import Path

import pytest

from pipeline.gcs_landing import (
    SOURCE_SHA256_METADATA_KEY,
    LandingConflictError,
    UploadAction,
    decide_upload,
    fx_object_name,
    manifest_object_name,
    sha256_file,
    transaction_object_name,
)


def test_transaction_object_name_preserves_business_date_partition():
    assert transaction_object_name(
        "business_date=2025-07-22/sales_bkk_20250722.csv.gz"
    ) == "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz"


def test_fx_object_name_removes_local_fx_daily_prefix():
    assert fx_object_name(
        "fx_daily/2025/07/fx_20250708.csv"
    ) == "fx/2025/07/fx_20250708.csv"


@pytest.mark.parametrize(
    ("local_path", "expected"),
    [
        (Path("data/daily_source_manifest.csv"), "manifests/daily_source_manifest.csv"),
        (Path("fx/manifest.csv"), "manifests/fx_manifest.csv"),
    ],
)
def test_manifest_object_name_maps_known_manifests(local_path, expected):
    assert manifest_object_name(local_path) == expected


def test_sha256_file_hashes_exact_bytes(tmp_path):
    source = tmp_path / "sample.bin"
    source.write_bytes(b"raw-bytes\x00\xff")

    assert sha256_file(source) == hashlib.sha256(b"raw-bytes\x00\xff").hexdigest()


def test_absent_object_is_uploaded():
    assert decide_upload(
        "abc", remote_exists=False, remote_metadata=None
    ) is UploadAction.UPLOAD


def test_matching_remote_checksum_is_skipped():
    assert decide_upload(
        "abc",
        remote_exists=True,
        remote_metadata={SOURCE_SHA256_METADATA_KEY: "abc"},
    ) is UploadAction.SKIP


def test_conflicting_remote_checksum_fails():
    with pytest.raises(LandingConflictError, match="checksum mismatch"):
        decide_upload(
            "abc",
            remote_exists=True,
            remote_metadata={SOURCE_SHA256_METADATA_KEY: "different"},
        )


def test_existing_object_without_checksum_metadata_fails():
    with pytest.raises(LandingConflictError, match="missing"):
        decide_upload("abc", remote_exists=True, remote_metadata={})
```

- [ ] **Step 2: Run focused tests to prove RED**

```powershell
python -m pytest tests/pipeline/test_gcs_landing.py -v
```

Expected: FAIL because `pipeline.gcs_landing` is not implemented.

- [ ] **Step 3: Implement the pure landing primitives**

Create `pipeline/gcs_landing.py` with these exact public contracts:

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
    normalized = local_path.as_posix()
    mapping = {
        "data/daily_source_manifest.csv": "manifests/daily_source_manifest.csv",
        "fx/manifest.csv": "manifests/fx_manifest.csv",
    }
    try:
        return mapping[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported manifest path: {normalized}") from exc


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

- [ ] **Step 4: Add source-discovery tests against a tiny fixture tree**

Append tests that create:

```text
repo/
  data/daily_regional_sales/business_date=2025-07-22/sales_bkk_20250722.csv.gz
  data/daily_source_manifest.csv
  fx/fx_daily/2025/07/fx_20250708.csv
  fx/manifest.csv
```

and assert `iter_landing_sources(repo)` returns canonical object names in sorted order:

```python
[
    "fx/2025/07/fx_20250708.csv",
    "manifests/daily_source_manifest.csv",
    "manifests/fx_manifest.csv",
    "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
]
```

- [ ] **Step 5: Implement deterministic source discovery**

Add to `pipeline/gcs_landing.py`:

```python
def iter_landing_sources(repo_root: Path) -> list[LandingSource]:
    sources: list[LandingSource] = []

    transaction_root = repo_root / "data" / "daily_regional_sales"
    for path in transaction_root.glob("business_date=*/*.csv.gz"):
        relative = path.relative_to(transaction_root).as_posix()
        sources.append(LandingSource(path, transaction_object_name(relative)))

    fx_root = repo_root / "fx"
    for path in (fx_root / "fx_daily").glob("*/*/fx_*.csv"):
        relative = path.relative_to(fx_root).as_posix()
        sources.append(LandingSource(path, fx_object_name(relative)))

    for path in [repo_root / "data" / "daily_source_manifest.csv", repo_root / "fx" / "manifest.csv"]:
        if not path.is_file():
            raise FileNotFoundError(path)
        sources.append(LandingSource(path, manifest_object_name(path.relative_to(repo_root))))

    return sorted(sources, key=lambda item: item.object_name)
```

Also fail explicitly when either transaction root or FX root is missing, so an accidental partial checkout cannot silently produce a partial landing set.

- [ ] **Step 6: Run pure landing tests**

```powershell
python -m pytest tests/pipeline/test_gcs_landing.py -v
python -m py_compile pipeline/gcs_landing.py
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

```powershell
git add pipeline/gcs_landing.py tests/pipeline/test_gcs_landing.py
git commit -m "feat: define immutable GCS landing contract"
```

---

### Task 3: Thin GCS Adapter and Credential-Free Workflow Tests

**Files:**
- Create: `pipeline/gcs_adapter.py`
- Create: `pipeline/gcs_workflows.py`
- Create: `tests/pipeline/test_gcs_workflows.py`

**Interfaces:**
- `ObjectMetadata(exists: bool, metadata: Mapping[str, str])`
- `GcsAdapter(project_id: str)` methods:
  - `ensure_bucket(bucket_name: str, location: str, *, create_if_missing: bool) -> str`
  - `get_object_metadata(bucket_name: str, object_name: str) -> ObjectMetadata`
  - `upload_file(bucket_name: str, object_name: str, local_path: Path, metadata: Mapping[str, str]) -> None`
  - `list_object_names(bucket_name: str, prefix: str = "") -> list[str]`
  - `download_bytes(bucket_name: str, object_name: str) -> bytes`
- `UploadSummary(uploaded: int, skipped: int)`
- `upload_sources(repo_root: Path, bucket_name: str, adapter, *, smoke: bool = False) -> UploadSummary`

- [ ] **Step 1: Write workflow tests using a tiny fake adapter**

Create `tests/pipeline/test_gcs_workflows.py`:

```python
from pathlib import Path

import pytest

from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY, LandingConflictError
from pipeline.gcs_workflows import upload_sources


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.upload_calls = []

    def get_object_metadata(self, bucket_name, object_name):
        from pipeline.gcs_adapter import ObjectMetadata

        value = self.objects.get(object_name)
        if value is None:
            return ObjectMetadata(exists=False, metadata={})
        return ObjectMetadata(exists=True, metadata=value["metadata"])

    def upload_file(self, bucket_name, object_name, local_path, metadata):
        payload = Path(local_path).read_bytes()
        self.objects[object_name] = {"bytes": payload, "metadata": dict(metadata)}
        self.upload_calls.append(object_name)


def make_repo(tmp_path):
    txn = tmp_path / "data/daily_regional_sales/business_date=2025-07-22/sales_bkk_20250722.csv.gz"
    txn.parent.mkdir(parents=True)
    txn.write_bytes(b"txn-source")

    tx_manifest = tmp_path / "data/daily_source_manifest.csv"
    tx_manifest.write_text("header\n", encoding="utf-8")

    fx = tmp_path / "fx/fx_daily/2025/07/fx_20250708.csv"
    fx.parent.mkdir(parents=True)
    fx.write_bytes(b"fx-source")

    fx_manifest = tmp_path / "fx/manifest.csv"
    fx_manifest.write_text("header\n", encoding="utf-8")
    return tmp_path


def test_upload_sources_uploads_absent_objects_and_is_idempotent(tmp_path):
    repo = make_repo(tmp_path)
    storage = FakeStorage()

    first = upload_sources(repo, "bucket", storage)
    second = upload_sources(repo, "bucket", storage)

    assert first.uploaded == 4
    assert first.skipped == 0
    assert second.uploaded == 0
    assert second.skipped == 4
    assert len(storage.upload_calls) == 4


def test_upload_sources_stores_source_sha256_metadata(tmp_path):
    repo = make_repo(tmp_path)
    storage = FakeStorage()

    upload_sources(repo, "bucket", storage)

    for remote in storage.objects.values():
        assert len(remote["metadata"][SOURCE_SHA256_METADATA_KEY]) == 64


def test_upload_sources_rejects_conflicting_existing_object(tmp_path):
    repo = make_repo(tmp_path)
    storage = FakeStorage()
    object_name = "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz"
    storage.objects[object_name] = {
        "bytes": b"different",
        "metadata": {SOURCE_SHA256_METADATA_KEY: "0" * 64},
    }

    with pytest.raises(LandingConflictError):
        upload_sources(repo, "bucket", storage)

    assert object_name not in storage.upload_calls
```

- [ ] **Step 2: Run workflow tests to prove RED**

```powershell
python -m pytest tests/pipeline/test_gcs_workflows.py -v
```

Expected: FAIL because adapter/workflow modules do not exist.

- [ ] **Step 3: Implement the adapter types and Google SDK wrapper**

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
            bucket = self._client.bucket(bucket_name)
            bucket = self._client.create_bucket(bucket, location=location)

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
```

`if_generation_match=0` is deliberate defense in depth: even after the pre-check, GCS must refuse a race that creates the same object before upload.

- [ ] **Step 4: Implement the workflow over the narrow adapter contract**

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
    one_txn = next(item for item in sources if item.object_name.startswith("transactions/"))
    one_fx = next(item for item in sources if item.object_name.startswith("fx/"))
    manifests = [item for item in sources if item.object_name.startswith("manifests/")]
    return sorted([one_txn, one_fx, *manifests], key=lambda item: item.object_name)


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

- [ ] **Step 5: Add and verify bounded smoke-subset behavior**

Add a test asserting `upload_sources(..., smoke=True)` uploads exactly four objects: one transaction file, one FX file, and both manifests. This keeps live F02 verification bounded while the default remains the full corpus uploader.

- [ ] **Step 6: Run Task 3 tests and compile**

Host venv must have the GCP dependency available before importing `pipeline.gcs_adapter`:

```powershell
python -m pip install -r requirements-gcp.txt
python -m pytest tests/pipeline/test_gcs_workflows.py -v
python -m py_compile pipeline/gcs_adapter.py pipeline/gcs_workflows.py
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```powershell
git add pipeline/gcs_adapter.py pipeline/gcs_workflows.py tests/pipeline/test_gcs_workflows.py
git commit -m "feat: add GCS landing adapter and workflow"
```

---

### Task 4: CLI Entry Points and Read-Only ADC Docker Boundary

**Files:**
- Create: `scripts/bootstrap_gcs.py`
- Create: `scripts/upload_landing_sources.py`
- Modify: `docker-compose.yml`
- Modify: `.gitignore` only if verification identifies an uncovered credential pattern.

**Interfaces:**
- `python scripts/bootstrap_gcs.py --create-if-missing`
- `python scripts/bootstrap_gcs.py --check-only`
- `python scripts/upload_landing_sources.py --smoke`
- `python scripts/upload_landing_sources.py` for full corpus when intentionally requested.
- Compose service `gcp-toolbox` with profile `gcp`.

- [ ] **Step 1: Write CLI contract tests without invoking GCP**

Extend `tests/pipeline/test_gcs_workflows.py` or create small source-level tests that assert:

```text
scripts/bootstrap_gcs.py
  loads GcpSettings
  instantiates GcsAdapter(settings.project_id)
  passes create_if_missing from CLI flag

scripts/upload_landing_sources.py
  supports --smoke
  calls upload_sources(repo_root, bucket_name, adapter, smoke=...)
```

Keep these tests source/argument-parser focused; do not mock Google internals.

- [ ] **Step 2: Implement bootstrap CLI**

Create `scripts/bootstrap_gcs.py`:

```python
from __future__ import annotations

import argparse

from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--create-if-missing", action="store_true")
    mode.add_argument("--check-only", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_gcp_settings()
    adapter = GcsAdapter(settings.project_id)
    location = adapter.ensure_bucket(
        settings.bucket_name,
        settings.location,
        create_if_missing=args.create_if_missing,
    )
    print(f"GCS bucket verified: {settings.bucket_name} location={location}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Implement upload CLI**

Create `scripts/upload_landing_sources.py`:

```python
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.gcs_workflows import upload_sources


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Upload one transaction, one FX file, and both manifests only.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_gcp_settings()
    adapter = GcsAdapter(settings.project_id)
    repo_root = Path(__file__).resolve().parents[1]

    summary = upload_sources(
        repo_root,
        settings.bucket_name,
        adapter,
        smoke=args.smoke,
    )
    print(f"landing upload complete: uploaded={summary.uploaded} skipped={summary.skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add dedicated `gcp-toolbox` Compose service**

Add a service that reuses `docker/toolbox.Dockerfile` but receives GCP env and ADC only under profile `gcp`:

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

Do not mount ADC into PostgreSQL. Do not add GCP credentials to the existing Airflow services in F02 because they still run only EmptyOperators.

- [ ] **Step 5: Verify Compose config without starting GCP service**

With `.env` containing a syntactically valid host path:

```powershell
docker compose config --quiet
docker compose --profile gcp config --services
```

Expected services include `gcp-toolbox`; existing Airflow/Postgres services remain unchanged.

- [ ] **Step 6: Build the GCP toolbox image**

```powershell
docker compose --profile gcp build gcp-toolbox
docker compose --profile gcp run --rm gcp-toolbox python -c "from google.cloud import storage; print(storage.__name__)"
```

Expected: import succeeds.

- [ ] **Step 7: Run the full credential-free test suite**

```powershell
python -m pytest tests -v --basetemp .pytest_tmp
docker compose config --quiet
```

Expected: all tests PASS and Compose config exits 0.

- [ ] **Step 8: Commit Task 4**

```powershell
git add scripts/bootstrap_gcs.py scripts/upload_landing_sources.py docker-compose.yml .gitignore
git commit -m "feat: add local GCS CLI and ADC container boundary"
```

---

### Task 5: Operator Documentation and Live GCP Verification

**Files:**
- Modify: `README.md`
- Optionally modify the master roadmap/spec wording only to reflect the already-approved GCS-only F02 split; do not change architecture beyond the approved F02 spec.

**Interfaces:**
- Operator workflow from Windows PowerShell + Docker Desktop.
- No secret/token values are printed or pasted into logs.

- [ ] **Step 1: Document prerequisite IAM boundary**

Add a concise README section that states:

```text
Developer/admin identity:
- may create the bucket during bootstrap
- must be allowed to impersonate the runtime service account

Runtime service account:
- normal landing access is bucket-scoped object access
- does not administer bucket IAM
- does not create/delete the bucket
```

Document that the runtime service account should receive bucket-scoped `roles/storage.objectAdmin` for F02 object create/get/list behavior, while bucket creation/location bootstrap is performed under the developer/admin identity. Do not grant project-wide Storage Admin to the runtime identity merely to make the smoke test easy.

- [ ] **Step 2: Document developer ADC bootstrap phase**

From the host, authenticate normal developer ADC when bucket creation is needed:

```powershell
gcloud auth application-default login
```

Set real values in ignored `.env`, then verify/create with the admin-capable ADC:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python scripts/bootstrap_gcs.py --create-if-missing
```

If the bucket already exists, `--check-only` is sufficient.

- [ ] **Step 3: Document impersonated runtime ADC phase**

After bootstrap/IAM is ready, replace local ADC with impersonated runtime ADC:

```powershell
gcloud auth application-default login `
  --impersonate-service-account=$env:BAHTFLOW_RUNTIME_SERVICE_ACCOUNT
```

The developer identity must have `roles/iam.serviceAccountTokenCreator` on that service account. Do not ask the user to display or paste the ADC JSON.

- [ ] **Step 4: Verify Docker resolves the impersonated ADC**

Run only non-secret identity/client checks:

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -c "import google.auth; c,p=google.auth.default(); print('project=', p); print('credential_type=', type(c).__name__)"
```

Expected: credential resolution succeeds. The command must not print token values.

- [ ] **Step 5: Run live bounded upload smoke test**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python scripts/upload_landing_sources.py --smoke
```

Expected first run:

```text
landing upload complete: uploaded=4 skipped=0
```

Run the exact command a second time.

Expected second run:

```text
landing upload complete: uploaded=0 skipped=4
```

If the bucket already contains some identical smoke objects from an earlier run, the exact uploaded/skipped split may differ on the first invocation; the required evidence is that the subsequent invocation reports all selected smoke objects skipped and no duplicate/overwrite occurs.

- [ ] **Step 6: Verify list/read/checksum live through the adapter**

Use a short Python command or add a documented `python -c` check that:

1. selects one uploaded smoke object;
2. reads its metadata;
3. confirms `bahtflow-source-sha256` is present and 64 hex characters;
4. downloads the object bytes;
5. compares SHA-256 of downloaded bytes with the stored metadata.

Do not print credential contents.

- [ ] **Step 7: Prove checksum conflict behavior using a disposable object**

Do not alter a real landing object. Use a temporary local fixture and a disposable object such as:

```text
_smoke_conflict/disposable.txt
```

Create the remote disposable object with intentionally different `bahtflow-source-sha256` metadata, invoke the same pure `decide_upload`/workflow conflict path against it, and capture the hard-failure output. Remove the disposable object after the proof.

If exercising this through the production uploader would require adding unsupported source-path behavior, use a small one-off verification command against `GcsAdapter.get_object_metadata()` plus `decide_upload()` rather than broadening the canonical landing contract.

- [ ] **Step 8: Run final local regression suite**

```powershell
python -m pytest tests -v --basetemp .pytest_tmp
docker compose config --quiet
git status
git diff main...HEAD -- . ':!docs/superpowers/specs/2026-09-01-feat-02-setup-gcs-design.md' ':!docs/superpowers/plans/2026-09-01-feat-02-setup-gcs.md'
```

Expected:

- pytest: zero failures;
- Compose config: exit 0;
- working tree clean after docs commit;
- diff contains only F02 GCS/config/test/docs changes;
- no BigQuery/dbt/Pandas/Airflow business implementation.

- [ ] **Step 9: Perform repository secret hygiene check**

At minimum inspect tracked filenames and diff:

```powershell
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
git diff main...HEAD --check
```

Expected: no credential file is tracked and diff check returns no whitespace errors. Do not grep and print the contents of `.env` or ADC files.

- [ ] **Step 10: Commit documentation**

```powershell
git add README.md
git commit -m "docs: add GCS bootstrap and smoke-test runbook"
```

---

## Feature 02 Completion Gate

Do not merge F02 until fresh evidence covers every item:

```text
[ ] google-cloud-storage dependency is reproducible in gcp-toolbox
[ ] central GCP config validation tests pass
[ ] canonical transaction/FX/manifest path tests pass
[ ] byte-level SHA-256 tests pass
[ ] absent -> upload behavior passes
[ ] same checksum -> skip behavior passes
[ ] different checksum -> hard fail behavior passes
[ ] missing remote checksum -> hard fail behavior passes
[ ] upload uses if_generation_match=0 race protection
[ ] docker compose config succeeds
[ ] ADC is read-only mounted only into gcp-toolbox in F02
[ ] no credential file is tracked
[ ] admin/bootstrap bucket create-or-check succeeds live
[ ] impersonated runtime ADC resolves in Docker live
[ ] live smoke upload/list/read/checksum succeeds
[ ] rerun of smoke selection is idempotent
[ ] disposable checksum mismatch fails without overwrite
[ ] full pytest suite has zero failures
[ ] git diff contains no BigQuery/dbt/Pandas/Airflow business code
```

Only after all gates pass should the branch move to Review Diff -> final verification -> merge to `main`.
