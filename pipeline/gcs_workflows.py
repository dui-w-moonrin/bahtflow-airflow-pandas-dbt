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
