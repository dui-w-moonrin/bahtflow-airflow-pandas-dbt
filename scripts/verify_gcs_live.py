from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pipeline.gcs_landing import (
    SOURCE_SHA256_METADATA_KEY,
    LandingConflictError,
    decide_upload,
    sha256_file,
)

VERIFY_OBJECT_NAME = "manifests/daily_source_manifest.csv"
DISPOSABLE_OBJECT_NAME = "_smoke_conflict/disposable.txt"
_DISPOSABLE_PAYLOAD = b"bahtflow-live-verifier\n"
_CHANGED_PAYLOAD = b"bahtflow-live-verifier-changed\n"


@dataclass(frozen=True)
class LiveVerificationSummary:
    readback_checksum_match: bool
    conflict_detected: bool
    disposable_cleanup: bool


def verify_live(bucket_name: str, adapter) -> LiveVerificationSummary:
    canonical = adapter.get_object_metadata(bucket_name, VERIFY_OBJECT_NAME)
    if not canonical.exists:
        raise RuntimeError(f"Verification object does not exist: {VERIFY_OBJECT_NAME}")

    stored_sha256 = canonical.metadata.get(SOURCE_SHA256_METADATA_KEY)
    if not stored_sha256:
        raise RuntimeError(
            f"Verification object is missing {SOURCE_SHA256_METADATA_KEY}: {VERIFY_OBJECT_NAME}"
        )

    downloaded = adapter.download_bytes(bucket_name, VERIFY_OBJECT_NAME)
    readback_checksum_match = (
        len(stored_sha256) == 64
        and hashlib.sha256(downloaded).hexdigest() == stored_sha256
    )
    if not readback_checksum_match:
        raise RuntimeError(f"Verification object checksum mismatch: {VERIFY_OBJECT_NAME}")

    existing_disposable = adapter.get_object_metadata(bucket_name, DISPOSABLE_OBJECT_NAME)
    if existing_disposable.exists:
        raise RuntimeError(
            "Disposable conflict object already exists; refusing to delete or overwrite it: "
            f"{DISPOSABLE_OBJECT_NAME}"
        )

    created_disposable = False
    conflict_detected = False
    disposable_cleanup = False

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "disposable.txt"
            local_path.write_bytes(_DISPOSABLE_PAYLOAD)
            original_sha256 = sha256_file(local_path)
            adapter.upload_file(
                bucket_name,
                DISPOSABLE_OBJECT_NAME,
                local_path,
                {SOURCE_SHA256_METADATA_KEY: original_sha256},
            )
            created_disposable = True

        remote_disposable = adapter.get_object_metadata(bucket_name, DISPOSABLE_OBJECT_NAME)
        if not remote_disposable.exists:
            raise RuntimeError("Disposable conflict object was not created")

        changed_sha256 = hashlib.sha256(_CHANGED_PAYLOAD).hexdigest()
        try:
            decide_upload(
                changed_sha256,
                remote_exists=remote_disposable.exists,
                remote_metadata=remote_disposable.metadata,
            )
        except LandingConflictError:
            conflict_detected = True

        if not conflict_detected:
            raise RuntimeError("Changed source checksum did not trigger LandingConflictError")
    finally:
        if created_disposable:
            adapter.delete_object(bucket_name, DISPOSABLE_OBJECT_NAME)
            disposable_cleanup = not adapter.get_object_metadata(
                bucket_name, DISPOSABLE_OBJECT_NAME
            ).exists

    if not disposable_cleanup:
        raise RuntimeError("Disposable conflict object cleanup could not be verified")

    return LiveVerificationSummary(
        readback_checksum_match=readback_checksum_match,
        conflict_detected=conflict_detected,
        disposable_cleanup=disposable_cleanup,
    )


def main() -> int:
    from pipeline.config import load_gcp_settings
    from pipeline.gcs_adapter import GcsAdapter

    settings = load_gcp_settings()
    adapter = GcsAdapter(settings.project_id)
    summary = verify_live(settings.bucket_name, adapter)
    print(f"object={VERIFY_OBJECT_NAME}")
    print(f"readback_checksum_match={summary.readback_checksum_match}")
    print(f"conflict_detected={summary.conflict_detected}")
    print(f"disposable_cleanup={summary.disposable_cleanup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
