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
