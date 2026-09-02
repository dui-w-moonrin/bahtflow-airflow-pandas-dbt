import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY
from scripts.verify_gcs_live import (
    DISPOSABLE_OBJECT_NAME,
    VERIFY_OBJECT_NAME,
    verify_live,
)


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.delete_calls = []

    def get_object_metadata(self, bucket_name, object_name):
        value = self.objects.get(object_name)
        if value is None:
            return SimpleNamespace(exists=False, metadata={})
        return SimpleNamespace(exists=True, metadata=value["metadata"])

    def upload_file(self, bucket_name, object_name, local_path, metadata):
        self.objects[object_name] = {
            "bytes": Path(local_path).read_bytes(),
            "metadata": dict(metadata),
        }

    def download_bytes(self, bucket_name, object_name):
        return self.objects[object_name]["bytes"]

    def delete_object(self, bucket_name, object_name):
        del self.objects[object_name]
        self.delete_calls.append(object_name)


def seed_verify_object(storage):
    payload = b"manifest-header\n"
    storage.objects[VERIFY_OBJECT_NAME] = {
        "bytes": payload,
        "metadata": {
            SOURCE_SHA256_METADATA_KEY: hashlib.sha256(payload).hexdigest(),
        },
    }


def test_verify_live_reads_checksum_detects_conflict_and_cleans_up():
    storage = FakeStorage()
    seed_verify_object(storage)

    summary = verify_live("bucket", storage)

    assert summary.readback_checksum_match is True
    assert summary.conflict_detected is True
    assert summary.disposable_cleanup is True
    assert DISPOSABLE_OBJECT_NAME not in storage.objects
    assert storage.delete_calls == [DISPOSABLE_OBJECT_NAME]


def test_verify_live_refuses_preexisting_disposable_without_deleting_it():
    storage = FakeStorage()
    seed_verify_object(storage)
    storage.objects[DISPOSABLE_OBJECT_NAME] = {
        "bytes": b"do-not-touch",
        "metadata": {SOURCE_SHA256_METADATA_KEY: "0" * 64},
    }

    with pytest.raises(RuntimeError, match="already exists"):
        verify_live("bucket", storage)

    assert DISPOSABLE_OBJECT_NAME in storage.objects
    assert storage.delete_calls == []
