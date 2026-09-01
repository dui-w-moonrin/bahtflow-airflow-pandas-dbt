from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY, LandingConflictError
from pipeline.gcs_workflows import upload_sources


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.upload_calls = []

    def get_object_metadata(self, bucket_name, object_name):
        value = self.objects.get(object_name)
        if value is None:
            return SimpleNamespace(exists=False, metadata={})
        return SimpleNamespace(exists=True, metadata=value["metadata"])

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


def test_smoke_upload_selects_one_txn_one_fx_and_both_manifests(tmp_path):
    repo = make_repo(tmp_path)
    storage = FakeStorage()

    summary = upload_sources(repo, "bucket", storage, smoke=True)

    assert summary.uploaded == 4
    assert sorted(storage.upload_calls) == [
        "fx/2025/07/fx_20250708.csv",
        "manifests/daily_source_manifest.csv",
        "manifests/fx_manifest.csv",
        "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
    ]


def test_bootstrap_cli_requires_exactly_one_mode():
    from scripts.bootstrap_gcs import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    assert parser.parse_args(["--check-only"]).check_only is True
    assert parser.parse_args(["--create-if-missing"]).create_if_missing is True


def test_upload_cli_supports_smoke_flag():
    from scripts.upload_landing_sources import build_parser

    parser = build_parser()
    assert parser.parse_args([]).smoke is False
    assert parser.parse_args(["--smoke"]).smoke is True
