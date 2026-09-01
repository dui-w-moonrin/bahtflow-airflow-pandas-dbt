import hashlib
from pathlib import Path

import pytest

from pipeline.gcs_landing import (
    SOURCE_SHA256_METADATA_KEY,
    LandingConflictError,
    UploadAction,
    decide_upload,
    fx_object_name,
    iter_landing_sources,
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


def _make_fixture_repo(root):
    txn = root / "data/daily_regional_sales/business_date=2025-07-22/sales_bkk_20250722.csv.gz"
    txn.parent.mkdir(parents=True)
    txn.write_bytes(b"txn")

    tx_manifest = root / "data/daily_source_manifest.csv"
    tx_manifest.write_text("header\n", encoding="utf-8")

    fx = root / "fx/fx_daily/2025/07/fx_20250708.csv"
    fx.parent.mkdir(parents=True)
    fx.write_bytes(b"fx")

    fx_manifest = root / "fx/manifest.csv"
    fx_manifest.write_text("header\n", encoding="utf-8")


def test_iter_landing_sources_returns_canonical_sorted_paths(tmp_path):
    _make_fixture_repo(tmp_path)

    assert [item.object_name for item in iter_landing_sources(tmp_path)] == [
        "fx/2025/07/fx_20250708.csv",
        "manifests/daily_source_manifest.csv",
        "manifests/fx_manifest.csv",
        "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
    ]


def test_iter_landing_sources_rejects_missing_transaction_root(tmp_path):
    (tmp_path / "fx/fx_daily").mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="daily_regional_sales"):
        iter_landing_sources(tmp_path)


def test_iter_landing_sources_rejects_missing_fx_root(tmp_path):
    (tmp_path / "data/daily_regional_sales").mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="fx_daily"):
        iter_landing_sources(tmp_path)
