from __future__ import annotations

import gzip
import hashlib
from datetime import date, datetime, timezone

from pipeline.gcs_adapter import ObjectMetadata
from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY
from pipeline.raw_load import (
    load_fx_raw_batch,
    load_raw_batch,
    load_transaction_raw_batch,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tx_object(d: date, region: str) -> str:
    return (
        f"transactions/business_date={d.isoformat()}/"
        f"sales_{region}_{d:%Y%m%d}.csv.gz"
    )


class FakeGcsAdapter:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def list_object_names(self, bucket_name: str, prefix: str = "") -> list[str]:
        return sorted(name for name in self.objects if name.startswith(prefix))

    def get_object_metadata(self, bucket_name: str, object_name: str) -> ObjectMetadata:
        if object_name not in self.objects:
            return ObjectMetadata(exists=False, metadata={})
        data = self.objects[object_name]
        return ObjectMetadata(
            exists=True,
            metadata={SOURCE_SHA256_METADATA_KEY: _sha256(data)},
        )

    def download_bytes(self, bucket_name: str, object_name: str) -> bytes:
        return self.objects[object_name]


class FakeBigQueryAdapter:
    def __init__(self):
        self.ids = {
            ("transactions", "2025-07-22"): set(),
            ("fx_rates", "2025-07-22"): set(),
        }
        self.rows = {
            ("transactions", "2025-07-22"): [],
            ("fx_rates", "2025-07-22"): [],
        }

    def query_source_row_ids(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        return set(self.ids[(table_id, partition_date.isoformat())])

    def append_rows(self, dataset_id, table_id, rows, schema):
        if not rows:
            return 0
        key_date = (
            rows[0]["batch_date"]
            if table_id == "transactions"
            else rows[0]["rate_date"]
        )
        key = (table_id, key_date)
        self.rows[key].extend(rows)
        self.ids[key].update(row["source_row_id"] for row in rows)
        return len(rows)

    def query_partition_row_count(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        return len(self.rows[(table_id, partition_date.isoformat())])


def _objects_with_fx(d: date) -> dict[str, bytes]:
    objects = {}
    for region in ("bkk", "central", "north", "northeast", "south"):
        objects[_tx_object(d, region)] = gzip.compress(
            (
                "txn,dtts,amount,currency\n"
                f"{region}-1,not-a-time,N/A,usd\n"
            ).encode("utf-8")
        )
    fx_name = f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"
    objects[fx_name] = (
        "rate_date,currency,mid_rate,rate_unit,source_provider,source_url\n"
        f"{d.isoformat()},USD,32.4,THB_PER_FOREIGN,BOT,https://example.test/usd\n"
        f"{d.isoformat()},EUR,37.9,THB_PER_FOREIGN,BOT,https://example.test/eur\n"
    ).encode("utf-8")
    return objects


def test_first_run_loads_and_rerun_inserts_zero_rows():
    d = date(2025, 7, 22)
    gcs = FakeGcsAdapter(_objects_with_fx(d))
    bq = FakeBigQueryAdapter()
    first = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=gcs,
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    second = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=gcs,
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 3, 0, tzinfo=timezone.utc),
    )
    assert (first.tx_files, first.tx_source_rows, first.tx_inserted_rows) == (5, 5, 5)
    assert (first.fx_status, first.fx_source_rows, first.fx_inserted_rows) == (
        "LOADED", 2, 2
    )
    assert (second.tx_inserted_rows, second.tx_partition_rows) == (0, 5)
    assert (second.fx_inserted_rows, second.fx_partition_rows) == (0, 2)
    assert bq.rows[("transactions", "2025-07-22")][0]["amount"] == "N/A"
    assert bq.rows[("transactions", "2025-07-22")][0]["dtts"] == "not-a-time"


def test_missing_same_day_fx_returns_no_new_rate():
    d = date(2025, 7, 22)
    objects = _objects_with_fx(d)
    del objects[f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"]
    summary = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(objects),
        bigquery_adapter=FakeBigQueryAdapter(),
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )
    assert summary.tx_inserted_rows == 5
    assert summary.fx_status == "NO_NEW_RATE"
    assert summary.fx_source_rows == 0
    assert summary.fx_inserted_rows == 0
    assert summary.fx_partition_rows == 0


def test_transaction_raw_loader_does_not_require_same_day_fx():
    d = date(2025, 7, 22)
    objects = _objects_with_fx(d)
    del objects[f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"]
    bq = FakeBigQueryAdapter()

    summary = load_transaction_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(objects),
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )

    assert summary.batch_date == "2025-07-22"
    assert summary.tx_files == 5
    assert summary.tx_source_rows == 5
    assert summary.tx_inserted_rows == 5
    assert summary.tx_partition_rows == 5
    assert bq.rows[("fx_rates", "2025-07-22")] == []


def test_fx_raw_loader_returns_no_new_rate_without_touching_transactions():
    d = date(2025, 7, 22)
    objects = _objects_with_fx(d)
    del objects[f"fx/{d:%Y}/{d:%m}/fx_{d:%Y%m%d}.csv"]
    bq = FakeBigQueryAdapter()

    summary = load_fx_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(objects),
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )

    assert summary.batch_date == "2025-07-22"
    assert summary.fx_status == "NO_NEW_RATE"
    assert summary.fx_source_rows == 0
    assert summary.fx_inserted_rows == 0
    assert summary.fx_partition_rows == 0
    assert bq.rows[("transactions", "2025-07-22")] == []


def test_combined_raw_loader_remains_backward_compatible_after_split():
    d = date(2025, 7, 22)
    bq = FakeBigQueryAdapter()

    summary = load_raw_batch(
        batch_date=d,
        bucket_name="bucket",
        gcs_adapter=FakeGcsAdapter(_objects_with_fx(d)),
        bigquery_adapter=bq,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )

    assert summary.tx_files == 5
    assert summary.tx_source_rows == 5
    assert summary.tx_inserted_rows == 5
    assert summary.tx_partition_rows == 5
    assert summary.fx_status == "LOADED"
    assert summary.fx_source_rows == 2
    assert summary.fx_inserted_rows == 2
    assert summary.fx_partition_rows == 2
