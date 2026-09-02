from __future__ import annotations

import gzip
import hashlib
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    anti_filter_existing,
    make_source_row_id,
    prepare_fx_frame,
    prepare_transaction_frame,
    same_day_fx_object_name,
    transaction_prefix,
    validate_transaction_objects,
    verify_source_checksum,
)


def _gzip_csv(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"))


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tx_names(batch_date: date) -> list[str]:
    return [
        (
            f"transactions/business_date={batch_date.isoformat()}/"
            f"sales_{region}_{batch_date:%Y%m%d}.csv.gz"
        )
        for region in EXPECTED_REGIONS
    ]


def test_transaction_prefix_and_region_order_are_fixed():
    d = date(2025, 7, 22)
    assert EXPECTED_REGIONS == (
        "bkk",
        "central",
        "north",
        "northeast",
        "south",
    )
    assert transaction_prefix(d) == "transactions/business_date=2025-07-22/"


def test_five_canonical_transaction_objects_are_accepted():
    d = date(2025, 7, 22)
    result = validate_transaction_objects(d, _tx_names(d))
    assert tuple(result) == EXPECTED_REGIONS
    assert result["north"].endswith("sales_north_20250722.csv.gz")


def test_missing_or_unexpected_transaction_object_is_rejected():
    d = date(2025, 7, 22)
    names = _tx_names(d)

    with pytest.raises(PandasIntakeError, match="Transaction object set mismatch"):
        validate_transaction_objects(d, names[:-1])

    with pytest.raises(PandasIntakeError, match="Transaction object set mismatch"):
        validate_transaction_objects(
            d,
            [
                *names,
                "transactions/business_date=2025-07-22/"
                "sales_unknown_20250722.csv.gz",
            ],
        )


def test_duplicate_transaction_object_is_rejected():
    d = date(2025, 7, 22)
    names = _tx_names(d)

    with pytest.raises(PandasIntakeError, match="Duplicate transaction object"):
        validate_transaction_objects(d, [*names, names[0]])


def test_same_day_fx_object_name_is_canonical():
    assert same_day_fx_object_name(date(2025, 7, 22)) == (
        "fx/2025/07/fx_20250722.csv"
    )


def test_checksum_missing_mismatch_and_match():
    source = b"abc"

    with pytest.raises(PandasIntakeError, match="Missing source checksum metadata"):
        verify_source_checksum(source, "")

    with pytest.raises(PandasIntakeError, match="Source checksum mismatch"):
        verify_source_checksum(source, "0" * 64)

    assert verify_source_checksum(source, _sha256(source)) == _sha256(source)


def test_source_row_id_is_stable_and_row_number_sensitive():
    checksum = "a" * 64
    object_name = _tx_names(date(2025, 7, 22))[0]

    first = make_source_row_id(object_name, checksum, 1)
    assert first == make_source_row_id(object_name, checksum, 1)
    assert first != make_source_row_id(object_name, checksum, 2)


def test_transaction_frame_enforces_exact_header():
    d = date(2025, 7, 22)
    source = _gzip_csv(
        "txn,dtts,wrong,currency\nT1,2025-07-22 01:00:00,10,THB\n"
    )

    with pytest.raises(PandasIntakeError, match="Transaction header mismatch"):
        prepare_transaction_frame(
            source_bytes=source,
            source_file=_tx_names(d)[0],
            source_checksum=_sha256(source),
            region="bkk",
            batch_date=d,
            ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
        )


def test_transaction_frame_preserves_dirty_strings_and_metadata():
    d = date(2025, 7, 22)
    source = _gzip_csv(
        "txn,dtts,amount,currency\n"
        "T1,not-a-time,N/A,usd\n"
        "T1,,N/A,usd\n"
    )
    checksum = _sha256(source)

    frame = prepare_transaction_frame(
        source_bytes=source,
        source_file=_tx_names(d)[0],
        source_checksum=checksum,
        region="bkk",
        batch_date=d,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )

    assert frame.loc[0, "amount"] == "N/A"
    assert frame.loc[0, "dtts"] == "not-a-time"
    assert frame.loc[1, "dtts"] == ""
    assert frame.loc[0, "currency"] == "usd"
    assert frame["source_row_number"].tolist() == [1, 2]
    assert frame.loc[0, "source_row_id"] != frame.loc[1, "source_row_id"]
    assert frame["batch_date"].tolist() == ["2025-07-22", "2025-07-22"]
    assert frame["ingested_at"].tolist() == [
        "2025-07-22T02:00:00Z",
        "2025-07-22T02:00:00Z",
    ]


def test_fx_frame_enforces_exact_header():
    d = date(2025, 7, 22)
    source = (
        "rate_date,currency,wrong,rate_unit,source_provider,source_url\n"
        "2025-07-22,USD,32.10,THB_PER_FCY,BOT,https://example.test\n"
    ).encode("utf-8")

    with pytest.raises(PandasIntakeError, match="FX header mismatch"):
        prepare_fx_frame(
            source_bytes=source,
            source_file=same_day_fx_object_name(d),
            source_checksum=_sha256(source),
            rate_date=d,
            ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
        )


def test_fx_frame_preserves_raw_rate_date_and_adds_metadata():
    d = date(2025, 7, 22)
    source = (
        "rate_date,currency,mid_rate,rate_unit,source_provider,source_url\n"
        "2025-07-22,EUR,37.40,THB_PER_FCY,BOT,https://example.test/eur\n"
        "2025-07-22,USD,32.10,THB_PER_FCY,BOT,https://example.test/usd\n"
    ).encode("utf-8")
    checksum = _sha256(source)

    frame = prepare_fx_frame(
        source_bytes=source,
        source_file=same_day_fx_object_name(d),
        source_checksum=checksum,
        rate_date=d,
        ingested_at=datetime(2025, 7, 22, 2, 0, tzinfo=timezone.utc),
    )

    assert frame["rate_date_raw"].tolist() == ["2025-07-22", "2025-07-22"]
    assert frame["currency"].tolist() == ["EUR", "USD"]
    assert frame["mid_rate"].tolist() == ["37.40", "32.10"]
    assert frame["source_row_number"].tolist() == [1, 2]
    assert frame["rate_date"].tolist() == ["2025-07-22", "2025-07-22"]
    assert frame["ingested_at"].tolist() == [
        "2025-07-22T02:00:00Z",
        "2025-07-22T02:00:00Z",
    ]


def test_anti_filter_existing_removes_only_already_loaded_source_rows():
    frame = pd.DataFrame(
        {
            "source_row_id": ["row-1", "row-2", "row-3"],
            "amount": ["10", "20", "30"],
        }
    )

    result = anti_filter_existing(frame, {"row-1", "row-3"})

    assert result["source_row_id"].tolist() == ["row-2"]
    assert result["amount"].tolist() == ["20"]
    assert result.index.tolist() == [0]


def test_anti_filter_existing_with_all_ids_returns_empty_same_columns():
    frame = pd.DataFrame(
        {
            "source_row_id": ["row-1", "row-2"],
            "amount": ["10", "20"],
        }
    )

    result = anti_filter_existing(frame, {"row-1", "row-2"})

    assert result.empty
    assert result.columns.tolist() == ["source_row_id", "amount"]
