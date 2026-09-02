from __future__ import annotations

import gzip
import hashlib
from datetime import date

import pytest

from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    make_source_row_id,
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
