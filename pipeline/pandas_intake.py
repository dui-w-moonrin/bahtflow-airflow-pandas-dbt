from __future__ import annotations

import hashlib
import io
from datetime import date, datetime, timezone

import pandas as pd

EXPECTED_REGIONS = ("bkk", "central", "north", "northeast", "south")
TRANSACTION_SOURCE_COLUMNS = ("txn", "dtts", "amount", "currency")
FX_SOURCE_COLUMNS = (
    "rate_date",
    "currency",
    "mid_rate",
    "rate_unit",
    "source_provider",
    "source_url",
)


class PandasIntakeError(RuntimeError):
    pass


def transaction_prefix(batch_date: date) -> str:
    return f"transactions/business_date={batch_date.isoformat()}/"


def _expected_transaction_name(batch_date: date, region: str) -> str:
    return (
        f"{transaction_prefix(batch_date)}"
        f"sales_{region}_{batch_date:%Y%m%d}.csv.gz"
    )


def validate_transaction_objects(
    batch_date: date,
    object_names: list[str],
) -> dict[str, str]:
    if len(object_names) != len(set(object_names)):
        raise PandasIntakeError("Duplicate transaction object discovered")

    expected = {
        region: _expected_transaction_name(batch_date, region)
        for region in EXPECTED_REGIONS
    }
    expected_names = set(expected.values())
    actual_names = set(object_names)
    if actual_names != expected_names:
        raise PandasIntakeError(
            "Transaction object set mismatch: "
            f"missing={sorted(expected_names - actual_names)} "
            f"unexpected={sorted(actual_names - expected_names)}"
        )
    return expected


def same_day_fx_object_name(batch_date: date) -> str:
    return f"fx/{batch_date:%Y}/{batch_date:%m}/fx_{batch_date:%Y%m%d}.csv"


def verify_source_checksum(source_bytes: bytes, expected_checksum: str) -> str:
    if not expected_checksum:
        raise PandasIntakeError("Missing source checksum metadata")
    actual = hashlib.sha256(source_bytes).hexdigest()
    if actual != expected_checksum:
        raise PandasIntakeError(
            "Source checksum mismatch: "
            f"expected={expected_checksum} actual={actual}"
        )
    return actual


def make_source_row_id(
    source_file: str,
    source_checksum: str,
    source_row_number: int,
) -> str:
    identity = f"{source_file}|{source_checksum}|{source_row_number}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()
