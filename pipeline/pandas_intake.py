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


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise PandasIntakeError("ingested_at must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _read_source_csv(
    source_bytes: bytes,
    *,
    expected_columns: tuple[str, ...],
    compression: str | None,
    label: str,
) -> pd.DataFrame:
    frame = pd.read_csv(
        io.BytesIO(source_bytes),
        compression=compression,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )
    actual_columns = tuple(frame.columns)
    if actual_columns != expected_columns:
        raise PandasIntakeError(
            f"{label} header mismatch: "
            f"expected={expected_columns} actual={actual_columns}"
        )
    return frame


def prepare_transaction_frame(
    *,
    source_bytes: bytes,
    source_file: str,
    source_checksum: str,
    region: str,
    batch_date: date,
    ingested_at: datetime,
) -> pd.DataFrame:
    verify_source_checksum(source_bytes, source_checksum)
    frame = _read_source_csv(
        source_bytes,
        expected_columns=TRANSACTION_SOURCE_COLUMNS,
        compression="gzip",
        label="Transaction",
    )
    frame["region"] = region
    frame["source_file"] = source_file
    frame["source_checksum"] = source_checksum
    frame["source_row_number"] = range(1, len(frame) + 1)
    frame["source_row_id"] = [
        make_source_row_id(source_file, source_checksum, row_number)
        for row_number in frame["source_row_number"]
    ]
    frame["batch_date"] = batch_date.isoformat()
    frame["ingested_at"] = _utc_text(ingested_at)
    return frame
