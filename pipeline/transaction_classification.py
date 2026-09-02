from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd

RAW_TRANSACTION_COLUMNS = (
    "txn",
    "dtts",
    "amount",
    "currency",
    "region",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "batch_date",
    "ingested_at",
)
BASE_REASON_ORDER = (
    "MISSING_TXN",
    "INVALID_DTTS",
    "DTTS_BATCH_DATE_MISMATCH",
    "INVALID_AMOUNT",
    "NEGATIVE_AMOUNT",
    "INVALID_CURRENCY",
    "INVALID_REGION",
)
VALID_CURRENCIES = frozenset({"THB", "USD", "EUR"})
VALID_REGIONS = frozenset({"bkk", "central", "north", "northeast", "south"})


class TransactionClassificationError(RuntimeError):
    pass


def _is_missing(value) -> bool:
    if value is None:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):
        return False
    return str(value).strip() == ""


def _parse_bigquery_numeric(value) -> Decimal | None:
    if _is_missing(value):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None
    _sign, digits, exponent = parsed.as_tuple()
    if exponent >= 0:
        scale = 0
        integer_digits = len(digits) + exponent
    else:
        scale = -exponent
        integer_digits = max(len(digits) - scale, 0)
    if scale > 9 or integer_digits > 29:
        return None
    return parsed


def _require_raw_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in RAW_TRANSACTION_COLUMNS if column not in frame.columns]
    if missing:
        raise TransactionClassificationError(
            f"Missing raw transaction columns: {missing!r}"
        )


def validate_and_canonicalize_transactions(
    raw_df: pd.DataFrame,
    batch_date: date,
) -> pd.DataFrame:
    _require_raw_columns(raw_df)
    frame = raw_df.loc[:, list(RAW_TRANSACTION_COLUMNS)].copy()
    txn_text = frame["txn"].map(
        lambda value: "" if _is_missing(value) else str(value).strip()
    )
    dtts_text = frame["dtts"].map(
        lambda value: "" if _is_missing(value) else str(value).strip()
    )
    amount_text = frame["amount"].map(
        lambda value: "" if _is_missing(value) else str(value).strip()
    )
    currency_text = frame["currency"].map(
        lambda value: "" if _is_missing(value) else str(value).strip().upper()
    )

    frame["_txn_canonical"] = txn_text
    frame["_transaction_dt"] = pd.to_datetime(
        dtts_text,
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    frame["_amount_numeric"] = amount_text.map(_parse_bigquery_numeric)
    frame["_currency_canonical"] = currency_text

    masks = {
        "MISSING_TXN": txn_text.eq(""),
        "INVALID_DTTS": frame["_transaction_dt"].isna(),
        "DTTS_BATCH_DATE_MISMATCH": (
            frame["_transaction_dt"].notna()
            & frame["_transaction_dt"].dt.date.ne(batch_date)
        ),
        "INVALID_AMOUNT": frame["_amount_numeric"].isna(),
        "NEGATIVE_AMOUNT": frame["_amount_numeric"].map(
            lambda value: value is not None and value < Decimal("0")
        ),
        "INVALID_CURRENCY": ~currency_text.isin(VALID_CURRENCIES),
        "INVALID_REGION": ~frame["region"].isin(VALID_REGIONS),
    }
    frame["_base_reason_codes"] = [
        [reason for reason in BASE_REASON_ORDER if bool(masks[reason].loc[index])]
        for index in frame.index
    ]
    return frame
