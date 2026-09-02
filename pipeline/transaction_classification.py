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


def _parse_decimal(value) -> Decimal | None:
    if _is_missing(value):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None
    return parsed


def validate_and_canonicalize_transactions(
    raw_df: pd.DataFrame,
    batch_date: date,
) -> pd.DataFrame:
    del batch_date  # Base-rule validation is introduced by later RED tests.
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
    frame["_amount_numeric"] = amount_text.map(_parse_decimal)
    frame["_currency_canonical"] = currency_text
    frame["_base_reason_codes"] = [[] for _ in range(len(frame))]
    return frame
