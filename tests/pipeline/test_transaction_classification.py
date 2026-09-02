from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd

from pipeline.transaction_classification import validate_and_canonicalize_transactions


def raw_row(**overrides):
    row = {
        "txn": " TX-1 ",
        "dtts": "2025-07-22 09:30:00",
        "amount": "100.50",
        "currency": " usd ",
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": 1,
        "source_row_id": "row-1",
        "batch_date": date(2025, 7, 22),
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def test_base_validation_canonicalizes_valid_values():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame([raw_row()]),
        date(2025, 7, 22),
    )

    row = result.iloc[0]
    assert row["_txn_canonical"] == "TX-1"
    assert row["_transaction_dt"] == pd.Timestamp("2025-07-22 09:30:00")
    assert row["_amount_numeric"] == Decimal("100.50")
    assert row["_currency_canonical"] == "USD"
    assert row["_base_reason_codes"] == []
