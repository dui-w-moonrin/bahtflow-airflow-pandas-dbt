from datetime import date, datetime, timezone

import pandas as pd
import pytest

import pipeline.transaction_classification as classification
from pipeline.transaction_classification import (
    TransactionClassificationError,
    classify_transactions,
)


def raw_row(**overrides):
    row = {
        "txn": "TX-G",
        "dtts": "2025-07-22 09:30:00",
        "amount": "100.50",
        "currency": "USD",
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": 1,
        "source_row_id": "guard-row",
        "batch_date": date(2025, 7, 22),
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def test_classification_reconciliation_fails_when_internal_outcome_drops_row(monkeypatch):
    frame = pd.DataFrame([raw_row()])

    def drop_all_outcomes(valid_df):
        accepted = valid_df.iloc[0:0].copy()
        duplicate_quarantine = valid_df.iloc[0:0].copy()
        duplicate_quarantine["_duplicate_reason"] = pd.Series(dtype="object")
        return accepted, duplicate_quarantine

    monkeypatch.setattr(classification, "classify_duplicates", drop_all_outcomes)

    with pytest.raises(TransactionClassificationError, match="Classification reconciliation failed"):
        classify_transactions(
            frame,
            date(2025, 7, 22),
            datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
        )
