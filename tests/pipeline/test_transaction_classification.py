from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

import pipeline.transaction_classification as classification
from pipeline.transaction_classification import (
    TransactionClassificationError,
    classify_transactions,
    validate_and_canonicalize_transactions,
)


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


def test_zero_amount_is_valid_and_negative_amount_is_flagged():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame(
            [
                raw_row(source_row_id="zero", amount="0"),
                raw_row(source_row_id="negative", amount="-0.01"),
            ]
        ),
        date(2025, 7, 22),
    ).set_index("source_row_id")

    assert result.loc["zero", "_base_reason_codes"] == []
    assert result.loc["negative", "_base_reason_codes"] == ["NEGATIVE_AMOUNT"]


def test_invalid_amount_covers_blank_text_non_finite_scale_and_width():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame(
            [
                raw_row(source_row_id="blank", amount=" "),
                raw_row(source_row_id="text", amount="N/A"),
                raw_row(source_row_id="inf", amount="Infinity"),
                raw_row(source_row_id="scale", amount="0.1234567891"),
                raw_row(
                    source_row_id="width",
                    amount="100000000000000000000000000000",
                ),
            ]
        ),
        date(2025, 7, 22),
    )

    assert result["_base_reason_codes"].tolist() == [
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
        ["INVALID_AMOUNT"],
    ]


def test_datetime_rules_distinguish_parse_failure_from_batch_mismatch():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame(
            [
                raw_row(source_row_id="bad-date", dtts="not-a-date"),
                raw_row(
                    source_row_id="wrong-day",
                    dtts="2025-07-23 00:00:00",
                ),
            ]
        ),
        date(2025, 7, 22),
    )

    assert result["_base_reason_codes"].tolist() == [
        ["INVALID_DTTS"],
        ["DTTS_BATCH_DATE_MISMATCH"],
    ]


def test_currency_and_region_rules_canonicalize_or_reject_explicitly():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame(
            [
                raw_row(source_row_id="eur", currency=" eur ", region="north"),
                raw_row(source_row_id="gbp", currency="GBP", region="north"),
                raw_row(
                    source_row_id="bad-region",
                    currency="THB",
                    region="unknown",
                ),
            ]
        ),
        date(2025, 7, 22),
    ).set_index("source_row_id")

    assert result.loc["eur", "_currency_canonical"] == "EUR"
    assert result.loc["eur", "_base_reason_codes"] == []
    assert result.loc["gbp", "_base_reason_codes"] == ["INVALID_CURRENCY"]
    assert result.loc["bad-region", "_base_reason_codes"] == ["INVALID_REGION"]


def test_multiple_base_failures_keep_fixed_reason_order():
    result = validate_and_canonicalize_transactions(
        pd.DataFrame(
            [
                raw_row(
                    txn=" ",
                    amount="N/A",
                    currency=" gbp ",
                    region="unknown",
                )
            ]
        ),
        date(2025, 7, 22),
    )

    assert result.iloc[0]["_base_reason_codes"] == [
        "MISSING_TXN",
        "INVALID_AMOUNT",
        "INVALID_CURRENCY",
        "INVALID_REGION",
    ]


def test_missing_required_raw_column_fails_explicitly():
    frame = pd.DataFrame([raw_row()]).drop(columns=["source_row_id"])

    with pytest.raises(TransactionClassificationError, match="source_row_id"):
        validate_and_canonicalize_transactions(frame, date(2025, 7, 22))


def test_exact_replay_accepts_lowest_source_lineage():
    frame = pd.DataFrame(
        [
            raw_row(
                txn="TX-R",
                source_row_id="north",
                source_file=(
                    "transactions/business_date=2025-07-22/"
                    "sales_north_20250722.csv.gz"
                ),
                source_row_number=1,
            ),
            raw_row(
                txn="TX-R",
                source_row_id="bkk-later",
                source_file=(
                    "transactions/business_date=2025-07-22/"
                    "sales_bkk_20250722.csv.gz"
                ),
                source_row_number=11,
            ),
            raw_row(
                txn=" TX-R ",
                source_row_id="winner",
                source_file=(
                    "transactions/business_date=2025-07-22/"
                    "sales_bkk_20250722.csv.gz"
                ),
                source_row_number=10,
            ),
        ]
    )

    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )

    assert result.accepted["source_row_id"].tolist() == ["winner"]
    quarantine = result.quarantine.sort_values("source_row_id").reset_index(drop=True)
    assert quarantine["source_row_id"].tolist() == ["bkk-later", "north"]
    assert quarantine["reason_codes"].tolist() == [
        ["DUPLICATE_REPLAY"],
        ["DUPLICATE_REPLAY"],
    ]


def test_conflicting_payloads_quarantine_all_base_valid_occurrences():
    frame = pd.DataFrame(
        [
            raw_row(
                txn="TX-C",
                amount="100.00",
                source_row_id="conflict-a",
                source_row_number=20,
            ),
            raw_row(
                txn=" TX-C ",
                amount="101.00",
                source_row_id="conflict-b",
                source_row_number=21,
            ),
        ]
    )

    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )

    assert result.accepted.empty
    quarantine = result.quarantine.sort_values("source_row_id").reset_index(drop=True)
    assert quarantine["source_row_id"].tolist() == ["conflict-a", "conflict-b"]
    assert quarantine["reason_codes"].tolist() == [
        ["DUPLICATE_CONFLICT"],
        ["DUPLICATE_CONFLICT"],
    ]


def test_base_invalid_row_does_not_poison_valid_duplicate_candidate():
    frame = pd.DataFrame(
        [
            raw_row(
                txn="TX-M",
                amount="N/A",
                source_row_id="malformed",
                source_row_number=30,
            ),
            raw_row(
                txn=" TX-M ",
                amount="100.00",
                source_row_id="valid",
                source_row_number=31,
            ),
        ]
    )

    result = classify_transactions(
        frame,
        date(2025, 7, 22),
        datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
    )

    assert result.accepted["source_row_id"].tolist() == ["valid"]
    assert result.quarantine["source_row_id"].tolist() == ["malformed"]
    assert result.quarantine["reason_codes"].tolist() == [["INVALID_AMOUNT"]]


def test_classify_duplicates_is_public_and_returns_duplicate_reasons():
    validated = validate_and_canonicalize_transactions(
        pd.DataFrame(
            [
                raw_row(txn="TX-P", source_row_id="first", source_row_number=1),
                raw_row(txn=" TX-P ", source_row_id="second", source_row_number=2),
            ]
        ),
        date(2025, 7, 22),
    )
    valid = validated[validated["_base_reason_codes"].map(len).eq(0)].copy()

    accepted, duplicate_quarantine = classification.classify_duplicates(valid)

    assert accepted["source_row_id"].tolist() == ["first"]
    assert duplicate_quarantine["source_row_id"].tolist() == ["second"]
    assert duplicate_quarantine["_duplicate_reason"].tolist() == [
        "DUPLICATE_REPLAY"
    ]
