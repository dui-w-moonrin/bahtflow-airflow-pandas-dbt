from datetime import date, datetime, timezone
from decimal import Decimal
import importlib

import pandas as pd
import pytest


BATCH_DATE = date(2025, 7, 22)
WHEN = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)


def _currency_fact_module():
    return importlib.import_module("pipeline.currency_fact")


def _fx_module():
    return importlib.import_module("pipeline.fx_resolution")


def accepted_row(txn, amount, currency, source_row_id):
    return {
        "txn": txn,
        "transaction_dt": datetime(2025, 7, 22, 9, 30),
        "amount": Decimal(amount) if isinstance(amount, str) else amount,
        "currency": currency,
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": 1,
        "source_row_id": source_row_id,
        "batch_date": BATCH_DATE,
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
        "classified_at": datetime(2026, 9, 2, 1, 0, tzinfo=timezone.utc),
    }


def make_fx(*, usd="32", eur="40", fx_rate_date=date(2025, 7, 21)):
    fx = _fx_module()
    return fx.EffectiveFxSnapshot(
        fx_rate_date=fx_rate_date,
        usd_thb_rate=Decimal(usd),
        eur_thb_rate=Decimal(eur),
        is_carried_forward=fx_rate_date < BATCH_DATE,
        staleness_days=(BATCH_DATE - fx_rate_date).days,
    )


def test_build_currency_fact_converts_thb_usd_eur_and_preserves_identity():
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame(
        [
            accepted_row("T-THB", "320", "THB", "row-thb"),
            accepted_row("T-USD", "10", "USD", "row-usd"),
            accepted_row("T-EUR", "8", "EUR", "row-eur"),
        ]
    )
    fact = fact_module.build_currency_fact(
        accepted,
        BATCH_DATE,
        make_fx(),
        WHEN,
    )
    by_txn = fact.set_index("txn")
    for txn in ("T-THB", "T-USD", "T-EUR"):
        assert by_txn.loc[txn, "amount_thb"] == Decimal("320")
        assert by_txn.loc[txn, "amount_usd"] == Decimal("10")
        assert by_txn.loc[txn, "amount_eur"] == Decimal("8")
    assert fact["source_row_id"].tolist() == ["row-thb", "row-usd", "row-eur"]
    assert fact["amount"].tolist() == [Decimal("320"), Decimal("10"), Decimal("8")]
    assert fact["currency"].tolist() == ["THB", "USD", "EUR"]
    assert fact["fx_rate_date"].tolist() == [date(2025, 7, 21)] * 3
    assert fact["is_carried_forward"].tolist() == [True] * 3
    assert fact["staleness_days"].tolist() == [1] * 3
    assert fact["fact_created_at"].tolist() == [WHEN] * 3
    assert len(fact) == len(accepted)


def test_derived_division_is_normalized_to_scale_nine():
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame([accepted_row("T", "1", "THB", "row-1")])
    fact = fact_module.build_currency_fact(
        accepted,
        BATCH_DATE,
        make_fx(usd="3", eur="7", fx_rate_date=BATCH_DATE),
        WHEN,
    )
    assert fact.iloc[0]["amount_usd"] == Decimal("0.333333333")
    assert fact.iloc[0]["amount_eur"] == Decimal("0.142857143")


@pytest.mark.parametrize("currency", ["JPY", "", None])
def test_impossible_accepted_currency_fails(currency):
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame([accepted_row("T", "1", currency, "row-1")])
    with pytest.raises(fact_module.CurrencyFactError, match="accepted currency"):
        fact_module.build_currency_fact(accepted, BATCH_DATE, make_fx(), WHEN)


def test_non_decimal_accepted_amount_fails():
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame([accepted_row("T", 1.5, "THB", "row-1")])
    with pytest.raises(fact_module.CurrencyFactError, match="accepted amount"):
        fact_module.build_currency_fact(accepted, BATCH_DATE, make_fx(), WHEN)


def test_negative_accepted_amount_fails():
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame([accepted_row("T", "-1", "THB", "row-1")])
    with pytest.raises(fact_module.CurrencyFactError, match="accepted amount"):
        fact_module.build_currency_fact(accepted, BATCH_DATE, make_fx(), WHEN)


def test_accepted_batch_date_mismatch_fails():
    fact_module = _currency_fact_module()
    row = accepted_row("T", "1", "THB", "row-1")
    row["batch_date"] = date(2025, 7, 21)
    with pytest.raises(fact_module.CurrencyFactError, match="batch_date"):
        fact_module.build_currency_fact(
            pd.DataFrame([row]), BATCH_DATE, make_fx(), WHEN
        )


def test_duplicate_source_row_id_in_accepted_fails():
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame(
        [
            accepted_row("T1", "1", "THB", "dup"),
            accepted_row("T2", "2", "THB", "dup"),
        ]
    )
    with pytest.raises(fact_module.CurrencyFactError, match="source_row_id"):
        fact_module.build_currency_fact(accepted, BATCH_DATE, make_fx(), WHEN)


def test_derived_numeric_overflow_fails():
    fact_module = _currency_fact_module()
    accepted = pd.DataFrame(
        [
            accepted_row(
                "T",
                "99999999999999999999999999999",
                "USD",
                "row-1",
            )
        ]
    )
    with pytest.raises(fact_module.CurrencyFactError, match="Currency conversion failed"):
        fact_module.build_currency_fact(
            accepted,
            BATCH_DATE,
            make_fx(
                usd="99999999999999999999999999999",
                eur="40",
                fx_rate_date=BATCH_DATE,
            ),
            WHEN,
        )
