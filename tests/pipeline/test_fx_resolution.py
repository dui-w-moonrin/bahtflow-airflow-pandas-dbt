from datetime import date
from decimal import Decimal
import importlib

import pandas as pd
import pytest


def _fx_module():
    return importlib.import_module("pipeline.fx_resolution")


def fx_row(rate_date, currency, mid_rate, *, rate_date_raw=None, row_number=None):
    number = row_number or (1 if currency == "USD" else 2)
    return {
        "rate_date_raw": rate_date_raw or rate_date.isoformat(),
        "currency": currency,
        "mid_rate": mid_rate,
        "rate_unit": "THB",
        "source_provider": "BOT",
        "source_url": "https://example.test/fx",
        "source_file": f"fx/{rate_date:%Y}/{rate_date:%m}/fx_{rate_date:%Y%m%d}.csv",
        "source_checksum": "abc",
        "source_row_number": number,
        "source_row_id": f"{rate_date}-{currency}-{number}",
        "rate_date": rate_date,
        "ingested_at": "2026-09-02T00:00:00Z",
    }


def pair(rate_date, usd="32.50", eur="37.25"):
    return [
        fx_row(rate_date, "USD", usd, row_number=1),
        fx_row(rate_date, "EUR", eur, row_number=2),
    ]


def test_same_day_pair_resolves_without_carry_forward():
    fx = _fx_module()
    batch_date = date(2025, 7, 22)
    snapshot = fx.resolve_effective_fx(pd.DataFrame(pair(batch_date)), batch_date)
    assert snapshot.fx_rate_date == batch_date
    assert snapshot.usd_thb_rate == Decimal("32.50")
    assert snapshot.eur_thb_rate == Decimal("37.25")
    assert snapshot.is_carried_forward is False
    assert snapshot.staleness_days == 0


def test_latest_prior_pair_is_carried_forward_and_future_is_ignored():
    fx = _fx_module()
    batch_date = date(2025, 7, 22)
    rows = pair(date(2025, 7, 21), usd="32.00", eur="37.00")
    rows += pair(date(2025, 7, 23), usd="99.00", eur="99.00")
    snapshot = fx.resolve_effective_fx(pd.DataFrame(rows), batch_date)
    assert snapshot.fx_rate_date == date(2025, 7, 21)
    assert snapshot.usd_thb_rate == Decimal("32.00")
    assert snapshot.eur_thb_rate == Decimal("37.00")
    assert snapshot.is_carried_forward is True
    assert snapshot.staleness_days == 1


def test_no_prior_publication_fails():
    fx = _fx_module()
    with pytest.raises(fx.FxResolutionError, match="No FX publication"):
        fx.resolve_effective_fx(
            pd.DataFrame(pair(date(2025, 7, 23))),
            date(2025, 7, 22),
        )


def test_latest_publication_missing_eur_fails_without_falling_back():
    fx = _fx_module()
    rows = pair(date(2025, 7, 20))
    rows.append(fx_row(date(2025, 7, 21), "USD", "32.00", row_number=1))
    with pytest.raises(fx.FxResolutionError, match="exact USD/EUR pair"):
        fx.resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_latest_publication_missing_usd_fails_without_falling_back():
    fx = _fx_module()
    rows = pair(date(2025, 7, 20))
    rows.append(fx_row(date(2025, 7, 21), "EUR", "37.00", row_number=2))
    with pytest.raises(fx.FxResolutionError, match="exact USD/EUR pair"):
        fx.resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_duplicate_currency_fails():
    fx = _fx_module()
    d = date(2025, 7, 21)
    rows = pair(d) + [fx_row(d, "USD", "32.60", row_number=3)]
    with pytest.raises(fx.FxResolutionError, match="exact USD/EUR pair"):
        fx.resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_unsupported_extra_currency_fails():
    fx = _fx_module()
    d = date(2025, 7, 21)
    rows = pair(d) + [fx_row(d, "JPY", "0.22", row_number=3)]
    with pytest.raises(fx.FxResolutionError, match="exact USD/EUR pair"):
        fx.resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


@pytest.mark.parametrize("rate", ["", "N/A", "NaN", "Infinity", "0", "-1"])
def test_invalid_or_nonpositive_rate_fails(rate):
    fx = _fx_module()
    d = date(2025, 7, 21)
    rows = [
        fx_row(d, "USD", rate, row_number=1),
        fx_row(d, "EUR", "37.25", row_number=2),
    ]
    with pytest.raises(fx.FxResolutionError, match="Invalid FX rate"):
        fx.resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_rate_date_raw_mismatch_fails():
    fx = _fx_module()
    d = date(2025, 7, 21)
    rows = [
        fx_row(
            d,
            "USD",
            "32.50",
            rate_date_raw="2025-07-20",
            row_number=1,
        ),
        fx_row(d, "EUR", "37.25", row_number=2),
    ]
    with pytest.raises(fx.FxResolutionError, match="rate_date_raw mismatch"):
        fx.resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))
