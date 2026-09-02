from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from pipeline.bigquery_numeric import BigQueryNumericError, parse_bigquery_numeric_text


RAW_FX_COLUMNS = (
    "rate_date_raw",
    "currency",
    "mid_rate",
    "rate_unit",
    "source_provider",
    "source_url",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "rate_date",
    "ingested_at",
)


class FxResolutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class EffectiveFxSnapshot:
    fx_rate_date: date
    usd_thb_rate: Decimal
    eur_thb_rate: Decimal
    is_carried_forward: bool
    staleness_days: int


def _as_date(value, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise FxResolutionError(f"Invalid {label}: {value!r}") from exc


def _require_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in RAW_FX_COLUMNS if column not in frame.columns]
    if missing:
        raise FxResolutionError(f"Missing raw FX columns: {missing!r}")


def _positive_rate(value, currency: str) -> Decimal:
    try:
        rate = parse_bigquery_numeric_text(value)
    except BigQueryNumericError as exc:
        raise FxResolutionError(
            f"Invalid FX rate for {currency}: {value!r}"
        ) from exc
    if rate <= Decimal("0"):
        raise FxResolutionError(f"Invalid FX rate for {currency}: {value!r}")
    return rate


def resolve_effective_fx(
    raw_fx_df: pd.DataFrame,
    batch_date: date,
) -> EffectiveFxSnapshot:
    _require_columns(raw_fx_df)
    frame = raw_fx_df.loc[:, list(RAW_FX_COLUMNS)].copy()
    frame["_rate_date"] = frame["rate_date"].map(
        lambda value: _as_date(value, "rate_date")
    )

    eligible = frame.loc[
        frame["_rate_date"].map(lambda value: value <= batch_date)
    ].copy()
    if eligible.empty:
        raise FxResolutionError(
            f"No FX publication at or before batch_date={batch_date.isoformat()}"
        )

    latest_date = max(eligible["_rate_date"])
    selected = eligible.loc[eligible["_rate_date"] == latest_date].copy()
    selected["_currency"] = selected["currency"].map(
        lambda value: str(value).strip().upper()
    )

    if len(selected) != 2 or set(selected["_currency"]) != {"USD", "EUR"}:
        raise FxResolutionError(
            f"Latest FX publication is not an exact USD/EUR pair: {latest_date}"
        )

    rates: dict[str, Decimal] = {}
    for record in selected.to_dict(orient="records"):
        raw_date = _as_date(record["rate_date_raw"], "rate_date_raw")
        if raw_date != latest_date:
            raise FxResolutionError(
                f"rate_date_raw mismatch: raw={raw_date} canonical={latest_date}"
            )
        currency = record["_currency"]
        rates[currency] = _positive_rate(record["mid_rate"], currency)

    staleness_days = (batch_date - latest_date).days
    return EffectiveFxSnapshot(
        fx_rate_date=latest_date,
        usd_thb_rate=rates["USD"],
        eur_thb_rate=rates["EUR"],
        is_carried_forward=latest_date < batch_date,
        staleness_days=staleness_days,
    )
