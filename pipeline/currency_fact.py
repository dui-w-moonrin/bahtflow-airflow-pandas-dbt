from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, DecimalException, localcontext

import pandas as pd

from pipeline.bigquery_numeric import BigQueryNumericError, normalize_bigquery_numeric
from pipeline.fx_resolution import EffectiveFxSnapshot


class CurrencyFactError(RuntimeError):
    pass


ACCEPTED_INPUT_COLUMNS = (
    "txn",
    "transaction_dt",
    "amount",
    "currency",
    "region",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "batch_date",
    "ingested_at",
    "classified_at",
)

FACT_COLUMNS = ACCEPTED_INPUT_COLUMNS + (
    "amount_thb",
    "amount_usd",
    "amount_eur",
    "fx_rate_date",
    "usd_thb_rate",
    "eur_thb_rate",
    "is_carried_forward",
    "staleness_days",
    "fact_created_at",
)

VALID_CURRENCIES = frozenset({"THB", "USD", "EUR"})


def _as_batch_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise CurrencyFactError(f"Invalid accepted batch_date: {value!r}") from exc


def _validate_accepted(accepted_df: pd.DataFrame, batch_date: date) -> None:
    missing = [
        column for column in ACCEPTED_INPUT_COLUMNS if column not in accepted_df.columns
    ]
    if missing:
        raise CurrencyFactError(f"Missing accepted columns: {missing!r}")

    normalized_dates = accepted_df["batch_date"].map(_as_batch_date)
    if not normalized_dates.map(lambda value: value == batch_date).all():
        raise CurrencyFactError("Accepted batch_date does not match target batch_date")

    source_ids = accepted_df["source_row_id"].map(
        lambda value: "" if value is None else str(value).strip()
    )
    if source_ids.eq("").any() or source_ids.duplicated().any():
        raise CurrencyFactError("Accepted source_row_id must be nonblank and unique")

    for _, row in accepted_df.reset_index(drop=True).iterrows():
        amount = row["amount"]
        if not isinstance(amount, Decimal) or not amount.is_finite() or amount < 0:
            raise CurrencyFactError(
                f"Invalid accepted amount for source_row_id={row['source_row_id']}: "
                f"{amount!r}"
            )
        try:
            normalized_amount = normalize_bigquery_numeric(amount)
        except BigQueryNumericError as exc:
            raise CurrencyFactError(
                f"Invalid accepted amount for source_row_id={row['source_row_id']}"
            ) from exc
        if normalized_amount != amount:
            raise CurrencyFactError(
                f"Invalid accepted amount for source_row_id={row['source_row_id']}: "
                "requires unexpected NUMERIC normalization"
            )

        currency = row["currency"]
        if currency not in VALID_CURRENCIES:
            raise CurrencyFactError(
                f"Invalid accepted currency for source_row_id={row['source_row_id']}: "
                f"{currency!r}"
            )


def _convert_amount(
    amount: Decimal,
    currency: str,
    usd_rate: Decimal,
    eur_rate: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    with localcontext() as context:
        context.prec = 80
        if currency == "THB":
            amount_thb = amount
        elif currency == "USD":
            amount_thb = amount * usd_rate
        else:
            amount_thb = amount * eur_rate

        amount_usd = amount_thb / usd_rate
        amount_eur = amount_thb / eur_rate

    return (
        normalize_bigquery_numeric(amount_thb),
        normalize_bigquery_numeric(amount_usd),
        normalize_bigquery_numeric(amount_eur),
    )


def build_currency_fact(
    accepted_df: pd.DataFrame,
    batch_date: date,
    fx: EffectiveFxSnapshot,
    fact_created_at: datetime,
) -> pd.DataFrame:
    _validate_accepted(accepted_df, batch_date)

    try:
        usd_rate = normalize_bigquery_numeric(fx.usd_thb_rate)
        eur_rate = normalize_bigquery_numeric(fx.eur_thb_rate)
    except BigQueryNumericError as exc:
        raise CurrencyFactError("FX snapshot contains nonrepresentable rate") from exc

    if usd_rate <= 0 or eur_rate <= 0:
        raise CurrencyFactError("FX snapshot rates must be positive")

    records: list[dict] = []
    for row in accepted_df.loc[:, list(ACCEPTED_INPUT_COLUMNS)].to_dict(
        orient="records"
    ):
        try:
            amount_thb, amount_usd, amount_eur = _convert_amount(
                row["amount"],
                row["currency"],
                usd_rate,
                eur_rate,
            )
        except (BigQueryNumericError, DecimalException, ZeroDivisionError) as exc:
            raise CurrencyFactError(
                f"Currency conversion failed for source_row_id={row['source_row_id']}"
            ) from exc

        records.append(
            {
                **row,
                "amount_thb": amount_thb,
                "amount_usd": amount_usd,
                "amount_eur": amount_eur,
                "fx_rate_date": fx.fx_rate_date,
                "usd_thb_rate": usd_rate,
                "eur_thb_rate": eur_rate,
                "is_carried_forward": fx.is_carried_forward,
                "staleness_days": fx.staleness_days,
                "fact_created_at": fact_created_at,
            }
        )

    fact = pd.DataFrame(records, columns=list(FACT_COLUMNS))

    if len(accepted_df) != len(fact):
        raise CurrencyFactError(
            "Generated fact row count does not match accepted row count"
        )

    accepted_ids = set(accepted_df["source_row_id"])
    fact_ids = set(fact["source_row_id"])
    if accepted_ids != fact_ids:
        raise CurrencyFactError(
            "Generated fact source_row_id set does not match accepted input"
        )

    return fact
