from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd

from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
)
from pipeline.currency_fact import ACCEPTED_INPUT_COLUMNS, build_currency_fact
from pipeline.fx_resolution import RAW_FX_COLUMNS, resolve_effective_fx
from pipeline.pandas_intake import anti_filter_existing


class CurrencyFactLoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class CurrencyFactLoadSummary:
    batch_date: str
    accepted_rows: int
    fx_rate_date: str
    is_carried_forward: bool
    staleness_days: int
    fact_rows: int
    fact_inserted_rows: int
    accepted_partition_rows: int
    fact_partition_rows: int
    reconciled: bool


def _json_safe_value(value):
    if isinstance(value, list):
        return list(value)
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _frame_to_records(frame: pd.DataFrame) -> list[dict]:
    return [
        {key: _json_safe_value(value) for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]


def build_and_load_currency_fact(
    *,
    batch_date: date,
    bigquery_adapter,
    fact_created_at: datetime | None = None,
) -> CurrencyFactLoadSummary:
    invocation_time = fact_created_at or datetime.now(timezone.utc)

    accepted_rows = bigquery_adapter.query_partition_rows(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
        ACCEPTED_INPUT_COLUMNS,
    )
    accepted_frame = pd.DataFrame(
        accepted_rows,
        columns=list(ACCEPTED_INPUT_COLUMNS),
    )

    fx_rows = bigquery_adapter.query_rows_through_date(
        "bahtflow_raw",
        "fx_rates",
        "rate_date",
        batch_date,
        RAW_FX_COLUMNS,
    )
    fx_frame = pd.DataFrame(fx_rows, columns=list(RAW_FX_COLUMNS))
    snapshot = resolve_effective_fx(fx_frame, batch_date)

    fact = build_currency_fact(
        accepted_frame,
        batch_date,
        snapshot,
        invocation_time,
    )

    existing_ids = bigquery_adapter.query_source_row_ids(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    fact_new = anti_filter_existing(fact, existing_ids)
    inserted = bigquery_adapter.append_rows(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        _frame_to_records(fact_new),
        FACT_TRANSACTIONS_SCHEMA,
    )

    accepted_partition_rows = bigquery_adapter.query_partition_row_count(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    fact_partition_rows = bigquery_adapter.query_partition_row_count(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )

    reconciled = accepted_partition_rows == fact_partition_rows
    if not reconciled:
        raise CurrencyFactLoadError(
            "Persisted currency fact reconciliation failed: "
            f"accepted={accepted_partition_rows} fact={fact_partition_rows}"
        )

    return CurrencyFactLoadSummary(
        batch_date=batch_date.isoformat(),
        accepted_rows=len(accepted_frame),
        fx_rate_date=snapshot.fx_rate_date.isoformat(),
        is_carried_forward=snapshot.is_carried_forward,
        staleness_days=snapshot.staleness_days,
        fact_rows=len(fact),
        fact_inserted_rows=inserted,
        accepted_partition_rows=accepted_partition_rows,
        fact_partition_rows=fact_partition_rows,
        reconciled=True,
    )
