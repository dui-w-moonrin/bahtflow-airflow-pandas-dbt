from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd

from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
)
from pipeline.pandas_intake import anti_filter_existing
from pipeline.transaction_classification import (
    RAW_TRANSACTION_COLUMNS,
    classify_transactions,
)


class ClassificationLoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClassificationLoadSummary:
    batch_date: str
    raw_rows: int
    accepted_rows: int
    quarantine_rows: int
    accepted_inserted_rows: int
    quarantine_inserted_rows: int
    accepted_partition_rows: int
    quarantine_partition_rows: int
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


def classify_and_load_batch(
    *,
    batch_date: date,
    bigquery_adapter,
    classified_at: datetime | None = None,
) -> ClassificationLoadSummary:
    invocation_time = classified_at or datetime.now(timezone.utc)
    raw_rows = bigquery_adapter.query_partition_rows(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_PARTITION_FIELD,
        batch_date,
        RAW_TRANSACTION_COLUMNS,
    )
    raw_frame = pd.DataFrame(raw_rows, columns=list(RAW_TRANSACTION_COLUMNS))
    result = classify_transactions(raw_frame, batch_date, invocation_time)

    accepted_existing = bigquery_adapter.query_source_row_ids(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    accepted_new = anti_filter_existing(result.accepted, accepted_existing)
    accepted_inserted = bigquery_adapter.append_rows(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        _frame_to_records(accepted_new),
        ACCEPTED_TRANSACTIONS_SCHEMA,
    )

    quarantine_existing = bigquery_adapter.query_source_row_ids(
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    quarantine_new = anti_filter_existing(result.quarantine, quarantine_existing)
    quarantine_inserted = bigquery_adapter.append_rows(
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        _frame_to_records(quarantine_new),
        QUARANTINE_TRANSACTIONS_SCHEMA,
    )

    accepted_partition_rows = bigquery_adapter.query_partition_row_count(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    quarantine_partition_rows = bigquery_adapter.query_partition_row_count(
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    reconciled = (
        len(raw_frame) == accepted_partition_rows + quarantine_partition_rows
    )
    if not reconciled:
        raise ClassificationLoadError(
            "Persisted classification reconciliation failed: "
            f"raw={len(raw_frame)} accepted={accepted_partition_rows} "
            f"quarantine={quarantine_partition_rows}"
        )

    return ClassificationLoadSummary(
        batch_date=batch_date.isoformat(),
        raw_rows=len(raw_frame),
        accepted_rows=len(result.accepted),
        quarantine_rows=len(result.quarantine),
        accepted_inserted_rows=accepted_inserted,
        quarantine_inserted_rows=quarantine_inserted,
        accepted_partition_rows=accepted_partition_rows,
        quarantine_partition_rows=quarantine_partition_rows,
        reconciled=True,
    )
