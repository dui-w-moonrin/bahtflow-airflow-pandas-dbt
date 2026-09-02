from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import pandas as pd

from pipeline.bigquery_contract import (
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)
from pipeline.gcs_landing import SOURCE_SHA256_METADATA_KEY
from pipeline.pandas_intake import (
    EXPECTED_REGIONS,
    PandasIntakeError,
    anti_filter_existing,
    prepare_fx_frame,
    prepare_transaction_frame,
    same_day_fx_object_name,
    transaction_prefix,
    validate_transaction_objects,
)


@dataclass(frozen=True)
class RawLoadSummary:
    batch_date: str
    tx_files: int
    tx_source_rows: int
    tx_inserted_rows: int
    tx_partition_rows: int
    fx_status: str
    fx_source_rows: int
    fx_inserted_rows: int
    fx_partition_rows: int


def _required_checksum(metadata) -> str:
    checksum = metadata.metadata.get(SOURCE_SHA256_METADATA_KEY)
    if not checksum:
        raise PandasIntakeError("Missing source checksum metadata")
    return checksum


def _load_tx_frame(*, batch_date, bucket_name, gcs_adapter, ingested_at):
    names = gcs_adapter.list_object_names(
        bucket_name,
        prefix=transaction_prefix(batch_date),
    )
    by_region = validate_transaction_objects(batch_date, names)

    frames = []
    for region in EXPECTED_REGIONS:
        object_name = by_region[region]
        metadata = gcs_adapter.get_object_metadata(bucket_name, object_name)
        if not metadata.exists:
            raise PandasIntakeError(
                f"Discovered transaction object disappeared: {object_name}"
            )
        source_bytes = gcs_adapter.download_bytes(bucket_name, object_name)
        frames.append(
            prepare_transaction_frame(
                source_bytes=source_bytes,
                source_file=object_name,
                source_checksum=_required_checksum(metadata),
                region=region,
                batch_date=batch_date,
                ingested_at=ingested_at,
            )
        )

    return pd.concat(frames, ignore_index=True)


def _load_fx_frame(*, batch_date, bucket_name, gcs_adapter, ingested_at):
    object_name = same_day_fx_object_name(batch_date)
    metadata = gcs_adapter.get_object_metadata(bucket_name, object_name)
    if not metadata.exists:
        return "NO_NEW_RATE", pd.DataFrame()

    source_bytes = gcs_adapter.download_bytes(bucket_name, object_name)
    return "LOADED", prepare_fx_frame(
        source_bytes=source_bytes,
        source_file=object_name,
        source_checksum=_required_checksum(metadata),
        rate_date=batch_date,
        ingested_at=ingested_at,
    )


def load_raw_batch(
    *,
    batch_date: date,
    bucket_name: str,
    gcs_adapter,
    bigquery_adapter,
    ingested_at: datetime | None = None,
) -> RawLoadSummary:
    invocation_time = ingested_at or datetime.now(timezone.utc)

    tx_frame = _load_tx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    tx_existing = bigquery_adapter.query_source_row_ids(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    tx_new = anti_filter_existing(tx_frame, tx_existing)
    tx_inserted = bigquery_adapter.append_rows(
        "bahtflow_raw",
        "transactions",
        tx_new.to_dict(orient="records"),
        TRANSACTIONS_SCHEMA,
    )
    tx_partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )

    fx_status, fx_frame = _load_fx_frame(
        batch_date=batch_date,
        bucket_name=bucket_name,
        gcs_adapter=gcs_adapter,
        ingested_at=invocation_time,
    )
    fx_inserted = 0
    if fx_status == "LOADED":
        fx_existing = bigquery_adapter.query_source_row_ids(
            "bahtflow_raw",
            "fx_rates",
            FX_RATES_PARTITION_FIELD,
            batch_date,
        )
        fx_new = anti_filter_existing(fx_frame, fx_existing)
        fx_inserted = bigquery_adapter.append_rows(
            "bahtflow_raw",
            "fx_rates",
            fx_new.to_dict(orient="records"),
            FX_RATES_SCHEMA,
        )

    fx_partition_rows = bigquery_adapter.query_partition_row_count(
        "bahtflow_raw",
        "fx_rates",
        FX_RATES_PARTITION_FIELD,
        batch_date,
    )

    return RawLoadSummary(
        batch_date=batch_date.isoformat(),
        tx_files=5,
        tx_source_rows=len(tx_frame),
        tx_inserted_rows=tx_inserted,
        tx_partition_rows=tx_partition_rows,
        fx_status=fx_status,
        fx_source_rows=len(fx_frame),
        fx_inserted_rows=fx_inserted,
        fx_partition_rows=fx_partition_rows,
    )
