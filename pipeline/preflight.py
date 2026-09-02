from __future__ import annotations

from dataclasses import dataclass

from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)


@dataclass(frozen=True)
class PreflightSummary:
    project_id: str
    bucket_name: str
    location: str
    datasets_verified: int
    tables_verified: int


_REQUIRED_DATASETS = (
    "bahtflow_raw",
    "bahtflow_ops",
    "bahtflow_analytics",
)

_REQUIRED_TABLES = (
    (
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_SCHEMA,
        TRANSACTIONS_PARTITION_FIELD,
    ),
    (
        "bahtflow_raw",
        "fx_rates",
        FX_RATES_SCHEMA,
        FX_RATES_PARTITION_FIELD,
    ),
    (
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_SCHEMA,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ),
    (
        QUARANTINE_DATASET_ID,
        QUARANTINE_TABLE_ID,
        QUARANTINE_TRANSACTIONS_SCHEMA,
        QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    ),
    (
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_SCHEMA,
        FACT_TRANSACTIONS_PARTITION_FIELD,
    ),
)


def run_preflight(*, settings, gcs_adapter, bigquery_adapter) -> PreflightSummary:
    gcs_adapter.ensure_bucket(
        settings.bucket_name,
        settings.location,
        create_if_missing=False,
    )

    for dataset_id in _REQUIRED_DATASETS:
        bigquery_adapter.validate_dataset(dataset_id, settings.location)

    for dataset_id, table_id, schema, partition_field in _REQUIRED_TABLES:
        bigquery_adapter.validate_partitioned_table(
            dataset_id,
            table_id,
            schema,
            partition_field,
        )

    return PreflightSummary(
        project_id=settings.project_id,
        bucket_name=settings.bucket_name,
        location=settings.location,
        datasets_verified=len(_REQUIRED_DATASETS),
        tables_verified=len(_REQUIRED_TABLES),
    )
