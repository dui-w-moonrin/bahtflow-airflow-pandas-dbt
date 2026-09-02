from pipeline.bigquery_contract import (
    DATASET_IDS,
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)


def _shape(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def test_dataset_ids_are_fixed_and_minimal():
    assert DATASET_IDS == (
        "bahtflow_raw",
        "bahtflow_ops",
        "bahtflow_analytics",
        "bahtflow_public",
    )


def test_transactions_raw_schema_and_partition_contract():
    assert _shape(TRANSACTIONS_SCHEMA) == [
        ("txn", "STRING", "NULLABLE"),
        ("dtts", "STRING", "NULLABLE"),
        ("amount", "STRING", "NULLABLE"),
        ("currency", "STRING", "NULLABLE"),
        ("region", "STRING", "REQUIRED"),
        ("source_file", "STRING", "REQUIRED"),
        ("source_checksum", "STRING", "REQUIRED"),
        ("source_row_number", "INTEGER", "REQUIRED"),
        ("source_row_id", "STRING", "REQUIRED"),
        ("batch_date", "DATE", "REQUIRED"),
        ("ingested_at", "TIMESTAMP", "REQUIRED"),
    ]
    assert TRANSACTIONS_PARTITION_FIELD == "batch_date"


def test_fx_raw_schema_and_partition_contract():
    assert _shape(FX_RATES_SCHEMA) == [
        ("rate_date_raw", "STRING", "REQUIRED"),
        ("currency", "STRING", "REQUIRED"),
        ("mid_rate", "STRING", "REQUIRED"),
        ("rate_unit", "STRING", "REQUIRED"),
        ("source_provider", "STRING", "REQUIRED"),
        ("source_url", "STRING", "REQUIRED"),
        ("source_file", "STRING", "REQUIRED"),
        ("source_checksum", "STRING", "REQUIRED"),
        ("source_row_number", "INTEGER", "REQUIRED"),
        ("source_row_id", "STRING", "REQUIRED"),
        ("rate_date", "DATE", "REQUIRED"),
        ("ingested_at", "TIMESTAMP", "REQUIRED"),
    ]
    assert FX_RATES_PARTITION_FIELD == "rate_date"
