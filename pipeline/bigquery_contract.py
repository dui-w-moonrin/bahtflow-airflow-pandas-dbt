from google.cloud import bigquery

DATASET_IDS = (
    "bahtflow_raw",
    "bahtflow_ops",
    "bahtflow_analytics",
    "bahtflow_public",
)

TRANSACTIONS_SCHEMA = (
    bigquery.SchemaField("txn", "STRING"),
    bigquery.SchemaField("dtts", "STRING"),
    bigquery.SchemaField("amount", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
)
TRANSACTIONS_PARTITION_FIELD = "batch_date"

FX_RATES_SCHEMA = (
    bigquery.SchemaField("rate_date_raw", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mid_rate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate_unit", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_provider", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
)
FX_RATES_PARTITION_FIELD = "rate_date"

ACCEPTED_DATASET_ID = "bahtflow_analytics"
ACCEPTED_TABLE_ID = "transactions_accepted"
ACCEPTED_TRANSACTIONS_SCHEMA = (
    bigquery.SchemaField("txn", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("transaction_dt", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("classified_at", "TIMESTAMP", mode="REQUIRED"),
)
ACCEPTED_TRANSACTIONS_PARTITION_FIELD = "batch_date"

QUARANTINE_DATASET_ID = "bahtflow_ops"
QUARANTINE_TABLE_ID = "transactions_quarantine"
QUARANTINE_TRANSACTIONS_SCHEMA = (
    bigquery.SchemaField("txn", "STRING"),
    bigquery.SchemaField("dtts", "STRING"),
    bigquery.SchemaField("amount", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("reason_codes", "STRING", mode="REPEATED"),
    bigquery.SchemaField("quarantined_at", "TIMESTAMP", mode="REQUIRED"),
)
QUARANTINE_TRANSACTIONS_PARTITION_FIELD = "batch_date"
