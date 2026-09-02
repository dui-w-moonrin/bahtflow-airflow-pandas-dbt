from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    ACCEPTED_TRANSACTIONS_SCHEMA,
    QUARANTINE_DATASET_ID,
    QUARANTINE_TABLE_ID,
    QUARANTINE_TRANSACTIONS_PARTITION_FIELD,
    QUARANTINE_TRANSACTIONS_SCHEMA,
)
from pipeline.config import load_gcp_settings


def bootstrap_classification(adapter) -> list[tuple[str, str]]:
    targets = (
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
    )

    statuses = []
    for dataset_id, table_id, schema, partition_field in targets:
        status = adapter.ensure_partitioned_table(
            dataset_id,
            table_id,
            schema,
            partition_field,
        )
        statuses.append((f"{dataset_id}.{table_id}", status))
    return statuses


def main() -> None:
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)

    for table_name, status in bootstrap_classification(adapter):
        print(f"table={table_name} status={status}")


if __name__ == "__main__":
    main()
