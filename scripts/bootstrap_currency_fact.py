from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.bigquery_contract import (
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
)
from pipeline.config import load_gcp_settings


def bootstrap_currency_fact(adapter) -> list[tuple[str, str]]:
    status = adapter.ensure_partitioned_table(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_SCHEMA,
        FACT_TRANSACTIONS_PARTITION_FIELD,
    )
    return [(f"{FACT_DATASET_ID}.{FACT_TABLE_ID}", status)]


def main() -> None:
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)
    for table_name, status in bootstrap_currency_fact(adapter):
        print(f"table={table_name} status={status}")


if __name__ == "__main__":
    main()
