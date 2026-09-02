from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.bigquery_contract import (
    DATASET_IDS,
    FX_RATES_PARTITION_FIELD,
    FX_RATES_SCHEMA,
    TRANSACTIONS_PARTITION_FIELD,
    TRANSACTIONS_SCHEMA,
)
from pipeline.config import load_gcp_settings


def main() -> None:
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)

    for dataset_id in DATASET_IDS:
        status = adapter.ensure_dataset(dataset_id, settings.location)
        print(f"dataset={dataset_id} status={status}")

    status = adapter.ensure_partitioned_table(
        "bahtflow_raw",
        "transactions",
        TRANSACTIONS_SCHEMA,
        TRANSACTIONS_PARTITION_FIELD,
    )
    print(f"table=bahtflow_raw.transactions status={status}")

    status = adapter.ensure_partitioned_table(
        "bahtflow_raw",
        "fx_rates",
        FX_RATES_SCHEMA,
        FX_RATES_PARTITION_FIELD,
    )
    print(f"table=bahtflow_raw.fx_rates status={status}")


if __name__ == "__main__":
    main()
