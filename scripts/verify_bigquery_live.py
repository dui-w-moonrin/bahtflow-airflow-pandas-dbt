from pipeline.bigquery_adapter import BigQueryAdapter, BigQueryContractError
from pipeline.bigquery_contract import (
    DATASET_IDS,
    FX_RATES_PARTITION_FIELD,
    TRANSACTIONS_PARTITION_FIELD,
)
from pipeline.config import load_gcp_settings


def _partition_field(table) -> str | None:
    return getattr(table.time_partitioning, "field", None)


def main() -> None:
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)

    datasets = [adapter.get_dataset(dataset_id) for dataset_id in DATASET_IDS]
    for dataset in datasets:
        if (dataset.location or "").upper() != settings.location.upper():
            raise BigQueryContractError(
                f"Dataset location mismatch during verification: {dataset.dataset_id}"
            )

    transactions = adapter.get_table("bahtflow_raw", "transactions")
    fx_rates = adapter.get_table("bahtflow_raw", "fx_rates")

    tx_partition = _partition_field(transactions)
    fx_partition = _partition_field(fx_rates)
    if tx_partition != TRANSACTIONS_PARTITION_FIELD:
        raise BigQueryContractError("transactions partition verification failed")
    if fx_partition != FX_RATES_PARTITION_FIELD:
        raise BigQueryContractError("fx_rates partition verification failed")

    project = settings.project_id
    tx_rows = adapter.query_scalar(
        f"SELECT COUNT(*) FROM `{project}.bahtflow_raw.transactions`"
    )
    fx_rows = adapter.query_scalar(
        f"SELECT COUNT(*) FROM `{project}.bahtflow_raw.fx_rates`"
    )
    if tx_rows != 0 or fx_rows != 0:
        raise BigQueryContractError(
            f"F03 raw tables must be empty: transactions={tx_rows} fx_rates={fx_rows}"
        )

    print(f"datasets={len(datasets)}")
    print("raw_tables=2")
    print(f"transactions_partition={tx_partition}")
    print(f"fx_rates_partition={fx_partition}")
    print(f"transactions_rows={tx_rows}")
    print(f"fx_rates_rows={fx_rows}")


if __name__ == "__main__":
    main()
