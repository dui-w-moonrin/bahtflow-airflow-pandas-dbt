import importlib

from pipeline.config import GcpSettings


class FakeGcs:
    def __init__(self):
        self.calls = []

    def ensure_bucket(self, bucket_name, location, *, create_if_missing):
        self.calls.append((bucket_name, location, create_if_missing))
        return location.lower()


class FakeBigQuery:
    def __init__(self):
        self.datasets = []
        self.tables = []

    def validate_dataset(self, dataset_id, location):
        self.datasets.append((dataset_id, location))
        return "verified"

    def validate_partitioned_table(self, dataset_id, table_id, schema, partition_field):
        self.tables.append((dataset_id, table_id, partition_field))
        return "verified"


def test_preflight_is_validate_only_and_checks_f04_f06_resources():
    preflight = importlib.import_module("pipeline.preflight")
    settings = GcpSettings(
        project_id="proj",
        bucket_name="bucket",
        location="asia-southeast1",
        runtime_service_account="runtime@proj.iam.gserviceaccount.com",
    )
    gcs = FakeGcs()
    bq = FakeBigQuery()

    summary = preflight.run_preflight(
        settings=settings,
        gcs_adapter=gcs,
        bigquery_adapter=bq,
    )

    assert gcs.calls == [("bucket", "asia-southeast1", False)]
    assert {dataset_id for dataset_id, _ in bq.datasets} == {
        "bahtflow_raw",
        "bahtflow_ops",
        "bahtflow_analytics",
    }
    assert set(bq.tables) == {
        ("bahtflow_raw", "transactions", "batch_date"),
        ("bahtflow_raw", "fx_rates", "rate_date"),
        ("bahtflow_analytics", "transactions_accepted", "batch_date"),
        ("bahtflow_ops", "transactions_quarantine", "batch_date"),
        ("bahtflow_analytics", "fct_transactions", "batch_date"),
    }
    assert summary.datasets_verified == 3
    assert summary.tables_verified == 5
