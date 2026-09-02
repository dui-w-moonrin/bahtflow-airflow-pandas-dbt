import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipeline.bigquery_adapter import BigQueryAdapter, BigQueryContractError


class FakeQueryJob:
    def __init__(self, value):
        self._value = value

    def result(self):
        return [(self._value,)]


class FakeClient:
    def __init__(self):
        self.datasets = {}
        self.tables = {}
        self.query_value = 0

    def get_dataset(self, full_id):
        if full_id not in self.datasets:
            raise NotFound("missing dataset")
        return self.datasets[full_id]

    def create_dataset(self, dataset):
        ref = dataset.reference
        self.datasets[f"{ref.project}.{ref.dataset_id}"] = dataset
        return dataset

    def get_table(self, full_id):
        if full_id not in self.tables:
            raise NotFound("missing table")
        return self.tables[full_id]

    def create_table(self, table):
        ref = table.reference
        self.tables[f"{ref.project}.{ref.dataset_id}.{ref.table_id}"] = table
        return table

    def query(self, sql):
        return FakeQueryJob(self.query_value)


def test_missing_dataset_is_created_in_expected_location():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)

    assert adapter.ensure_dataset("bahtflow_raw", "asia-southeast1") == "created"
    assert client.datasets["proj.bahtflow_raw"].location == "asia-southeast1"


def test_existing_dataset_same_location_is_verified():
    client = FakeClient()
    dataset = bigquery.Dataset("proj.bahtflow_raw")
    dataset.location = "asia-southeast1"
    client.datasets["proj.bahtflow_raw"] = dataset
    adapter = BigQueryAdapter("proj", client=client)

    assert adapter.ensure_dataset("bahtflow_raw", "ASIA-SOUTHEAST1") == "verified"


def test_existing_dataset_wrong_location_fails():
    client = FakeClient()
    dataset = bigquery.Dataset("proj.bahtflow_raw")
    dataset.location = "US"
    client.datasets["proj.bahtflow_raw"] = dataset
    adapter = BigQueryAdapter("proj", client=client)

    with pytest.raises(BigQueryContractError, match="Dataset location mismatch"):
        adapter.ensure_dataset("bahtflow_raw", "asia-southeast1")


def test_missing_partitioned_table_is_created():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)

    status = adapter.ensure_partitioned_table(
        "bahtflow_raw", "transactions", schema, "batch_date"
    )

    assert status == "created"
    table = client.tables["proj.bahtflow_raw.transactions"]
    assert table.time_partitioning.field == "batch_date"
    assert table.time_partitioning.type_ == "DAY"


def test_existing_table_exact_schema_and_partition_is_verified():
    client = FakeClient()
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)
    table = bigquery.Table("proj.bahtflow_raw.transactions", schema=list(schema))
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="batch_date",
    )
    client.tables["proj.bahtflow_raw.transactions"] = table
    adapter = BigQueryAdapter("proj", client=client)

    assert (
        adapter.ensure_partitioned_table(
            "bahtflow_raw", "transactions", schema, "batch_date"
        )
        == "verified"
    )


def test_existing_table_schema_drift_fails():
    client = FakeClient()
    expected = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)
    table = bigquery.Table(
        "proj.bahtflow_raw.transactions",
        schema=[bigquery.SchemaField("wrong", "STRING")],
    )
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="batch_date",
    )
    client.tables["proj.bahtflow_raw.transactions"] = table
    adapter = BigQueryAdapter("proj", client=client)

    with pytest.raises(BigQueryContractError, match="Table schema mismatch"):
        adapter.ensure_partitioned_table(
            "bahtflow_raw", "transactions", expected, "batch_date"
        )


def test_existing_table_partition_drift_fails():
    client = FakeClient()
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)
    table = bigquery.Table("proj.bahtflow_raw.transactions", schema=list(schema))
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="wrong_date",
    )
    client.tables["proj.bahtflow_raw.transactions"] = table
    adapter = BigQueryAdapter("proj", client=client)

    with pytest.raises(BigQueryContractError, match="Table partition mismatch"):
        adapter.ensure_partitioned_table(
            "bahtflow_raw", "transactions", schema, "batch_date"
        )


def test_query_scalar_returns_first_value_as_int():
    client = FakeClient()
    client.query_value = 17
    adapter = BigQueryAdapter("proj", client=client)

    assert adapter.query_scalar("SELECT 17") == 17
