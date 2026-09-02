from datetime import date

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipeline.bigquery_adapter import BigQueryAdapter, BigQueryContractError


class FakeQueryJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeLoadJob:
    def result(self):
        return self


class FakeMappingRow:
    def __init__(self, values):
        self._values = values

    def items(self):
        return self._values.items()


class FakeClient:
    def __init__(self):
        self.datasets = {}
        self.tables = {}
        self.query_value = 0
        self.query_rows = None
        self.query_calls = []
        self.load_calls = []

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

    def query(self, sql, job_config=None):
        self.query_calls.append((sql, job_config))
        rows = self.query_rows if self.query_rows is not None else [(self.query_value,)]
        return FakeQueryJob(rows)

    def load_table_from_json(self, rows, destination, job_config=None):
        self.load_calls.append((rows, destination, job_config))
        return FakeLoadJob()


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


def test_existing_table_partition_type_drift_fails():
    client = FakeClient()
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)
    table = bigquery.Table("proj.bahtflow_raw.transactions", schema=list(schema))
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="batch_date",
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


def test_query_source_row_ids_is_partition_scoped():
    client = FakeClient()
    client.query_rows = [("id-a",), ("id-b",)]
    adapter = BigQueryAdapter("proj", client=client)
    result = adapter.query_source_row_ids(
        "bahtflow_raw", "transactions", "batch_date", date(2025, 7, 22)
    )
    assert result == {"id-a", "id-b"}
    sql, job_config = client.query_calls[-1]
    assert "`proj.bahtflow_raw.transactions`" in sql
    assert "WHERE batch_date = @partition_date" in sql
    parameter = job_config.query_parameters[0]
    assert parameter.name == "partition_date"
    assert parameter.type_ == "DATE"
    assert parameter.value == date(2025, 7, 22)


def test_append_rows_uses_write_append_load_job():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    schema = (bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),)
    rows = [{"source_row_id": "id-a"}, {"source_row_id": "id-b"}]
    assert adapter.append_rows("bahtflow_raw", "transactions", rows, schema) == 2
    loaded_rows, destination, job_config = client.load_calls[-1]
    assert loaded_rows == rows
    assert destination == "proj.bahtflow_raw.transactions"
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert [(f.name, f.field_type, f.mode) for f in job_config.schema] == [
        ("source_row_id", "STRING", "REQUIRED")
    ]


def test_append_rows_skips_empty_input():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    assert adapter.append_rows("bahtflow_raw", "transactions", [], ()) == 0
    assert client.load_calls == []


def test_query_partition_row_count_is_partition_scoped():
    client = FakeClient()
    client.query_rows = [(8978,)]
    adapter = BigQueryAdapter("proj", client=client)
    count = adapter.query_partition_row_count(
        "bahtflow_raw", "transactions", "batch_date", date(2025, 7, 22)
    )
    assert count == 8978
    sql, job_config = client.query_calls[-1]
    assert "SELECT COUNT(*)" in sql
    assert "WHERE batch_date = @partition_date" in sql
    assert job_config.query_parameters[0].value == date(2025, 7, 22)


def test_query_partition_rows_selects_requested_columns_and_returns_dicts():
    client = FakeClient()
    client.query_rows = [
        FakeMappingRow({"txn": "TX-1", "source_row_id": "row-1"}),
        FakeMappingRow({"txn": "TX-2", "source_row_id": "row-2"}),
    ]
    adapter = BigQueryAdapter("proj", client=client)
    rows = adapter.query_partition_rows(
        "bahtflow_raw",
        "transactions",
        "batch_date",
        date(2025, 7, 22),
        ("txn", "source_row_id"),
    )
    assert rows == [
        {"txn": "TX-1", "source_row_id": "row-1"},
        {"txn": "TX-2", "source_row_id": "row-2"},
    ]
    sql, job_config = client.query_calls[-1]
    assert "SELECT txn, source_row_id" in sql
    assert "`proj.bahtflow_raw.transactions`" in sql
    assert "WHERE batch_date = @partition_date" in sql
    assert job_config.query_parameters[0].value == date(2025, 7, 22)


def test_query_rows_through_date_is_parameterized_and_returns_dicts():
    client = FakeClient()
    client.query_rows = [
        FakeMappingRow({"rate_date": date(2025, 7, 21), "currency": "USD"}),
        FakeMappingRow({"rate_date": date(2025, 7, 21), "currency": "EUR"}),
    ]
    adapter = BigQueryAdapter("proj", client=client)

    rows = adapter.query_rows_through_date(
        "bahtflow_raw",
        "fx_rates",
        "rate_date",
        date(2025, 7, 22),
        ("rate_date", "currency"),
    )

    assert rows == [
        {"rate_date": date(2025, 7, 21), "currency": "USD"},
        {"rate_date": date(2025, 7, 21), "currency": "EUR"},
    ]
    sql, job_config = client.query_calls[-1]
    assert "SELECT rate_date, currency" in sql
    assert "`proj.bahtflow_raw.fx_rates`" in sql
    assert "WHERE rate_date <= @through_date" in sql
    parameter = job_config.query_parameters[0]
    assert parameter.name == "through_date"
    assert parameter.type_ == "DATE"
    assert parameter.value == date(2025, 7, 22)


def test_validate_dataset_returns_verified_for_existing_match():
    client = FakeClient()
    dataset = bigquery.Dataset("proj.bahtflow_raw")
    dataset.location = "asia-southeast1"
    client.datasets["proj.bahtflow_raw"] = dataset
    adapter = BigQueryAdapter("proj", client=client)

    assert adapter.validate_dataset("bahtflow_raw", "ASIA-SOUTHEAST1") == "verified"


def test_validate_dataset_missing_fails_without_creation():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)

    with pytest.raises(BigQueryContractError, match="Dataset does not exist"):
        adapter.validate_dataset("bahtflow_raw", "asia-southeast1")

    assert client.datasets == {}


def test_validate_partitioned_table_returns_verified_for_existing_match():
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
        adapter.validate_partitioned_table(
            "bahtflow_raw", "transactions", schema, "batch_date"
        )
        == "verified"
    )


def test_validate_partitioned_table_missing_fails_without_creation():
    client = FakeClient()
    adapter = BigQueryAdapter("proj", client=client)
    schema = (bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),)

    with pytest.raises(BigQueryContractError, match="Table does not exist"):
        adapter.validate_partitioned_table(
            "bahtflow_raw", "transactions", schema, "batch_date"
        )

    assert client.tables == {}
