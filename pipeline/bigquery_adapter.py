from __future__ import annotations

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BigQueryContractError(RuntimeError):
    pass


def _schema_shape(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


class BigQueryAdapter:
    def __init__(self, project_id: str, client=None):
        self._project_id = project_id
        self._client = client or bigquery.Client(project=project_id)

    def ensure_dataset(self, dataset_id: str, location: str) -> str:
        full_id = f"{self._project_id}.{dataset_id}"
        try:
            existing = self._client.get_dataset(full_id)
        except NotFound:
            dataset = bigquery.Dataset(full_id)
            dataset.location = location
            self._client.create_dataset(dataset)
            return "created"

        if (existing.location or "").upper() != location.upper():
            raise BigQueryContractError(
                f"Dataset location mismatch for {full_id}: "
                f"expected={location} actual={existing.location}"
            )
        return "verified"

    def ensure_partitioned_table(
        self,
        dataset_id: str,
        table_id: str,
        schema,
        partition_field: str,
    ) -> str:
        full_id = f"{self._project_id}.{dataset_id}.{table_id}"
        try:
            existing = self._client.get_table(full_id)
        except NotFound:
            table = bigquery.Table(full_id, schema=list(schema))
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
            self._client.create_table(table)
            return "created"

        if _schema_shape(existing.schema) != _schema_shape(schema):
            raise BigQueryContractError(f"Table schema mismatch for {full_id}")

        actual_partition = getattr(existing.time_partitioning, "field", None)
        actual_partition_type = getattr(existing.time_partitioning, "type_", None)
        if actual_partition != partition_field or actual_partition_type != "DAY":
            raise BigQueryContractError(
                f"Table partition mismatch for {full_id}: "
                f"expected=DAY:{partition_field} "
                f"actual={actual_partition_type}:{actual_partition}"
            )
        return "verified"

    def get_dataset(self, dataset_id: str):
        return self._client.get_dataset(f"{self._project_id}.{dataset_id}")

    def get_table(self, dataset_id: str, table_id: str):
        return self._client.get_table(
            f"{self._project_id}.{dataset_id}.{table_id}"
        )

    def query_scalar(self, sql: str) -> int:
        rows = self._client.query(sql).result()
        return int(next(iter(rows))[0])
