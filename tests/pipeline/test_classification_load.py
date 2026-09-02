from datetime import date, datetime, timezone

import pytest

import pipeline.classification_load as classification_load
from pipeline.classification_load import classify_and_load_batch


def row_matches_date(row, partition_date):
    return row["batch_date"] in (partition_date, partition_date.isoformat())


@pytest.fixture
def raw_rows_fixture():
    common = {
        "dtts": "2025-07-22 09:30:00",
        "amount": "100.00",
        "currency": "THB",
        "region": "bkk",
        "source_checksum": "abc",
        "batch_date": date(2025, 7, 22),
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }
    return [
        {
            **common,
            "txn": "TX-U",
            "source_file": (
                "transactions/business_date=2025-07-22/"
                "sales_bkk_20250722.csv.gz"
            ),
            "source_row_number": 1,
            "source_row_id": "unique",
        },
        {
            **common,
            "txn": "TX-R",
            "source_file": (
                "transactions/business_date=2025-07-22/"
                "sales_bkk_20250722.csv.gz"
            ),
            "source_row_number": 2,
            "source_row_id": "replay-winner",
        },
        {
            **common,
            "txn": "TX-R",
            "source_file": (
                "transactions/business_date=2025-07-22/"
                "sales_north_20250722.csv.gz"
            ),
            "source_row_number": 1,
            "source_row_id": "replay-loser",
        },
        {
            **common,
            "txn": "TX-I",
            "amount": "N/A",
            "source_file": (
                "transactions/business_date=2025-07-22/"
                "sales_south_20250722.csv.gz"
            ),
            "source_row_number": 1,
            "source_row_id": "invalid",
        },
    ]


class StatefulBigQueryFake:
    def __init__(self, raw_rows):
        self.raw_rows = list(raw_rows)
        self.outputs = {
            ("bahtflow_analytics", "transactions_accepted"): [],
            ("bahtflow_ops", "transactions_quarantine"): [],
        }
        self.fail_next_quarantine_append = False

    def query_partition_rows(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
        columns,
    ):
        assert (dataset_id, table_id, partition_field) == (
            "bahtflow_raw",
            "transactions",
            "batch_date",
        )
        return [
            {column: row[column] for column in columns}
            for row in self.raw_rows
            if row_matches_date(row, partition_date)
        ]

    def query_source_row_ids(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
    ):
        assert partition_field == "batch_date"
        return {
            row["source_row_id"]
            for row in self.outputs[(dataset_id, table_id)]
            if row_matches_date(row, partition_date)
        }

    def append_rows(self, dataset_id, table_id, rows, schema):
        if self.fail_next_quarantine_append and (dataset_id, table_id) == (
            "bahtflow_ops",
            "transactions_quarantine",
        ):
            self.fail_next_quarantine_append = False
            raise RuntimeError("simulated quarantine write failure")
        self.outputs[(dataset_id, table_id)].extend(rows)
        return len(rows)

    def query_partition_row_count(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
    ):
        assert partition_field == "batch_date"
        return sum(
            1
            for row in self.outputs[(dataset_id, table_id)]
            if row_matches_date(row, partition_date)
        )


def test_first_run_classifies_and_rerun_inserts_zero(raw_rows_fixture):
    fake = StatefulBigQueryFake(raw_rows_fixture)
    when = datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc)

    first = classify_and_load_batch(
        batch_date=date(2025, 7, 22),
        bigquery_adapter=fake,
        classified_at=when,
    )
    second = classify_and_load_batch(
        batch_date=date(2025, 7, 22),
        bigquery_adapter=fake,
        classified_at=when,
    )

    assert (first.raw_rows, first.accepted_rows, first.quarantine_rows) == (4, 2, 2)
    assert (first.accepted_inserted_rows, first.quarantine_inserted_rows) == (2, 2)
    assert (first.accepted_partition_rows, first.quarantine_partition_rows) == (2, 2)
    assert first.reconciled is True
    assert (second.accepted_inserted_rows, second.quarantine_inserted_rows) == (0, 0)
    assert (second.accepted_partition_rows, second.quarantine_partition_rows) == (2, 2)
    assert second.reconciled is True


class MismatchedCountFake(StatefulBigQueryFake):
    def query_partition_row_count(
        self,
        dataset_id,
        table_id,
        partition_field,
        partition_date,
    ):
        count = super().query_partition_row_count(
            dataset_id,
            table_id,
            partition_field,
            partition_date,
        )
        if (dataset_id, table_id) == (
            "bahtflow_analytics",
            "transactions_accepted",
        ):
            return count + 1
        return count


def test_persisted_reconciliation_mismatch_fails(raw_rows_fixture):
    fake = MismatchedCountFake(raw_rows_fixture)

    try:
        classify_and_load_batch(
            batch_date=date(2025, 7, 22),
            bigquery_adapter=fake,
            classified_at=datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc),
        )
    except Exception as exc:
        assert isinstance(exc, classification_load.ClassificationLoadError)
        assert "Persisted classification reconciliation failed" in str(exc)
    else:
        pytest.fail("Persisted reconciliation mismatch did not fail the run")
