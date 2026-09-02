from datetime import date, datetime, timezone
from decimal import Decimal
import importlib

import pandas as pd
import pytest


BATCH_DATE = date(2025, 7, 22)
WHEN = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)


def _load_module():
    return importlib.import_module("pipeline.currency_fact_load")


def _fact_module():
    return importlib.import_module("pipeline.currency_fact")


def _fx_module():
    return importlib.import_module("pipeline.fx_resolution")


def accepted_row(txn, amount, currency, source_row_id, row_number):
    return {
        "txn": txn,
        "transaction_dt": datetime(2025, 7, 22, 9, 30),
        "amount": Decimal(amount),
        "currency": currency,
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": row_number,
        "source_row_id": source_row_id,
        "batch_date": BATCH_DATE,
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
        "classified_at": datetime(2026, 9, 2, 1, 0, tzinfo=timezone.utc),
    }


def fx_row(currency, rate, row_number):
    return {
        "rate_date_raw": BATCH_DATE.isoformat(),
        "currency": currency,
        "mid_rate": rate,
        "rate_unit": "THB",
        "source_provider": "BOT",
        "source_url": "https://example.test/fx",
        "source_file": "fx/2025/07/fx_20250722.csv",
        "source_checksum": "fx-abc",
        "source_row_number": row_number,
        "source_row_id": f"fx-{currency}",
        "rate_date": BATCH_DATE,
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }


def row_matches_date(row, partition_date):
    return row["batch_date"] in (partition_date, partition_date.isoformat())


class StatefulFactFake:
    def __init__(self):
        self.accepted_rows = [
            accepted_row("T1", "320", "THB", "row-1", 1),
            accepted_row("T2", "10", "USD", "row-2", 2),
        ]
        self.fx_rows = [fx_row("USD", "32", 1), fx_row("EUR", "40", 2)]
        self.fact_rows = []

    def query_partition_rows(
        self, dataset_id, table_id, partition_field, partition_date, columns
    ):
        assert (dataset_id, table_id, partition_field) == (
            "bahtflow_analytics",
            "transactions_accepted",
            "batch_date",
        )
        return [
            {column: row[column] for column in columns}
            for row in self.accepted_rows
            if row_matches_date(row, partition_date)
        ]

    def query_rows_through_date(
        self, dataset_id, table_id, date_field, through_date, columns
    ):
        assert (dataset_id, table_id, date_field) == (
            "bahtflow_raw",
            "fx_rates",
            "rate_date",
        )
        return [
            {column: row[column] for column in columns}
            for row in self.fx_rows
            if row["rate_date"] <= through_date
        ]

    def query_source_row_ids(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        assert (dataset_id, table_id, partition_field) == (
            "bahtflow_analytics",
            "fct_transactions",
            "batch_date",
        )
        return {
            row["source_row_id"]
            for row in self.fact_rows
            if row_matches_date(row, partition_date)
        }

    def append_rows(self, dataset_id, table_id, rows, schema):
        assert (dataset_id, table_id) == (
            "bahtflow_analytics",
            "fct_transactions",
        )
        self.fact_rows.extend(rows)
        return len(rows)

    def query_partition_row_count(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        assert partition_field == "batch_date"
        if (dataset_id, table_id) == (
            "bahtflow_analytics",
            "transactions_accepted",
        ):
            return sum(
                1
                for row in self.accepted_rows
                if row_matches_date(row, partition_date)
            )
        assert (dataset_id, table_id) == (
            "bahtflow_analytics",
            "fct_transactions",
        )
        return sum(
            1 for row in self.fact_rows if row_matches_date(row, partition_date)
        )


def test_first_run_builds_fact_and_rerun_inserts_zero():
    load = _load_module()
    fake = StatefulFactFake()
    first = load.build_and_load_currency_fact(
        batch_date=BATCH_DATE,
        bigquery_adapter=fake,
        fact_created_at=WHEN,
    )
    second = load.build_and_load_currency_fact(
        batch_date=BATCH_DATE,
        bigquery_adapter=fake,
        fact_created_at=WHEN,
    )
    assert first.accepted_rows == 2
    assert first.fact_rows == 2
    assert first.fact_inserted_rows == 2
    assert first.accepted_partition_rows == 2
    assert first.fact_partition_rows == 2
    assert first.reconciled is True
    assert second.fact_inserted_rows == 0
    assert second.fact_partition_rows == 2
    assert second.reconciled is True


def test_retry_with_one_persisted_fact_appends_only_missing_row():
    load = _load_module()
    fact_module = _fact_module()
    fx_module = _fx_module()
    fake = StatefulFactFake()
    snapshot = fx_module.EffectiveFxSnapshot(
        fx_rate_date=BATCH_DATE,
        usd_thb_rate=Decimal("32"),
        eur_thb_rate=Decimal("40"),
        is_carried_forward=False,
        staleness_days=0,
    )
    generated = fact_module.build_currency_fact(
        pd.DataFrame(fake.accepted_rows),
        BATCH_DATE,
        snapshot,
        WHEN,
    )
    first_record = generated.iloc[0].to_dict()
    first_record["batch_date"] = first_record["batch_date"].isoformat()
    fake.fact_rows.append(first_record)

    summary = load.build_and_load_currency_fact(
        batch_date=BATCH_DATE,
        bigquery_adapter=fake,
        fact_created_at=WHEN,
    )

    assert summary.fact_inserted_rows == 1
    assert summary.fact_partition_rows == 2
    assert summary.reconciled is True


class MismatchedCountFake(StatefulFactFake):
    def query_partition_row_count(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        count = super().query_partition_row_count(
            dataset_id, table_id, partition_field, partition_date
        )
        if (dataset_id, table_id) == (
            "bahtflow_analytics",
            "fct_transactions",
        ):
            return count + 1
        return count


def test_persisted_fact_count_mismatch_fails():
    load = _load_module()
    fake = MismatchedCountFake()
    with pytest.raises(
        load.CurrencyFactLoadError,
        match="Persisted currency fact reconciliation failed",
    ):
        load.build_and_load_currency_fact(
            batch_date=BATCH_DATE,
            bigquery_adapter=fake,
            fact_created_at=WHEN,
        )
