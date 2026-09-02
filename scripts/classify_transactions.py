from __future__ import annotations

import argparse
from datetime import date

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.classification_load import classify_and_load_batch
from pipeline.config import load_gcp_settings


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Classify one raw BahtFlow transaction batch"
    )
    parser.add_argument("--batch-date", required=True, type=date.fromisoformat)
    return parser.parse_args(argv)


def run_classification(batch_date: date):
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)
    return classify_and_load_batch(
        batch_date=batch_date,
        bigquery_adapter=adapter,
    )


def main(argv=None) -> None:
    args = parse_args(argv)
    summary = run_classification(args.batch_date)
    fields = (
        "batch_date",
        "raw_rows",
        "accepted_rows",
        "quarantine_rows",
        "accepted_inserted_rows",
        "quarantine_inserted_rows",
        "accepted_partition_rows",
        "quarantine_partition_rows",
        "reconciled",
    )
    for field in fields:
        print(f"{field}={getattr(summary, field)}")


if __name__ == "__main__":
    main()
