from __future__ import annotations

import argparse
from datetime import date

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.config import load_gcp_settings
from pipeline.currency_fact_load import build_and_load_currency_fact


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Build one BahtFlow converted currency fact batch"
    )
    parser.add_argument("--batch-date", required=True, type=date.fromisoformat)
    return parser.parse_args(argv)


def run_currency_fact(batch_date: date):
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)
    return build_and_load_currency_fact(
        batch_date=batch_date,
        bigquery_adapter=adapter,
    )


def main(argv=None) -> None:
    args = parse_args(argv)
    summary = run_currency_fact(args.batch_date)
    fields = (
        "batch_date",
        "accepted_rows",
        "fx_rate_date",
        "is_carried_forward",
        "staleness_days",
        "fact_rows",
        "fact_inserted_rows",
        "accepted_partition_rows",
        "fact_partition_rows",
        "reconciled",
    )
    for field in fields:
        print(f"{field}={getattr(summary, field)}")


if __name__ == "__main__":
    main()
