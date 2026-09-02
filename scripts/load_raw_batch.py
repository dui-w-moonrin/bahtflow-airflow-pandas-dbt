from __future__ import annotations

import argparse
from datetime import date

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.config import load_gcp_settings
from pipeline.gcs_adapter import GcsAdapter
from pipeline.raw_load import load_raw_batch


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True, type=date.fromisoformat)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_gcp_settings()
    summary = load_raw_batch(
        batch_date=args.batch_date,
        bucket_name=settings.bucket_name,
        gcs_adapter=GcsAdapter(settings.project_id),
        bigquery_adapter=BigQueryAdapter(settings.project_id),
    )
    for key, value in (
        ("batch_date", summary.batch_date),
        ("tx_files", summary.tx_files),
        ("tx_source_rows", summary.tx_source_rows),
        ("tx_inserted_rows", summary.tx_inserted_rows),
        ("tx_partition_rows", summary.tx_partition_rows),
        ("fx_status", summary.fx_status),
        ("fx_source_rows", summary.fx_source_rows),
        ("fx_inserted_rows", summary.fx_inserted_rows),
        ("fx_partition_rows", summary.fx_partition_rows),
    ):
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
