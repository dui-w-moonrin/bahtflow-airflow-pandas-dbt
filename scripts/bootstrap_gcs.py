from __future__ import annotations

import argparse

from pipeline.config import load_gcp_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--create-if-missing", action="store_true")
    mode.add_argument("--check-only", action="store_true")
    return parser


def main() -> int:
    from pipeline.gcs_adapter import GcsAdapter

    args = build_parser().parse_args()
    settings = load_gcp_settings()
    adapter = GcsAdapter(settings.project_id)
    location = adapter.ensure_bucket(
        settings.bucket_name,
        settings.location,
        create_if_missing=args.create_if_missing,
    )
    print(f"GCS bucket verified: {settings.bucket_name} location={location}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
