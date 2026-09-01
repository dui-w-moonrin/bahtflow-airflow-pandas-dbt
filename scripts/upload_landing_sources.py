from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.config import load_gcp_settings
from pipeline.gcs_workflows import upload_sources


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Upload one transaction, one FX file, and both manifests only.",
    )
    return parser


def main() -> int:
    from pipeline.gcs_adapter import GcsAdapter

    args = build_parser().parse_args()
    settings = load_gcp_settings()
    adapter = GcsAdapter(settings.project_id)
    repo_root = Path(__file__).resolve().parents[1]

    summary = upload_sources(
        repo_root,
        settings.bucket_name,
        adapter,
        smoke=args.smoke,
    )
    print(f"landing upload complete: uploaded={summary.uploaded} skipped={summary.skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
