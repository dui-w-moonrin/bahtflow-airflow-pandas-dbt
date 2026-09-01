from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


REQUIRED_COLUMNS = ("txn", "dtts", "amount", "currency")
EXPECTED_REGIONS = {"bkk", "central", "north", "northeast", "south"}
EXPECTED_START = "2025-07-22"
EXPECTED_END = "2026-07-16"
EXPECTED_DAYS = 360
EXPECTED_TOTAL_ROWS = 3_672_845
MANIFEST_COLUMNS = (
    "business_date",
    "region",
    "file_path",
    "rows",
    "min_dtts",
    "max_dtts",
    "compressed_bytes",
    "sha256",
)
DAILY_FILE_PATTERN = re.compile(r"^sales_(bkk|central|north|northeast|south)_(\d{8})\.csv\.gz$")


@dataclass(frozen=True)
class ValidationSummary:
    business_dates: int
    files: int
    rows: int
    start_date: str
    end_date: str


def parse_batch_directory(path: Path) -> str:
    prefix = "business_date="
    if not path.name.startswith(prefix):
        raise ValueError(f"Unexpected batch directory: {path}")
    business_date = path.name.removeprefix(prefix)
    try:
        date.fromisoformat(business_date)
    except ValueError as error:
        raise ValueError(f"Invalid business date directory: {path.name}") from error
    return business_date


def parse_daily_file(path: Path, business_date: str) -> str:
    match = DAILY_FILE_PATTERN.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Unexpected daily source filename: {path.name}")
    if match.group(2) != business_date.replace("-", ""):
        raise ValueError(f"Filename date does not match directory for {path}")
    return match.group(1)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def scan_daily_file(path: Path, business_date: str, region: str, root: Path) -> dict[str, str | int]:
    rows = 0
    min_dtts: str | None = None
    max_dtts: str | None = None

    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != REQUIRED_COLUMNS:
            raise ValueError(f"Unexpected columns in {path}: {reader.fieldnames!r}")
        for row in reader:
            timestamp = row.get("dtts")
            if timestamp is None:
                raise ValueError(f"Missing dtts value in {path}")
            try:
                row_date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").date().isoformat()
            except ValueError as error:
                raise ValueError(f"Invalid dtts value {timestamp!r} in {path}") from error
            if row_date != business_date:
                raise ValueError(f"Row date {row_date} does not match batch {business_date} in {path}")
            rows += 1
            min_dtts = timestamp if min_dtts is None or timestamp < min_dtts else min_dtts
            max_dtts = timestamp if max_dtts is None or timestamp > max_dtts else max_dtts

    return {
        "business_date": business_date,
        "region": region,
        "file_path": path.relative_to(root).as_posix(),
        "rows": rows,
        "min_dtts": min_dtts or "",
        "max_dtts": max_dtts or "",
        "compressed_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def validate_daily_source(
    root: Path,
    manifest_path: Path,
    *,
    enforce_full_corpus: bool = False,
) -> ValidationSummary:
    batch_directories = sorted(path for path in root.glob("business_date=*") if path.is_dir())
    if not batch_directories:
        raise ValueError(f"No daily batch directories found under {root}")

    manifest_rows: list[dict[str, str | int]] = []
    for batch_directory in batch_directories:
        business_date = parse_batch_directory(batch_directory)
        regional_files = sorted(path for path in batch_directory.glob("sales_*.csv.gz") if path.is_file())
        regions = {parse_daily_file(path, business_date) for path in regional_files}
        missing_regions = EXPECTED_REGIONS - regions
        unexpected_regions = regions - EXPECTED_REGIONS
        if missing_regions or unexpected_regions:
            raise ValueError(
                f"Batch {business_date} has missing regions {sorted(missing_regions)!r} "
                f"and unexpected regions {sorted(unexpected_regions)!r}"
            )
        if len(regional_files) != len(EXPECTED_REGIONS):
            raise ValueError(f"Batch {business_date} must contain exactly five regional source files")
        for path in regional_files:
            region = parse_daily_file(path, business_date)
            manifest_rows.append(scan_daily_file(path, business_date, region, root))

    manifest_rows.sort(key=lambda row: (str(row["business_date"]), str(row["region"])))
    business_dates = [str(row["business_date"]) for row in manifest_rows]
    summary = ValidationSummary(
        business_dates=len(set(business_dates)),
        files=len(manifest_rows),
        rows=sum(int(row["rows"]) for row in manifest_rows),
        start_date=min(business_dates),
        end_date=max(business_dates),
    )

    if enforce_full_corpus:
        expected = (EXPECTED_DAYS, EXPECTED_DAYS * len(EXPECTED_REGIONS), EXPECTED_TOTAL_ROWS, EXPECTED_START, EXPECTED_END)
        actual = (summary.business_dates, summary.files, summary.rows, summary.start_date, summary.end_date)
        if actual != expected:
            raise ValueError(f"Full corpus validation failed: expected {expected!r}, found {actual!r}")

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(manifest_rows)

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate BahtFlow daily regional source batches and write a manifest.")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--allow-partial", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        summary = validate_daily_source(
            args.root,
            args.manifest,
            enforce_full_corpus=not args.allow_partial,
        )
    except ValueError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error

    print(
        " ".join(
            (
                f"business_dates={summary.business_dates}",
                f"files={summary.files}",
                f"rows={summary.rows}",
                f"start_date={summary.start_date}",
                f"end_date={summary.end_date}",
            )
        )
    )


if __name__ == "__main__":
    main()
