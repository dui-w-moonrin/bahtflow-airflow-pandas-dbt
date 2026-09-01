from __future__ import annotations

import argparse
import csv
import gzip
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TextIO


REQUIRED_COLUMNS = ("txn", "dtts", "amount", "currency")
SOURCE_FILE_PATTERN = re.compile(r"^sales_(bkk|central|north|northeast|south)_\d{6}\.csv\.gz$")


@dataclass(frozen=True)
class SplitSummary:
    input_files: int
    output_files: int
    rows_written: int
    business_dates: int


def parse_region(path: Path) -> str:
    match = SOURCE_FILE_PATTERN.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Unsupported regional source filename: {path.name}")
    return match.group(1)


def business_date(row: dict[str, str | None]) -> str:
    timestamp = row.get("dtts")
    if timestamp is None:
        raise ValueError("Source row has no dtts value")
    try:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").date().isoformat()
    except ValueError as error:
        raise ValueError(f"Invalid dtts value: {timestamp!r}") from error


def iter_source_files(input_root: Path) -> list[Path]:
    return sorted(path for path in input_root.rglob("sales_*.csv.gz") if path.is_file())


def split_source_file(source_path: Path, output_root: Path, output_paths: set[Path]) -> int:
    region = parse_region(source_path)
    handles: dict[str, TextIO] = {}
    writers: dict[str, csv.DictWriter] = {}
    rows_written = 0

    try:
        with gzip.open(source_path, "rt", encoding="utf-8", newline="") as source_handle:
            reader = csv.DictReader(source_handle)
            if tuple(reader.fieldnames or ()) != REQUIRED_COLUMNS:
                raise ValueError(
                    f"Unexpected columns in {source_path}: {reader.fieldnames!r}; expected {list(REQUIRED_COLUMNS)!r}"
                )

            for row in reader:
                date = business_date(row)
                writer = writers.get(date)
                if writer is None:
                    yyyymmdd = date.replace("-", "")
                    output_path = output_root / f"business_date={date}" / f"sales_{region}_{yyyymmdd}.csv.gz"
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    handle = gzip.open(output_path, "wt", encoding="utf-8", newline="")
                    writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS)
                    writer.writeheader()
                    handles[date] = handle
                    writers[date] = writer
                    output_paths.add(output_path)

                writer.writerow({column: row.get(column) for column in REQUIRED_COLUMNS})
                rows_written += 1
    finally:
        for handle in handles.values():
            handle.close()

    return rows_written


def split_source_tree(input_root: Path, output_root: Path) -> SplitSummary:
    source_files = iter_source_files(input_root)
    if not source_files:
        raise ValueError(f"No regional gzip CSV files found under {input_root}")

    output_paths: set[Path] = set()
    rows_written = 0
    for source_path in source_files:
        rows_written += split_source_file(source_path, output_root, output_paths)

    return SplitSummary(
        input_files=len(source_files),
        output_files=len(output_paths),
        rows_written=rows_written,
        business_dates=len({path.parent.name.removeprefix("business_date=") for path in output_paths}),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split BahtFlow regional monthly source files into daily batches.")
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = split_source_tree(args.input_root, args.output_root)
    print(
        " ".join(
            (
                f"input_files={summary.input_files}",
                f"output_files={summary.output_files}",
                f"rows_written={summary.rows_written}",
                f"business_dates={summary.business_dates}",
            )
        )
    )


if __name__ == "__main__":
    main()
