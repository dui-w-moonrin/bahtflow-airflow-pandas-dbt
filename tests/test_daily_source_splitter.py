from __future__ import annotations

import csv
import gzip
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPLITTER = PROJECT_ROOT / "scripts" / "split_regional_bootstrap_to_daily.py"
HEADER = ("txn", "dtts", "amount", "currency")


def write_gzip_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)


def read_gzip_csv(path: Path) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(reader.fieldnames or ()), list(reader)


def test_splitter_materializes_daily_regional_files_without_cleaning_rows(
    tmp_path: Path,
) -> None:
    """Catches a splitter that drops dirty values, changes headers, or routes a day to the wrong file."""
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"

    write_gzip_csv(
        input_root / "year=2025" / "month=07" / "sales_north_202507.csv.gz",
        [
            {"txn": "TXN-0000000001", "dtts": "2025-07-22 09:00:00", "amount": "100.00", "currency": "THB"},
            {"txn": "TXN-0000000002", "dtts": "2025-07-23 09:00:00", "amount": "N/A", "currency": "USD"},
        ],
    )
    write_gzip_csv(
        input_root / "year=2025" / "month=07" / "sales_bkk_202507.csv.gz",
        [
            {"txn": "TXN-0000000003", "dtts": "2025-07-22 10:00:00", "amount": "200.00", "currency": "EUR"},
            {"txn": "TXN-0000000004", "dtts": "2025-07-23 10:00:00", "amount": "300.00", "currency": "THB"},
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SPLITTER),
            "--input-root",
            str(input_root),
            "--output-root",
            str(output_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "input_files=2" in result.stdout
    assert "business_dates=2" in result.stdout
    assert "output_files=4" in result.stdout
    assert "rows_written=4" in result.stdout

    north_day_one = output_root / "business_date=2025-07-22" / "sales_north_20250722.csv.gz"
    bkk_day_two = output_root / "business_date=2025-07-23" / "sales_bkk_20250723.csv.gz"

    assert north_day_one.exists()
    assert bkk_day_two.exists()
    assert read_gzip_csv(north_day_one) == (
        HEADER,
        [{"txn": "TXN-0000000001", "dtts": "2025-07-22 09:00:00", "amount": "100.00", "currency": "THB"}],
    )
    assert read_gzip_csv(output_root / "business_date=2025-07-23" / "sales_north_20250723.csv.gz") == (
        HEADER,
        [{"txn": "TXN-0000000002", "dtts": "2025-07-23 09:00:00", "amount": "N/A", "currency": "USD"}],
    )
