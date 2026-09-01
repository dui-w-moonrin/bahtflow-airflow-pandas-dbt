from __future__ import annotations

import csv
import gzip
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = PROJECT_ROOT / "scripts" / "validate_daily_source.py"
HEADER = ("txn", "dtts", "amount", "currency")
REGIONS = ("bkk", "central", "north", "northeast", "south")


def write_daily_file(root: Path, business_date: str, region: str, sequence: int) -> None:
    output_path = root / f"business_date={business_date}" / f"sales_{region}_{business_date.replace('-', '')}.csv.gz"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        writer.writerow(
            {
                "txn": f"TXN-{sequence:010d}",
                "dtts": f"{business_date} 09:00:00",
                "amount": "100.00",
                "currency": "THB",
            }
        )


def write_complete_fixture(root: Path) -> None:
    sequence = 1
    for business_date in ("2025-07-22", "2025-07-23"):
        for region in REGIONS:
            write_daily_file(root, business_date, region, sequence)
            sequence += 1


def run_validator(root: Path, manifest_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(VALIDATOR),
            "--root",
            str(root),
            "--manifest",
            str(manifest_path),
            "--allow-partial",
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_validator_writes_a_sorted_manifest_for_complete_daily_batches(tmp_path: Path) -> None:
    """Catches a validator that misses files, trusts directory dates, or emits an unusable manifest."""
    root = tmp_path / "daily"
    manifest_path = tmp_path / "daily_source_manifest.csv"
    write_complete_fixture(root)

    result = run_validator(root, manifest_path)

    assert result.returncode == 0, result.stderr
    assert "business_dates=2" in result.stdout
    assert "files=10" in result.stdout
    assert "rows=10" in result.stdout
    assert "start_date=2025-07-22" in result.stdout
    assert "end_date=2025-07-23" in result.stdout

    manifest_rows = list(csv.DictReader(manifest_path.open(encoding="utf-8", newline="")))
    assert list(manifest_rows[0]) == [
        "business_date",
        "region",
        "file_path",
        "rows",
        "min_dtts",
        "max_dtts",
        "compressed_bytes",
        "sha256",
    ]
    assert [(row["business_date"], row["region"]) for row in manifest_rows] == [
        ("2025-07-22", "bkk"),
        ("2025-07-22", "central"),
        ("2025-07-22", "north"),
        ("2025-07-22", "northeast"),
        ("2025-07-22", "south"),
        ("2025-07-23", "bkk"),
        ("2025-07-23", "central"),
        ("2025-07-23", "north"),
        ("2025-07-23", "northeast"),
        ("2025-07-23", "south"),
    ]


def test_validator_rejects_a_business_date_missing_a_region(tmp_path: Path) -> None:
    """Catches a validator that treats an incomplete daily batch as ready for Airflow processing."""
    root = tmp_path / "daily"
    manifest_path = tmp_path / "daily_source_manifest.csv"
    write_complete_fixture(root)
    (root / "business_date=2025-07-23" / "sales_south_20250723.csv.gz").unlink()

    result = run_validator(root, manifest_path)

    assert result.returncode != 0
    assert "missing regions" in result.stderr
