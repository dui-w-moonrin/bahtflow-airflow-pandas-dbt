# Daily regional source contract

`daily_regional_sales/` is the immutable input fixture for the BahtFlow batch pipeline. It contains 360 logical business-day batches from **2025-07-22** through **2026-07-16**. Every batch has exactly five gzip CSV files, so there are **1,800 physical files** and **3,672,845 data rows** in total.

## Path and schema

```text
daily_regional_sales/
  business_date=YYYY-MM-DD/
    sales_{region}_YYYYMMDD.csv.gz
```

`region` is one of `bkk`, `central`, `north`, `northeast`, or `south`. Region intentionally lives in the filename rather than as a CSV column; the pipeline derives it from the source path.

Every decompressed file has this exact header and no additional columns:

```csv
txn,dtts,amount,currency
```

`dtts` is a timestamp in `YYYY-MM-DD HH:MM:SS`, and each row's calendar date must match its enclosing `business_date` directory.

## Deliberately preserved data quality issues

This is not a cleaned analytics dataset. It intentionally retains invalid monetary values (for example `N/A`), duplicate transactions, and conflicting records. A future Pandas intake stage will report file-level quality evidence; dbt will then make classifications and quarantine rules explicit. Do not deduplicate or coerce these source files in place.

## Manifest

`daily_source_manifest.csv` is written by `scripts/validate_daily_source.py`. It contains one row per compressed source file, sorted by business date and region:

```text
business_date,region,file_path,rows,min_dtts,max_dtts,compressed_bytes,sha256
```

The SHA-256 and compressed-byte fields make every committed input file independently auditable. Regenerate the manifest only after re-running the full validator:

```powershell
python scripts/validate_daily_source.py `
  --root data\daily_regional_sales `
  --manifest data\daily_source_manifest.csv
```
