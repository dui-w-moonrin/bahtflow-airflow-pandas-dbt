# Daily Source Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Create the public bahtflow-airflow-pandas-dbt repository and materialize a versioned 360-business-day BahtFlow source corpus under data/.

**Architecture:** Existing BahtFlow regional monthly gzip CSV files are the immutable input. A streaming Python splitter creates five regional gzip files for each business date: 360 logical daily batches and 1,800 physical files. It preserves the original row schema and file-name-derived region contract. A manifest and separate validator make the committed corpus auditable before Airflow is introduced.

**Tech Stack:** Python 3.13 standard library (csv, gzip, hashlib, dataclasses, pathlib), pytest, Git, GitHub CLI.

**Spec:** docs/superpowers/specs/2026-09-01-bahtflow-airflow-pandas-dbt-design.md

## Global Constraints

- Create the GitHub repository as public and leave the remote empty until the daily corpus has passed validation.
- Use C:\workspace\projects\bahtflow-databricks-declarative-pipeline\data\bootstrap_csv_gz as the source input for this phase.
- Preserve the CSV columns exactly as txn,dtts,amount,currency; do not add region to CSV rows.
- Preserve all dirty rows and duplicates. Region remains derivable from sales_{region}_YYYYMMDD.csv.gz.
- Materialize 360 business dates from 2025-07-22 through 2026-07-16, with five regional files per date.
- The expected total source row count is 3,672,845. No credentials, tokens, or Google Cloud resources are created in this phase.
- Commit after each independently testable task. Push only the validated public corpus and its documentation.

---

## File Structure

    bahtflow-airflow-pandas-dbt/
      data/
        daily_regional_sales/
          business_date=YYYY-MM-DD/
            sales_{region}_YYYYMMDD.csv.gz
        daily_source_manifest.csv
        README.md
      scripts/
        split_regional_bootstrap_to_daily.py
        validate_daily_source.py
      tests/
        test_daily_source_splitter.py
        test_daily_source_validator.py
      .gitignore
      docs/superpowers/specs/...
      docs/superpowers/plans/2026-09-01-daily-source-bootstrap.md

## Task 1: Create the Public but Empty GitHub Repository

**Files:**
- Create: .gitignore
- Modify: local Git configuration only (origin remote)

**Interfaces:**
- Consumes: initialized local repository on branch main.
- Produces: an empty public GitHub repository at dui-w-moonrin/bahtflow-airflow-pandas-dbt and a local origin remote. No branch is pushed in this task.

- [ ] **Step 1: Verify GitHub authentication and repository-name availability**

    gh auth status
    gh repo view dui-w-moonrin/bahtflow-airflow-pandas-dbt

Expected: authentication succeeds; the view command reports that the repository does not exist.

- [ ] **Step 2: Create the public remote without README, license, or initial commit**

    gh repo create dui-w-moonrin/bahtflow-airflow-pandas-dbt --public --description "Production-minded BahtFlow batch ELT pipeline with Airflow, Pandas, BigQuery, and dbt."
    git remote add origin https://github.com/dui-w-moonrin/bahtflow-airflow-pandas-dbt.git

Expected: the GitHub repository exists and has no commits; origin resolves to the exact URL above.

- [ ] **Step 3: Add public-repository safety ignores**

Create .gitignore with these exact entries:

    .env
    .env.*
    !.env.example
    *.key
    *.pem
    service-account*.json
    __pycache__/
    .pytest_cache/
    .venv/

- [ ] **Step 4: Verify the remote remains empty and the local ignore file is not pushed**

    gh repo view dui-w-moonrin/bahtflow-airflow-pandas-dbt --json name,visibility,defaultBranchRef
    git remote -v

Expected: visibility is PUBLIC, defaultBranchRef is null, and both fetch/push URLs point to the repository.

- [ ] **Step 5: Commit local repository hygiene without pushing**

    git add .gitignore
    git commit -m "Add public repository ignore rules"

## Task 2: Build a Streaming Daily Regional Source Splitter

**Files:**
- Create: scripts/split_regional_bootstrap_to_daily.py
- Create: tests/test_daily_source_splitter.py

**Interfaces:**
- Consumes: gzip CSV files named sales_{region}_YYYYMM.csv.gz with columns txn,dtts,amount,currency.
- Produces: SplitSummary(input_files: int, output_files: int, rows_written: int, business_dates: int) and daily gzip files at business_date=YYYY-MM-DD/sales_{region}_YYYYMMDD.csv.gz.
- Public API: split_source_tree(input_root: Path, output_root: Path) -> SplitSummary.

- [ ] **Step 1: Write failing splitter tests with a two-region, two-day fixture**

The test creates two gzip CSV inputs, calls split_source_tree, and asserts:

    summary = split_source_tree(input_root, output_root)

    assert summary.input_files == 2
    assert summary.business_dates == 2
    assert summary.output_files == 4
    assert summary.rows_written == 4
    assert (output_root / "business_date=2025-07-22" / "sales_north_20250722.csv.gz").exists()
    assert (output_root / "business_date=2025-07-23" / "sales_bkk_20250723.csv.gz").exists()

It also asserts every output header remains exactly txn,dtts,amount,currency and a row with amount N/A is copied unchanged.

- [ ] **Step 2: Run the splitter test to verify it fails**

    python -m pytest tests/test_daily_source_splitter.py -v

Expected: FAIL because the module and split_source_tree do not exist.

- [ ] **Step 3: Write the minimal streaming splitter**

Implement these interfaces:

    REQUIRED_COLUMNS = ("txn", "dtts", "amount", "currency")
    OUTPUT_NAME = "sales_{region}_{yyyymmdd}.csv.gz"

    def parse_region(path: Path) -> str: ...
    def business_date(row: dict[str, str]) -> str: ...
    def split_source_tree(input_root: Path, output_root: Path) -> SplitSummary: ...

Use gzip.open in text mode and csv.DictReader to read one input row at a time. Cache one csv.DictWriter per (business_date, region) target, write each header once, and close every gzip handle in a finally block. Reject an input file whose header differs from REQUIRED_COLUMNS. Do not coerce or clean field values.

- [ ] **Step 4: Run the splitter test to verify it passes**

    python -m pytest tests/test_daily_source_splitter.py -v

Expected: PASS.

- [ ] **Step 5: Commit the splitter and its tests**

    git add scripts/split_regional_bootstrap_to_daily.py tests/test_daily_source_splitter.py
    git commit -m "Add daily regional source splitter"

## Task 3: Add a Daily Corpus Manifest and Validator

**Files:**
- Create: scripts/validate_daily_source.py
- Create: tests/test_daily_source_validator.py
- Create: data/daily_source_manifest.csv (generated)

**Interfaces:**
- Consumes: daily regional source tree created by split_source_tree.
- Produces: ValidationSummary(business_dates: int, files: int, rows: int, start_date: str, end_date: str) and a manifest with columns business_date,region,file_path,rows,min_dtts,max_dtts,compressed_bytes,sha256.
- Public API: validate_daily_source(root: Path, manifest_path: Path) -> ValidationSummary.

- [ ] **Step 1: Write failing validator tests**

The fixture creates two business dates and five region files per date. It asserts:

    summary = validate_daily_source(root, manifest_path)

    assert summary.business_dates == 2
    assert summary.files == 10
    assert summary.rows == 10
    assert summary.start_date == "2025-07-22"
    assert summary.end_date == "2025-07-23"

The first manifest line must be:

    business_date,region,file_path,rows,min_dtts,max_dtts,compressed_bytes,sha256

A second test removes sales_south_20250723.csv.gz and expects a ValueError containing missing regions.

- [ ] **Step 2: Run the validator tests to verify they fail**

    python -m pytest tests/test_daily_source_validator.py -v

Expected: FAIL because the module and validate_daily_source do not exist.

- [ ] **Step 3: Write the validator and manifest writer**

Implement these invariants:

    EXPECTED_REGIONS = {"bkk", "central", "north", "northeast", "south"}
    EXPECTED_START = "2025-07-22"
    EXPECTED_END = "2026-07-16"
    EXPECTED_DAYS = 360
    EXPECTED_TOTAL_ROWS = 3_672_845

The validator scans every gzip file with csv.DictReader, verifies the four-column header, verifies each row dtts date matches its directory business date, calculates row counts/date bounds/SHA-256/compressed bytes, and writes manifest rows sorted by business date then region. The full-corpus CLI fails unless it finds exactly 360 dates, 1,800 files, the five expected regions for every date, the stated start/end dates, and 3,672,845 total rows.

- [ ] **Step 4: Run the validator tests to verify they pass**

    python -m pytest tests/test_daily_source_validator.py -v

Expected: PASS.

- [ ] **Step 5: Commit the validator and tests**

    git add scripts/validate_daily_source.py tests/test_daily_source_validator.py
    git commit -m "Add daily source validation manifest"

## Task 4: Materialize, Validate, Document, and Publish the Daily Corpus

**Files:**
- Create: data/daily_regional_sales/business_date=.../*.csv.gz (1,800 generated files)
- Create: data/daily_source_manifest.csv
- Create: data/README.md
- Create: README.md

**Interfaces:**
- Consumes: original regional source root and tested splitter/validator.
- Produces: Git-tracked daily source corpus preserving all 3,672,845 records and a public GitHub main branch.

- [ ] **Step 1: Run the splitter against the existing BahtFlow source**

    python scripts/split_regional_bootstrap_to_daily.py --input-root C:\workspace\projects\bahtflow-databricks-declarative-pipeline\data\bootstrap_csv_gz --output-root data\daily_regional_sales

Expected: 65 input files, 1,800 output files, 360 business dates, and 3,672,845 written rows.

- [ ] **Step 2: Run full-corpus validation and write manifest**

    python scripts/validate_daily_source.py --root data\daily_regional_sales --manifest data\daily_source_manifest.csv

Expected: exactly 360 business dates, 1,800 files, 3,672,845 rows, start date 2025-07-22, and end date 2026-07-16.

- [ ] **Step 3: Document the daily source contract**

Write data/README.md with the daily path convention, original CSV schema, five region filename prefixes, manifest columns, exact corpus counts, and a statement that deliberately dirty values and duplicate records are preserved for downstream dbt classification.

Write a root README.md introduction containing project purpose, Airflow/Pandas/BigQuery/dbt architecture, and a note that Airflow/dbt are implemented in later phases.

- [ ] **Step 4: Run validation and Git hygiene checks**

    python -m pytest tests -v
    git diff --check
    git status --short

Expected: all tests pass, git diff --check has no output, and the only uncommitted paths are the daily corpus, manifest, and two README files.

- [ ] **Step 5: Commit and push the validated first public release**

    git add README.md data scripts tests docs .gitignore
    git commit -m "Add validated daily BahtFlow source corpus"
    git push -u origin main
    gh repo view dui-w-moonrin/bahtflow-airflow-pandas-dbt --json name,visibility,url,defaultBranchRef

Expected: the repository is public, main is its default branch, and GitHub contains the design, plan, splitter, validator, tests, manifest, and 1,800 daily source files.
