# Feature 06: FX + Currency Fact Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the latest published USD/EUR FX snapshot at or before one logical batch date, convert every Feature 05 accepted transaction into THB/USD/EUR with Decimal arithmetic, and persist one idempotent BigQuery fact row per accepted transaction.

**Architecture:** Keep all effective-FX selection, validation, Decimal normalization, and currency conversion in focused Pandas/Python modules. BigQuery is used only for exact-schema bootstrap, target-partition/history reads, append-only persistence, idempotency lookups, and partition counts. The one-batch loader composes those units and exposes a CLI; Airflow wiring is deferred to Feature 07.

**Tech Stack:** Python 3.12, Pandas 2.3.3, Python `decimal.Decimal`, `google-cloud-bigquery` 3.44.0, pytest, Docker Compose, BigQuery.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-06-fx-currency-fact-design.md`

## Global Constraints

- Work on branch `feat/06-fx-currency-fact`, forked from `main` at Feature 05 completion commit `8915b5aab3161add8c3bf54b708014f66d3ecbc1`.
- Use the user's normal feature-branch workflow; do not create or require a Git worktree.
- Pandas/Python owns FX validation, effective-date resolution, conversion, and derived fields.
- BigQuery SQL may only perform warehouse mechanics such as date-bounded reads, partition-scoped reads, ID lookups, and counts; it must not resolve effective FX or calculate converted amounts.
- Resolve the **latest published** `rate_date <= batch_date`; if that newest publication is malformed, fail rather than falling back to an older valid publication.
- The selected FX snapshot must contain exactly one USD row and one EUR row, with no unsupported extra currency.
- `mid_rate` semantics are THB per one unit of USD or EUR.
- There is no maximum staleness threshold in v1.
- All monetary and FX arithmetic uses `Decimal`; `float` is forbidden in the conversion path.
- BigQuery `NUMERIC` storage normalization uses maximum scale 9, maximum integer digits 29, and `ROUND_HALF_EVEN` only when scale reduction is required.
- `bahtflow_analytics.fct_transactions` is DAY-partitioned by `batch_date` and contains exactly one row per accepted transaction.
- Reuse stable `source_row_id` as the fact idempotency key; persistence is partition-scoped anti-filter + `WRITE_APPEND` under the existing single-writer assumption.
- No `MERGE`, replace, truncate, delete, persisted effective-FX table, second quarantine stage, dbt, Airflow F07 wiring, or F08 mart work in this feature.
- Primary live acceptance date is `2025-07-22`; Feature 05 measured 8,803 accepted rows for this partition.
- Do not guess live FX rates, converted amounts, or first-run inserted counts; measure them from BigQuery.

## File Map

- Create `pipeline/bigquery_numeric.py` — shared F06 Decimal parsing/BigQuery-NUMERIC normalization helper.
- Create `pipeline/fx_resolution.py` — raw FX latest-publication selection, validation, and `EffectiveFxSnapshot`.
- Create `pipeline/currency_fact.py` — accepted-row assumptions, THB bridge conversion, fact projection, and in-memory reconciliation.
- Create `pipeline/currency_fact_load.py` — BigQuery reads, anti-filter append, persisted reconciliation, and summary.
- Modify `pipeline/bigquery_contract.py` — exact `fct_transactions` schema constants.
- Modify `pipeline/bigquery_adapter.py` — narrow date-bounded row reader for raw FX history.
- Create `scripts/bootstrap_currency_fact.py` — create/verify the fact table.
- Create `scripts/build_currency_fact.py` — execute one logical batch date and print a stable summary.
- Create `tests/pipeline/test_bigquery_numeric.py`.
- Create `tests/pipeline/test_fx_resolution.py`.
- Create `tests/pipeline/test_currency_fact.py`.
- Create `tests/pipeline/test_currency_fact_load.py`.
- Modify `tests/pipeline/test_bigquery_contract.py`.
- Modify `tests/pipeline/test_bigquery_adapter.py`.
- Create `tests/scripts/test_bootstrap_currency_fact.py`.
- Create `tests/scripts/test_build_currency_fact.py`.
- Modify `README.md` only after live acceptance is measured.

---

### Task 1: Fact table contract and bootstrap

**Files:**
- Modify: `pipeline/bigquery_contract.py`
- Create: `scripts/bootstrap_currency_fact.py`
- Modify: `tests/pipeline/test_bigquery_contract.py`
- Create: `tests/scripts/test_bootstrap_currency_fact.py`

**Interfaces:**
- Consumes: existing `BigQueryAdapter.ensure_partitioned_table(dataset_id, table_id, schema, partition_field) -> str`.
- Produces: `FACT_DATASET_ID`, `FACT_TABLE_ID`, `FACT_TRANSACTIONS_SCHEMA`, `FACT_TRANSACTIONS_PARTITION_FIELD`; `bootstrap_currency_fact(adapter) -> list[tuple[str, str]]`.

- [ ] **Step 1: Write the failing fact-schema contract test**

Append to `tests/pipeline/test_bigquery_contract.py`:

```python
from pipeline.bigquery_contract import (
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
)


def test_currency_fact_contract_is_exact():
    assert FACT_DATASET_ID == "bahtflow_analytics"
    assert FACT_TABLE_ID == "fct_transactions"
    assert FACT_TRANSACTIONS_PARTITION_FIELD == "batch_date"
    assert [(field.name, field.field_type, field.mode) for field in FACT_TRANSACTIONS_SCHEMA] == [
        ("txn", "STRING", "REQUIRED"),
        ("transaction_dt", "DATETIME", "REQUIRED"),
        ("amount", "NUMERIC", "REQUIRED"),
        ("currency", "STRING", "REQUIRED"),
        ("region", "STRING", "REQUIRED"),
        ("source_file", "STRING", "REQUIRED"),
        ("source_checksum", "STRING", "REQUIRED"),
        ("source_row_number", "INTEGER", "REQUIRED"),
        ("source_row_id", "STRING", "REQUIRED"),
        ("batch_date", "DATE", "REQUIRED"),
        ("ingested_at", "TIMESTAMP", "REQUIRED"),
        ("classified_at", "TIMESTAMP", "REQUIRED"),
        ("amount_thb", "NUMERIC", "REQUIRED"),
        ("amount_usd", "NUMERIC", "REQUIRED"),
        ("amount_eur", "NUMERIC", "REQUIRED"),
        ("fx_rate_date", "DATE", "REQUIRED"),
        ("usd_thb_rate", "NUMERIC", "REQUIRED"),
        ("eur_thb_rate", "NUMERIC", "REQUIRED"),
        ("is_carried_forward", "BOOLEAN", "REQUIRED"),
        ("staleness_days", "INTEGER", "REQUIRED"),
        ("fact_created_at", "TIMESTAMP", "REQUIRED"),
    ]
```

- [ ] **Step 2: Run the contract test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py::test_currency_fact_contract_is_exact -v
```

Expected: collection/import failure because the F06 fact constants do not exist yet.

- [ ] **Step 3: Add the exact fact contract**

Append to `pipeline/bigquery_contract.py`:

```python
FACT_DATASET_ID = "bahtflow_analytics"
FACT_TABLE_ID = "fct_transactions"
FACT_TRANSACTIONS_SCHEMA = (
    bigquery.SchemaField("txn", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("transaction_dt", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("batch_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("classified_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("amount_thb", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("amount_usd", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("amount_eur", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("fx_rate_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("usd_thb_rate", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("eur_thb_rate", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("is_carried_forward", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("staleness_days", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("fact_created_at", "TIMESTAMP", mode="REQUIRED"),
)
FACT_TRANSACTIONS_PARTITION_FIELD = "batch_date"
```

- [ ] **Step 4: Run the contract test and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py::test_currency_fact_contract_is_exact -v
```

Expected: PASS.

- [ ] **Step 5: Write the failing bootstrap test**

Create `tests/scripts/test_bootstrap_currency_fact.py`:

```python
from scripts.bootstrap_currency_fact import bootstrap_currency_fact


class RecordingAdapter:
    def __init__(self):
        self.calls = []

    def ensure_partitioned_table(self, dataset_id, table_id, schema, partition_field):
        self.calls.append((dataset_id, table_id, schema, partition_field))
        return "verified"


def test_bootstrap_currency_fact_targets_only_f06_fact_table():
    adapter = RecordingAdapter()

    statuses = bootstrap_currency_fact(adapter)

    assert statuses == [("bahtflow_analytics.fct_transactions", "verified")]
    assert [
        (dataset, table, partition)
        for dataset, table, _schema, partition in adapter.calls
    ] == [("bahtflow_analytics", "fct_transactions", "batch_date")]
```

- [ ] **Step 6: Run the bootstrap test and observe RED**

Run:

```powershell
pytest tests/scripts/test_bootstrap_currency_fact.py -v
```

Expected: collection/import failure because `scripts.bootstrap_currency_fact` does not exist.

- [ ] **Step 7: Implement the narrow bootstrap script**

Create `scripts/bootstrap_currency_fact.py`:

```python
from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.bigquery_contract import (
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
)
from pipeline.config import load_gcp_settings


def bootstrap_currency_fact(adapter) -> list[tuple[str, str]]:
    status = adapter.ensure_partitioned_table(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_SCHEMA,
        FACT_TRANSACTIONS_PARTITION_FIELD,
    )
    return [(f"{FACT_DATASET_ID}.{FACT_TABLE_ID}", status)]


def main() -> None:
    settings = load_gcp_settings()
    adapter = BigQueryAdapter(settings.project_id)
    for table_name, status in bootstrap_currency_fact(adapter):
        print(f"table={table_name} status={status}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Run Task 1 tests**

Run:

```powershell
pytest tests/pipeline/test_bigquery_contract.py tests/scripts/test_bootstrap_currency_fact.py -v
```

Expected: all selected tests PASS.

- [ ] **Step 9: Commit Task 1**

```powershell
git add pipeline/bigquery_contract.py tests/pipeline/test_bigquery_contract.py scripts/bootstrap_currency_fact.py tests/scripts/test_bootstrap_currency_fact.py
git commit -m "feat: add currency fact BigQuery contract"
```

---

### Task 2: Date-bounded BigQuery row reader for FX history

**Files:**
- Modify: `pipeline/bigquery_adapter.py`
- Modify: `tests/pipeline/test_bigquery_adapter.py`

**Interfaces:**
- Consumes: existing `BigQueryAdapter` and BigQuery query parameter pattern.
- Produces: `BigQueryAdapter.query_rows_through_date(dataset_id: str, table_id: str, date_field: str, through_date: date, columns: tuple[str, ...]) -> list[dict]`.

- [ ] **Step 1: Write the failing adapter test**

Append to `tests/pipeline/test_bigquery_adapter.py`:

```python
def test_query_rows_through_date_is_parameterized_and_returns_dicts():
    client = FakeClient()
    client.query_rows = [
        FakeMappingRow({"rate_date": date(2025, 7, 21), "currency": "USD"}),
        FakeMappingRow({"rate_date": date(2025, 7, 21), "currency": "EUR"}),
    ]
    adapter = BigQueryAdapter("proj", client=client)

    rows = adapter.query_rows_through_date(
        "bahtflow_raw",
        "fx_rates",
        "rate_date",
        date(2025, 7, 22),
        ("rate_date", "currency"),
    )

    assert rows == [
        {"rate_date": date(2025, 7, 21), "currency": "USD"},
        {"rate_date": date(2025, 7, 21), "currency": "EUR"},
    ]
    sql, job_config = client.query_calls[-1]
    assert "SELECT rate_date, currency" in sql
    assert "`proj.bahtflow_raw.fx_rates`" in sql
    assert "WHERE rate_date <= @through_date" in sql
    parameter = job_config.query_parameters[0]
    assert parameter.name == "through_date"
    assert parameter.type_ == "DATE"
    assert parameter.value == date(2025, 7, 22)
```

- [ ] **Step 2: Run the test and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py::test_query_rows_through_date_is_parameterized_and_returns_dicts -v
```

Expected: FAIL with `AttributeError` because `query_rows_through_date` is not implemented.

- [ ] **Step 3: Add the minimal warehouse-mechanics reader**

Add to `BigQueryAdapter` in `pipeline/bigquery_adapter.py`:

```python
    def query_rows_through_date(
        self,
        dataset_id: str,
        table_id: str,
        date_field: str,
        through_date: date,
        columns: tuple[str, ...],
    ) -> list[dict]:
        if not columns:
            raise ValueError("columns must not be empty")

        full_id = f"{self._project_id}.{dataset_id}.{table_id}"
        select_list = ", ".join(columns)
        sql = (
            f"SELECT {select_list} FROM `{full_id}` "
            f"WHERE {date_field} <= @through_date"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("through_date", "DATE", through_date)
            ]
        )
        rows = self._client.query(sql, job_config=job_config).result()
        return [dict(row.items()) for row in rows]
```

Do not add `MAX(rate_date)`, currency filters, joins, conversion expressions, or fallback logic to SQL.

- [ ] **Step 4: Run adapter tests**

Run:

```powershell
pytest tests/pipeline/test_bigquery_adapter.py -v
```

Expected: all adapter tests PASS.

- [ ] **Step 5: Commit Task 2**

```powershell
git add pipeline/bigquery_adapter.py tests/pipeline/test_bigquery_adapter.py
git commit -m "feat: add date-bounded BigQuery row reader"
```

---

### Task 3: Shared Decimal / BigQuery NUMERIC normalization

**Files:**
- Create: `pipeline/bigquery_numeric.py`
- Create: `tests/pipeline/test_bigquery_numeric.py`

**Interfaces:**
- Consumes: Python `Decimal` values or raw numeric text.
- Produces: `BigQueryNumericError`; `normalize_bigquery_numeric(value: Decimal) -> Decimal`; `parse_bigquery_numeric_text(value) -> Decimal`.

- [ ] **Step 1: Write failing tests for parsing, scale-9 rounding, and overflow**

Create `tests/pipeline/test_bigquery_numeric.py`:

```python
from decimal import Decimal

import pytest

from pipeline.bigquery_numeric import (
    BigQueryNumericError,
    normalize_bigquery_numeric,
    parse_bigquery_numeric_text,
)


def test_parse_bigquery_numeric_text_preserves_valid_decimal():
    assert parse_bigquery_numeric_text(" 35.123456789 ") == Decimal("35.123456789")


def test_normalize_bigquery_numeric_uses_half_even_at_scale_nine():
    assert normalize_bigquery_numeric(Decimal("1.2345678905")) == Decimal("1.234567890")
    assert normalize_bigquery_numeric(Decimal("1.2345678915")) == Decimal("1.234567892")


@pytest.mark.parametrize("value", ["", "N/A", "NaN", "Infinity", "-Infinity"])
def test_parse_bigquery_numeric_text_rejects_invalid_or_nonfinite(value):
    with pytest.raises(BigQueryNumericError):
        parse_bigquery_numeric_text(value)


def test_normalize_bigquery_numeric_rejects_more_than_29_integer_digits():
    with pytest.raises(BigQueryNumericError, match="integer digits"):
        normalize_bigquery_numeric(Decimal("123456789012345678901234567890"))
```

- [ ] **Step 2: Run tests and observe RED**

Run:

```powershell
pytest tests/pipeline/test_bigquery_numeric.py -v
```

Expected: collection/import failure because `pipeline.bigquery_numeric` does not exist.

- [ ] **Step 3: Implement deterministic NUMERIC normalization**

Create `pipeline/bigquery_numeric.py`:

```python
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext


class BigQueryNumericError(ValueError):
    pass


NUMERIC_QUANTUM = Decimal("0.000000001")


def _integer_digits(value: Decimal) -> int:
    _sign, digits, exponent = value.as_tuple()
    if exponent >= 0:
        return len(digits) + exponent
    return max(len(digits) + exponent, 0)


def normalize_bigquery_numeric(value: Decimal) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise BigQueryNumericError(f"Not a finite Decimal: {value!r}")

    with localcontext() as context:
        context.prec = 80
        exponent = value.as_tuple().exponent
        normalized = (
            value.quantize(NUMERIC_QUANTUM, rounding=ROUND_HALF_EVEN)
            if exponent < -9
            else value
        )

    if _integer_digits(normalized) > 29:
        raise BigQueryNumericError(
            f"BigQuery NUMERIC integer digits exceeded: {normalized}"
        )
    return normalized


def parse_bigquery_numeric_text(value) -> Decimal:
    text = "" if value is None else str(value).strip()
    if text == "":
        raise BigQueryNumericError("Blank BigQuery NUMERIC value")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise BigQueryNumericError(f"Invalid BigQuery NUMERIC value: {value!r}") from exc
    return normalize_bigquery_numeric(parsed)
```

- [ ] **Step 4: Run helper tests**

Run:

```powershell
pytest tests/pipeline/test_bigquery_numeric.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit Task 3**

```powershell
git add pipeline/bigquery_numeric.py tests/pipeline/test_bigquery_numeric.py
git commit -m "feat: add Decimal NUMERIC normalization"
```

---

### Task 4: Effective FX resolver

**Files:**
- Create: `pipeline/fx_resolution.py`
- Create: `tests/pipeline/test_fx_resolution.py`

**Interfaces:**
- Consumes: a Pandas DataFrame containing raw FX history rows with `rate_date <= batch_date`.
- Produces: `FxResolutionError`; immutable `EffectiveFxSnapshot(fx_rate_date: date, usd_thb_rate: Decimal, eur_thb_rate: Decimal, is_carried_forward: bool, staleness_days: int)`; `resolve_effective_fx(raw_fx_df: pd.DataFrame, batch_date: date) -> EffectiveFxSnapshot`.

- [ ] **Step 1: Write the initial RED tests for same-day, carry-forward, and future exclusion**

Create `tests/pipeline/test_fx_resolution.py` with these helpers and tests:

```python
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from pipeline.fx_resolution import FxResolutionError, resolve_effective_fx


def fx_row(rate_date, currency, mid_rate, *, rate_date_raw=None):
    return {
        "rate_date_raw": rate_date_raw or rate_date.isoformat(),
        "currency": currency,
        "mid_rate": mid_rate,
        "rate_unit": "THB",
        "source_provider": "BOT",
        "source_url": "https://example.test/fx",
        "source_file": f"fx/{rate_date:%Y}/{rate_date:%m}/fx_{rate_date:%Y%m%d}.csv",
        "source_checksum": "abc",
        "source_row_number": 1 if currency == "USD" else 2,
        "source_row_id": f"{rate_date}-{currency}",
        "rate_date": rate_date,
        "ingested_at": "2026-09-02T00:00:00Z",
    }


def pair(rate_date, usd="32.50", eur="37.25"):
    return [fx_row(rate_date, "USD", usd), fx_row(rate_date, "EUR", eur)]


def test_same_day_pair_resolves_without_carry_forward():
    batch_date = date(2025, 7, 22)
    snapshot = resolve_effective_fx(pd.DataFrame(pair(batch_date)), batch_date)

    assert snapshot.fx_rate_date == batch_date
    assert snapshot.usd_thb_rate == Decimal("32.50")
    assert snapshot.eur_thb_rate == Decimal("37.25")
    assert snapshot.is_carried_forward is False
    assert snapshot.staleness_days == 0


def test_latest_prior_pair_is_carried_forward_and_future_is_ignored():
    batch_date = date(2025, 7, 22)
    rows = pair(date(2025, 7, 21), usd="32.00", eur="37.00")
    rows += pair(date(2025, 7, 23), usd="99.00", eur="99.00")

    snapshot = resolve_effective_fx(pd.DataFrame(rows), batch_date)

    assert snapshot.fx_rate_date == date(2025, 7, 21)
    assert snapshot.usd_thb_rate == Decimal("32.00")
    assert snapshot.eur_thb_rate == Decimal("37.00")
    assert snapshot.is_carried_forward is True
    assert snapshot.staleness_days == 1
```

- [ ] **Step 2: Run targeted tests and observe RED**

Run:

```powershell
pytest tests/pipeline/test_fx_resolution.py -v
```

Expected: collection/import failure because `pipeline.fx_resolution` does not exist.

- [ ] **Step 3: Implement only the public types and successful resolution path**

Create `pipeline/fx_resolution.py` with `RAW_FX_COLUMNS`, `FxResolutionError`, `EffectiveFxSnapshot`, required-column validation, date filtering, latest-date selection, exact USD/EUR mapping, positive Decimal rate parsing through `parse_bigquery_numeric_text`, and staleness calculation. Keep selection in Pandas; do not call BigQuery from this module.

Use this public shape:

```python
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pandas as pd

from pipeline.bigquery_numeric import BigQueryNumericError, parse_bigquery_numeric_text


RAW_FX_COLUMNS = (
    "rate_date_raw",
    "currency",
    "mid_rate",
    "rate_unit",
    "source_provider",
    "source_url",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "rate_date",
    "ingested_at",
)


class FxResolutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class EffectiveFxSnapshot:
    fx_rate_date: date
    usd_thb_rate: Decimal
    eur_thb_rate: Decimal
    is_carried_forward: bool
    staleness_days: int
```

For the successful path, filter `rate_date <= batch_date` defensively even though the adapter already applies that bound, select `max(rate_date)`, and require `set(selected["currency"]) == {"USD", "EUR"}` with exactly two rows.

- [ ] **Step 4: Run the two initial tests and observe GREEN**

Run:

```powershell
pytest tests/pipeline/test_fx_resolution.py::test_same_day_pair_resolves_without_carry_forward tests/pipeline/test_fx_resolution.py::test_latest_prior_pair_is_carried_forward_and_future_is_ignored -v
```

Expected: both PASS.

- [ ] **Step 5: Add RED edge-case tests for the full spec**

Append tests that assert `FxResolutionError` for each case below using the same `fx_row`/`pair` fixtures:

```python
def test_no_prior_publication_fails():
    rows = pair(date(2025, 7, 23))
    with pytest.raises(FxResolutionError, match="No FX publication"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_latest_publication_missing_eur_fails_without_falling_back():
    rows = pair(date(2025, 7, 20))
    rows.append(fx_row(date(2025, 7, 21), "USD", "32.00"))
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_duplicate_currency_fails():
    d = date(2025, 7, 21)
    rows = pair(d) + [fx_row(d, "USD", "32.60")]
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_unsupported_extra_currency_fails():
    d = date(2025, 7, 21)
    rows = pair(d) + [fx_row(d, "JPY", "0.22")]
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


@pytest.mark.parametrize("rate", ["", "N/A", "NaN", "0", "-1"])
def test_invalid_nonpositive_rate_fails(rate):
    d = date(2025, 7, 21)
    rows = [fx_row(d, "USD", rate), fx_row(d, "EUR", "37.25")]
    with pytest.raises(FxResolutionError, match="Invalid FX rate"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_rate_date_raw_mismatch_fails():
    d = date(2025, 7, 21)
    rows = [
        fx_row(d, "USD", "32.50", rate_date_raw="2025-07-20"),
        fx_row(d, "EUR", "37.25"),
    ]
    with pytest.raises(FxResolutionError, match="rate_date_raw"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))
```

Also add a missing-USD test symmetric to missing-EUR.

- [ ] **Step 6: Run the expanded suite and observe RED failures for unimplemented validation**

Run:

```powershell
pytest tests/pipeline/test_fx_resolution.py -v
```

Expected: at least one edge-case test FAILS until all specified validation is implemented.

- [ ] **Step 7: Implement the remaining validation minimally**

Update `resolve_effective_fx` so the **selected latest publication only** is validated. Parse canonical `rate_date` values as Python `date` if BigQuery provided date objects; if test fixtures or fakes provide ISO strings, normalize with `date.fromisoformat(str(value))`. Require every selected `rate_date_raw` to parse with `date.fromisoformat(str(value).strip())` and equal the selected canonical date. Convert `currency` with `str(value).strip().upper()` only for validation/mapping; raw FX table remains unchanged. Wrap `BigQueryNumericError` as `FxResolutionError("Invalid FX rate ...")`. Require each parsed rate `> Decimal("0")`.

- [ ] **Step 8: Run the full FX resolver suite**

Run:

```powershell
pytest tests/pipeline/test_fx_resolution.py tests/pipeline/test_bigquery_numeric.py -v
```

Expected: all selected tests PASS.

- [ ] **Step 9: Commit Task 4**

```powershell
git add pipeline/fx_resolution.py tests/pipeline/test_fx_resolution.py
git commit -m "feat: resolve effective FX snapshots"
```

---

### Task 5: Currency fact transformation

**Files:**
- Create: `pipeline/currency_fact.py`
- Create: `tests/pipeline/test_currency_fact.py`

**Interfaces:**
- Consumes: Feature 05 accepted DataFrame, `batch_date: date`, `EffectiveFxSnapshot`, `fact_created_at: datetime`.
- Produces: `CurrencyFactError`; `ACCEPTED_INPUT_COLUMNS`; `FACT_COLUMNS`; `build_currency_fact(accepted_df: pd.DataFrame, batch_date: date, fx: EffectiveFxSnapshot, fact_created_at: datetime) -> pd.DataFrame`.

- [ ] **Step 1: Write the RED happy-path conversion tests**

Create `tests/pipeline/test_currency_fact.py` with one THB, one USD, and one EUR accepted row. Use amounts that make expected Decimal math obvious, for example rates USD=32 and EUR=40:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from pipeline.currency_fact import CurrencyFactError, build_currency_fact
from pipeline.fx_resolution import EffectiveFxSnapshot


BATCH_DATE = date(2025, 7, 22)
WHEN = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
FX = EffectiveFxSnapshot(
    fx_rate_date=date(2025, 7, 21),
    usd_thb_rate=Decimal("32"),
    eur_thb_rate=Decimal("40"),
    is_carried_forward=True,
    staleness_days=1,
)


def accepted_row(txn, amount, currency, source_row_id):
    return {
        "txn": txn,
        "transaction_dt": datetime(2025, 7, 22, 9, 30),
        "amount": Decimal(amount),
        "currency": currency,
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": 1,
        "source_row_id": source_row_id,
        "batch_date": BATCH_DATE,
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
        "classified_at": datetime(2026, 9, 2, 1, 0, tzinfo=timezone.utc),
    }


def test_build_currency_fact_converts_thb_usd_eur_and_preserves_identity():
    accepted = pd.DataFrame([
        accepted_row("T-THB", "320", "THB", "row-thb"),
        accepted_row("T-USD", "10", "USD", "row-usd"),
        accepted_row("T-EUR", "8", "EUR", "row-eur"),
    ])

    fact = build_currency_fact(accepted, BATCH_DATE, FX, WHEN)

    by_txn = fact.set_index("txn")
    assert by_txn.loc["T-THB", "amount_thb"] == Decimal("320")
    assert by_txn.loc["T-THB", "amount_usd"] == Decimal("10")
    assert by_txn.loc["T-THB", "amount_eur"] == Decimal("8")
    assert by_txn.loc["T-USD", "amount_thb"] == Decimal("320")
    assert by_txn.loc["T-USD", "amount_usd"] == Decimal("10")
    assert by_txn.loc["T-USD", "amount_eur"] == Decimal("8")
    assert by_txn.loc["T-EUR", "amount_thb"] == Decimal("320")
    assert by_txn.loc["T-EUR", "amount_usd"] == Decimal("10")
    assert by_txn.loc["T-EUR", "amount_eur"] == Decimal("8")
    assert fact["source_row_id"].tolist() == ["row-thb", "row-usd", "row-eur"]
    assert len(fact) == len(accepted)
```

- [ ] **Step 2: Run and observe RED**

Run:

```powershell
pytest tests/pipeline/test_currency_fact.py::test_build_currency_fact_converts_thb_usd_eur_and_preserves_identity -v
```

Expected: collection/import failure because `pipeline.currency_fact` does not exist.

- [ ] **Step 3: Implement the minimal successful conversion path**

Create `pipeline/currency_fact.py` with:

```python
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, localcontext

import pandas as pd

from pipeline.bigquery_numeric import BigQueryNumericError, normalize_bigquery_numeric
from pipeline.fx_resolution import EffectiveFxSnapshot


class CurrencyFactError(RuntimeError):
    pass


ACCEPTED_INPUT_COLUMNS = (
    "txn",
    "transaction_dt",
    "amount",
    "currency",
    "region",
    "source_file",
    "source_checksum",
    "source_row_number",
    "source_row_id",
    "batch_date",
    "ingested_at",
    "classified_at",
)

FACT_COLUMNS = ACCEPTED_INPUT_COLUMNS + (
    "amount_thb",
    "amount_usd",
    "amount_eur",
    "fx_rate_date",
    "usd_thb_rate",
    "eur_thb_rate",
    "is_carried_forward",
    "staleness_days",
    "fact_created_at",
)
```

Implement a row conversion using `localcontext()` with precision 80, THB bridge formulas from the spec, and `normalize_bigquery_numeric` on `amount_thb`, `amount_usd`, `amount_eur`, `usd_thb_rate`, and `eur_thb_rate`. Preserve the original accepted `amount` and `currency` fields unchanged.

- [ ] **Step 4: Run the happy-path test and observe GREEN**

Run the targeted test from Step 2. Expected: PASS.

- [ ] **Step 5: Add RED tests for precision and impossible post-F05 states**

Append tests asserting:

```python
def test_derived_division_is_normalized_to_scale_nine_half_even():
    fx = EffectiveFxSnapshot(
        fx_rate_date=BATCH_DATE,
        usd_thb_rate=Decimal("3"),
        eur_thb_rate=Decimal("7"),
        is_carried_forward=False,
        staleness_days=0,
    )
    accepted = pd.DataFrame([accepted_row("T", "1", "THB", "row-1")])

    fact = build_currency_fact(accepted, BATCH_DATE, fx, WHEN)

    assert fact.iloc[0]["amount_usd"] == Decimal("0.333333333")
    assert fact.iloc[0]["amount_eur"] == Decimal("0.142857143")


@pytest.mark.parametrize("currency", ["JPY", "", None])
def test_impossible_accepted_currency_fails(currency):
    accepted = pd.DataFrame([accepted_row("T", "1", currency, "row-1")])
    with pytest.raises(CurrencyFactError, match="accepted currency"):
        build_currency_fact(accepted, BATCH_DATE, FX, WHEN)


def test_accepted_batch_date_mismatch_fails():
    row = accepted_row("T", "1", "THB", "row-1")
    row["batch_date"] = date(2025, 7, 21)
    with pytest.raises(CurrencyFactError, match="batch_date"):
        build_currency_fact(pd.DataFrame([row]), BATCH_DATE, FX, WHEN)


def test_duplicate_source_row_id_in_accepted_fails():
    accepted = pd.DataFrame([
        accepted_row("T1", "1", "THB", "dup"),
        accepted_row("T2", "2", "THB", "dup"),
    ])
    with pytest.raises(CurrencyFactError, match="source_row_id"):
        build_currency_fact(accepted, BATCH_DATE, FX, WHEN)
```

Also test non-Decimal accepted amount and a derived value that exceeds 29 integer digits, both raising `CurrencyFactError`.

- [ ] **Step 6: Run the expanded suite and observe RED where validation is missing**

Run:

```powershell
pytest tests/pipeline/test_currency_fact.py -v
```

Expected: one or more edge-case tests FAIL until validation is complete.

- [ ] **Step 7: Implement validation and exact in-memory reconciliation**

Before conversion require all `ACCEPTED_INPUT_COLUMNS`, exact target `batch_date`, unique/nonblank `source_row_id`, `amount` values that are finite `Decimal` and non-negative, and currency in `{THB, USD, EUR}`. Wrap `BigQueryNumericError`, `decimal` arithmetic errors, or divide errors as `CurrencyFactError` with the transaction/source-row identifier in the message.

After projection verify:

```python
if len(accepted_df) != len(fact):
    raise CurrencyFactError(...)
if set(accepted_df["source_row_id"]) != set(fact["source_row_id"]):
    raise CurrencyFactError(...)
```

- [ ] **Step 8: Run conversion + numeric suites**

Run:

```powershell
pytest tests/pipeline/test_currency_fact.py tests/pipeline/test_bigquery_numeric.py tests/pipeline/test_fx_resolution.py -v
```

Expected: all selected tests PASS.

- [ ] **Step 9: Commit Task 5**

```powershell
git add pipeline/currency_fact.py tests/pipeline/test_currency_fact.py
git commit -m "feat: build converted transaction facts"
```

---

### Task 6: One-batch fact persistence, idempotency, and retry

**Files:**
- Create: `pipeline/currency_fact_load.py`
- Create: `tests/pipeline/test_currency_fact_load.py`

**Interfaces:**
- Consumes: `BigQueryAdapter.query_partition_rows`, `query_rows_through_date`, `query_source_row_ids`, `append_rows`, `query_partition_row_count`; `resolve_effective_fx`; `build_currency_fact`.
- Produces: `CurrencyFactLoadError`; immutable `CurrencyFactLoadSummary`; `build_and_load_currency_fact(batch_date: date, bigquery_adapter, fact_created_at: datetime | None = None) -> CurrencyFactLoadSummary`.

- [ ] **Step 1: Write a stateful fake and RED first-run/rerun test**

Create `tests/pipeline/test_currency_fact_load.py`. The fake should maintain accepted inputs, raw FX history, and persisted fact rows in memory. Its methods must implement the same five adapter calls used by production. Use one THB and one USD accepted row plus a complete same-day FX pair.

The first test must assert:

```python
first = build_and_load_currency_fact(
    batch_date=BATCH_DATE,
    bigquery_adapter=fake,
    fact_created_at=WHEN,
)
second = build_and_load_currency_fact(
    batch_date=BATCH_DATE,
    bigquery_adapter=fake,
    fact_created_at=WHEN,
)

assert first.accepted_rows == 2
assert first.fact_rows == 2
assert first.fact_inserted_rows == 2
assert first.accepted_partition_rows == 2
assert first.fact_partition_rows == 2
assert first.reconciled is True
assert second.fact_inserted_rows == 0
assert second.fact_partition_rows == 2
assert second.reconciled is True
```

- [ ] **Step 2: Run and observe RED**

Run:

```powershell
pytest tests/pipeline/test_currency_fact_load.py::test_first_run_builds_fact_and_rerun_inserts_zero -v
```

Expected: collection/import failure because `pipeline.currency_fact_load` does not exist.

- [ ] **Step 3: Implement the minimal loader and summary**

Create `pipeline/currency_fact_load.py` with:

```python
from dataclasses import dataclass
from datetime import date, datetime, timezone


class CurrencyFactLoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class CurrencyFactLoadSummary:
    batch_date: str
    accepted_rows: int
    fx_rate_date: str
    is_carried_forward: bool
    staleness_days: int
    fact_rows: int
    fact_inserted_rows: int
    accepted_partition_rows: int
    fact_partition_rows: int
    reconciled: bool
```

`build_and_load_currency_fact` must:

1. read `transactions_accepted` only for the target `batch_date` using `ACCEPTED_INPUT_COLUMNS`;
2. read `bahtflow_raw.fx_rates` through the target date using `RAW_FX_COLUMNS` and `query_rows_through_date`;
3. resolve the FX snapshot;
4. build the complete fact DataFrame;
5. query existing fact IDs for the target partition;
6. use existing `pipeline.pandas_intake.anti_filter_existing` on `source_row_id`;
7. serialize Decimals as strings, dates/datetimes as ISO text, lists as lists, and Pandas missing values as `None`, following the proven Feature 05 `_frame_to_records` pattern;
8. append unseen rows with `FACT_TRANSACTIONS_SCHEMA`;
9. query accepted and fact partition counts;
10. fail unless those persisted counts are equal.

Use `datetime.now(timezone.utc)` only when `fact_created_at` is not supplied.

- [ ] **Step 4: Run first-run/rerun test and observe GREEN**

Run the targeted test from Step 2. Expected: PASS.

- [ ] **Step 5: Add RED partial-retry and persisted-mismatch tests**

Add a test that pre-populates the fake fact output with exactly one of the two deterministic generated fact rows, then calls `build_and_load_currency_fact` and asserts only one row is inserted and the final partition has two rows.

Add a `MismatchedCountFake` whose fact partition count reports one extra row and assert:

```python
with pytest.raises(CurrencyFactLoadError, match="Persisted currency fact reconciliation failed"):
    build_and_load_currency_fact(...)
```

- [ ] **Step 6: Run persistence suite and observe RED if retry/count handling is incomplete**

Run:

```powershell
pytest tests/pipeline/test_currency_fact_load.py -v
```

Expected: edge-case failures until the loader handles pre-existing IDs and persisted reconciliation exactly.

- [ ] **Step 7: Complete retry and reconciliation behavior**

Make the persisted invariant explicit:

```python
reconciled = accepted_partition_rows == fact_partition_rows
if not reconciled:
    raise CurrencyFactLoadError(
        "Persisted currency fact reconciliation failed: "
        f"accepted={accepted_partition_rows} fact={fact_partition_rows}"
    )
```

Do not add rollback, delete, merge, replacement, or source/reference rewrite behavior.

- [ ] **Step 8: Run F06 pipeline suites**

Run:

```powershell
pytest tests/pipeline/test_bigquery_numeric.py tests/pipeline/test_fx_resolution.py tests/pipeline/test_currency_fact.py tests/pipeline/test_currency_fact_load.py -v
```

Expected: all selected tests PASS.

- [ ] **Step 9: Commit Task 6**

```powershell
git add pipeline/currency_fact_load.py tests/pipeline/test_currency_fact_load.py
git commit -m "feat: persist idempotent currency facts"
```

---

### Task 7: One-batch CLI

**Files:**
- Create: `scripts/build_currency_fact.py`
- Create: `tests/scripts/test_build_currency_fact.py`

**Interfaces:**
- Consumes: `build_and_load_currency_fact` and `BigQueryAdapter`.
- Produces: `parse_args(argv=None)`, `run_currency_fact(batch_date: date)`, `main(argv=None)` with stable line-oriented summary output.

- [ ] **Step 1: Write the failing CLI contract test**

Create `tests/scripts/test_build_currency_fact.py` using monkeypatch to replace `run_currency_fact` with a deterministic summary. Assert required `--batch-date` parsing and exact output field order:

```python
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
```

Use a fake summary with `batch_date="2025-07-22"`, `accepted_rows=2`, `fx_rate_date="2025-07-21"`, `is_carried_forward=True`, `staleness_days=1`, `fact_rows=2`, `fact_inserted_rows=2`, `accepted_partition_rows=2`, `fact_partition_rows=2`, `reconciled=True` and assert those ten lines are printed in that order.

- [ ] **Step 2: Run and observe RED**

Run:

```powershell
pytest tests/scripts/test_build_currency_fact.py -v
```

Expected: collection/import failure because `scripts.build_currency_fact` does not exist.

- [ ] **Step 3: Implement CLI following the F05 pattern**

Create `scripts/build_currency_fact.py`:

```python
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
```

- [ ] **Step 4: Run CLI test and focused F06 gate**

Run:

```powershell
pytest tests/scripts/test_build_currency_fact.py tests/scripts/test_bootstrap_currency_fact.py tests/pipeline/test_bigquery_numeric.py tests/pipeline/test_fx_resolution.py tests/pipeline/test_currency_fact.py tests/pipeline/test_currency_fact_load.py -v
```

Expected: all selected tests PASS.

- [ ] **Step 5: Commit Task 7**

```powershell
git add scripts/build_currency_fact.py tests/scripts/test_build_currency_fact.py
git commit -m "feat: add currency fact CLI"
```

---

### Task 8: Live acceptance, README runbook, and final review

**Files:**
- Modify: `README.md`
- No production behavior change unless live verification exposes a bug; any bug fix must stop this task and return to systematic-debugging + TDD before changing code.

**Interfaces:**
- Consumes: all completed F06 code and the existing live GCP project.
- Produces: measured acceptance evidence, documented F06 runbook, clean final diff ready for integration.

- [ ] **Step 1: Switch local checkout to the F06 branch and run the full local gate**

```powershell
git switch feat/06-fx-currency-fact
git pull --ff-only origin feat/06-fx-currency-fact
pytest

python -m py_compile `
  pipeline/bigquery_numeric.py `
  pipeline/fx_resolution.py `
  pipeline/currency_fact.py `
  pipeline/currency_fact_load.py `
  pipeline/bigquery_contract.py `
  pipeline/bigquery_adapter.py `
  scripts/bootstrap_currency_fact.py `
  scripts/build_currency_fact.py

docker compose config --quiet
git diff --check
git status --short
git ls-files | Select-String -Pattern 'application_default_credentials|service-account|\.pem$|\.key$'
```

Required: pytest has zero failures; compile/config/diff/status/credential checks are clean/quiet.

- [ ] **Step 2: Build the GCP toolbox**

```powershell
docker compose --profile gcp build gcp-toolbox
```

Required: image build exits successfully.

- [ ] **Step 3: Bootstrap the fact table twice**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_currency_fact

docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.bootstrap_currency_fact
```

Required: fresh project/table state reports `created` on first creation when absent; second run reports `verified`. If the table already exists from an earlier acceptance attempt, `verified` is acceptable on both runs.

- [ ] **Step 4: Record primary acceptance pre-state for `2025-07-22`**

Use a stdin Python script to avoid PowerShell quoting problems:

```powershell
@'
from pipeline.config import load_gcp_settings
from google.cloud import bigquery

s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
base = s.project_id
accepted = next(iter(c.query(f"""SELECT COUNT(*) FROM `{base}.bahtflow_analytics.transactions_accepted` WHERE batch_date=DATE('2025-07-22')""").result()))[0]
fact = next(iter(c.query(f"""SELECT COUNT(*) FROM `{base}.bahtflow_analytics.fct_transactions` WHERE batch_date=DATE('2025-07-22')""").result()))[0]
fx = list(c.query(f"""SELECT rate_date, currency, mid_rate FROM `{base}.bahtflow_raw.fx_rates` WHERE rate_date <= DATE('2025-07-22') ORDER BY rate_date DESC, currency LIMIT 6""").result())
print("accepted_before=", accepted)
print("fact_before=", fact)
print("latest_fx_rows=", [tuple(r) for r in fx])
'@ | docker compose --profile gcp run --rm -T gcp-toolbox python -
```

Required primary source evidence: `accepted_before=8803`. Do not proceed with a first-run insertion expectation until the actual `fact_before` state is known. If `fact_before` is nonzero from an earlier run, treat the next run as idempotency/recovery evidence rather than pretending it is a fresh first run.

- [ ] **Step 5: Run the primary live currency-fact batch**

```powershell
docker compose --profile gcp run --rm gcp-toolbox `
  python -m scripts.build_currency_fact --batch-date 2025-07-22
```

Required relationships:

```text
accepted_rows=8803
fact_rows=8803
accepted_partition_rows=8803
fact_partition_rows=8803
reconciled=True
fx_rate_date <= 2025-07-22
staleness_days >= 0
```

If Step 4 proved `fact_before=0`, also require `fact_inserted_rows=8803`. Otherwise require only that inserted count equals the number of previously missing fact `source_row_id` values; do not fabricate a first-run claim.

- [ ] **Step 6: Rerun the exact same command for idempotency evidence**

Run the Step 5 command again.

Required:

```text
accepted_rows=8803
fact_rows=8803
fact_inserted_rows=0
accepted_partition_rows=8803
fact_partition_rows=8803
reconciled=True
```

and the FX lineage fields must remain deterministic for the same immutable inputs.

- [ ] **Step 7: Inspect live THB/USD/EUR fact samples and source-row set equality**

```powershell
@'
from pipeline.config import load_gcp_settings
from google.cloud import bigquery

s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
base = s.project_id
sample_sql = f"""
SELECT txn, amount, currency, amount_thb, amount_usd, amount_eur,
       fx_rate_date, usd_thb_rate, eur_thb_rate,
       is_carried_forward, staleness_days, source_row_id
FROM `{base}.bahtflow_analytics.fct_transactions`
WHERE batch_date=DATE('2025-07-22') AND currency=@currency
ORDER BY source_file, source_row_number
LIMIT 1
"""
for currency in ("THB", "USD", "EUR"):
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("currency", "STRING", currency)])
    print(currency.lower()+"_sample=", [tuple(r) for r in c.query(sample_sql, job_config=cfg).result()])
set_diff_sql = f"""
SELECT COUNT(*) FROM (
  (SELECT source_row_id FROM `{base}.bahtflow_analytics.transactions_accepted` WHERE batch_date=DATE('2025-07-22')
   EXCEPT DISTINCT
   SELECT source_row_id FROM `{base}.bahtflow_analytics.fct_transactions` WHERE batch_date=DATE('2025-07-22'))
  UNION ALL
  (SELECT source_row_id FROM `{base}.bahtflow_analytics.fct_transactions` WHERE batch_date=DATE('2025-07-22')
   EXCEPT DISTINCT
   SELECT source_row_id FROM `{base}.bahtflow_analytics.transactions_accepted` WHERE batch_date=DATE('2025-07-22'))
)
"""
print("source_row_id_set_difference=", next(iter(c.query(set_diff_sql).result()))[0])
'@ | docker compose --profile gcp run --rm -T gcp-toolbox python -
```

Required: each currency sample exists; converted values and FX lineage are populated; `source_row_id_set_difference=0`.

- [ ] **Step 8: Optional live carry-forward demonstration only if current raw + accepted state already supports it**

Query for a date that has accepted rows but no same-day FX and a prior FX publication. If such a date is already loaded in both accepted/raw FX state, run `scripts.build_currency_fact` for it and require `is_carried_forward=True` and `staleness_days>0`. If no such currently-loaded date exists, record that carry-forward is proven by unit tests and defer end-to-end historical demonstration to F07; do not load extra historical batches merely to satisfy F06.

- [ ] **Step 9: Update README with measured F06 runbook/evidence**

Add `## Feature 06: FX + Currency Fact` after the F05 section. Document:

- bootstrap command;
- one-batch command;
- latest-published `rate_date <= batch_date` semantics;
- exact USD/EUR pair requirement;
- no fallback from malformed latest publication;
- Decimal / BigQuery NUMERIC scale-9 normalization;
- THB bridge conversion;
- FX lineage fields;
- append-only `source_row_id` idempotency;
- measured primary acceptance counts/rates/staleness from Steps 4-7;
- rerun inserted zero;
- carry-forward live evidence only if actually executed, otherwise say unit tests cover carry-forward and F07 will demonstrate it historically.

Do not document guessed rate values or claims not observed in this run.

- [ ] **Step 10: Commit README**

```powershell
git add README.md
git commit -m "docs: add Feature 06 currency fact runbook"
git push origin feat/06-fx-currency-fact
```

- [ ] **Step 11: Run the fresh final local gate after the README commit**

Repeat the complete Step 1 verification commands. Required: fresh zero-failure/clean evidence.

- [ ] **Step 12: Review `main...feat/06-fx-currency-fact` before finishing**

Run:

```powershell
git diff --stat main...feat/06-fx-currency-fact
git diff --check main...feat/06-fx-currency-fact
```

Also review changed production files against these rejection criteria:

- reject BigQuery SQL containing `MAX(rate_date)`, business currency filters used to choose the snapshot, arithmetic conversion, or FX fallback logic;
- reject any `float` conversion path;
- reject future FX selection;
- reject fallback from malformed latest publication to an older valid publication;
- reject `MERGE`, truncate, replace, delete, or cross-partition destructive writes;
- reject a persisted `fx_effective_daily` table;
- reject second-stage transaction quarantine;
- reject F07 Airflow DAG wiring or F08 mart/publication scope;
- reject documentation claims that were not measured.

Only after live evidence, the fresh final gate, and diff review are clean should the feature enter `superpowers:finishing-a-development-branch` for the user's integration choice. Preserve the feature branch after local merge if the user repeats the established preference to keep all feature branches until the project is finished.
