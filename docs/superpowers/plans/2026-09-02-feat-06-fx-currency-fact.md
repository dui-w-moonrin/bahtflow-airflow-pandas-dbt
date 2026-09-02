# Feature 06: FX + Currency Fact Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the latest published USD/EUR FX snapshot at or before one logical batch date, convert every Feature 05 accepted transaction into THB/USD/EUR with Decimal arithmetic, and persist one idempotent BigQuery fact row per accepted transaction.

**Architecture:** Keep effective-FX selection, validation, BigQuery-NUMERIC normalization, and currency conversion in focused Pandas/Python modules. BigQuery is used only for exact-schema bootstrap, date/partition-scoped reads, append-only persistence, ID lookups, and counts. A one-batch loader composes those units and a narrow CLI exposes it; Airflow wiring remains Feature 07 scope.

**Tech Stack:** Python 3.12, Pandas 2.3.3, Python `decimal.Decimal`, `google-cloud-bigquery` 3.44.0, pytest, Docker Compose, BigQuery.

**Spec:** `docs/superpowers/specs/2026-09-02-feat-06-fx-currency-fact-design.md`

## Global Constraints

- Work on branch `feat/06-fx-currency-fact`, forked from `main` at Feature 05 completion commit `8915b5aab3161add8c3bf54b708014f66d3ecbc1`.
- Use the user's normal feature-branch workflow; do not create or require a Git worktree.
- Pandas/Python owns FX validation, effective-date resolution, conversion, and derived fields.
- BigQuery SQL may only perform warehouse mechanics such as date-bounded reads, partition reads, ID lookups, and counts; it must not resolve effective FX or calculate converted amounts.
- Resolve the **latest published** `rate_date <= batch_date`; if that newest publication is malformed, fail rather than falling back to an older valid publication.
- The selected FX snapshot must contain exactly one USD row and one EUR row, with no unsupported extra currency.
- `mid_rate` means THB per one unit of USD or EUR.
- There is no maximum staleness threshold in v1.
- All monetary and FX arithmetic uses `Decimal`; `float` is forbidden in the conversion path.
- BigQuery `NUMERIC` storage normalization uses maximum scale 9, maximum integer digits 29, and `ROUND_HALF_EVEN` only when scale reduction is required.
- `bahtflow_analytics.fct_transactions` is DAY-partitioned by `batch_date` and contains exactly one row per accepted transaction.
- Reuse stable `source_row_id` as the fact idempotency key; persistence is target-partition anti-filter + `WRITE_APPEND` under the existing single-writer assumption.
- No `MERGE`, replace, truncate, delete, persisted effective-FX table, second quarantine stage, dbt, Airflow F07 wiring, or F08 mart work in this feature.
- Primary live acceptance date is `2025-07-22`; Feature 05 measured 8,803 accepted rows for this partition.
- Do not guess live FX rates, converted amounts, or first-run inserted counts; measure them from BigQuery.

## File Map

- Create `pipeline/bigquery_numeric.py` — shared F06 Decimal parsing and BigQuery-NUMERIC normalization.
- Create `pipeline/fx_resolution.py` — raw FX latest-publication selection, validation, and `EffectiveFxSnapshot`.
- Create `pipeline/currency_fact.py` — accepted-row assumptions, THB-bridge conversion, fact projection, and in-memory reconciliation.
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
    assert [(f.name, f.field_type, f.mode) for f in FACT_TRANSACTIONS_SCHEMA] == [
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

```powershell
pytest tests/pipeline/test_bigquery_contract.py::test_currency_fact_contract_is_exact -v
```

Expected: collection/import failure because the F06 fact constants do not exist.

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
- Consumes: existing `BigQueryAdapter` query pattern.
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

- [ ] **Step 2: Run the adapter test and observe RED**

```powershell
pytest tests/pipeline/test_bigquery_adapter.py::test_query_rows_through_date_is_parameterized_and_returns_dicts -v
```

Expected: FAIL with `AttributeError` because `query_rows_through_date` is not implemented.

- [ ] **Step 3: Add the warehouse-mechanics reader**

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

Do not add `MAX(rate_date)`, business currency filtering, arithmetic, joins, or fallback logic to SQL.

- [ ] **Step 4: Run adapter tests**

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
- Consumes: `Decimal` or raw numeric text.
- Produces: `BigQueryNumericError`; `normalize_bigquery_numeric(value: Decimal) -> Decimal`; `parse_bigquery_numeric_text(value) -> Decimal`.

- [ ] **Step 1: Write failing normalization tests**

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

- [ ] **Step 2: Run helper tests and observe RED**

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
        normalized = (
            value.quantize(NUMERIC_QUANTUM, rounding=ROUND_HALF_EVEN)
            if value.as_tuple().exponent < -9
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
        raise BigQueryNumericError(
            f"Invalid BigQuery NUMERIC value: {value!r}"
        ) from exc
    return normalize_bigquery_numeric(parsed)
```

- [ ] **Step 4: Run helper tests**

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
- Consumes: raw FX history DataFrame and `batch_date: date`.
- Produces: `FxResolutionError`; immutable `EffectiveFxSnapshot`; `RAW_FX_COLUMNS`; `resolve_effective_fx(raw_fx_df: pd.DataFrame, batch_date: date) -> EffectiveFxSnapshot`.

- [ ] **Step 1: Write initial RED tests for same-day and carry-forward selection**

Create `tests/pipeline/test_fx_resolution.py`:

```python
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from pipeline.fx_resolution import FxResolutionError, resolve_effective_fx


def fx_row(rate_date, currency, mid_rate, *, rate_date_raw=None, row_number=None):
    return {
        "rate_date_raw": rate_date_raw or rate_date.isoformat(),
        "currency": currency,
        "mid_rate": mid_rate,
        "rate_unit": "THB",
        "source_provider": "BOT",
        "source_url": "https://example.test/fx",
        "source_file": f"fx/{rate_date:%Y}/{rate_date:%m}/fx_{rate_date:%Y%m%d}.csv",
        "source_checksum": "abc",
        "source_row_number": row_number or (1 if currency == "USD" else 2),
        "source_row_id": f"{rate_date}-{currency}-{row_number or 0}",
        "rate_date": rate_date,
        "ingested_at": "2026-09-02T00:00:00Z",
    }


def pair(rate_date, usd="32.50", eur="37.25"):
    return [
        fx_row(rate_date, "USD", usd, row_number=1),
        fx_row(rate_date, "EUR", eur, row_number=2),
    ]


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

- [ ] **Step 2: Run initial resolver tests and observe RED**

```powershell
pytest tests/pipeline/test_fx_resolution.py -v
```

Expected: collection/import failure because `pipeline.fx_resolution` does not exist.

- [ ] **Step 3: Implement the successful resolver path**

Create `pipeline/fx_resolution.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
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


def _as_date(value, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise FxResolutionError(f"Invalid {label}: {value!r}") from exc


def _require_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in RAW_FX_COLUMNS if column not in frame.columns]
    if missing:
        raise FxResolutionError(f"Missing raw FX columns: {missing!r}")


def _positive_rate(value, currency: str) -> Decimal:
    try:
        rate = parse_bigquery_numeric_text(value)
    except BigQueryNumericError as exc:
        raise FxResolutionError(f"Invalid FX rate for {currency}: {value!r}") from exc
    if rate <= Decimal("0"):
        raise FxResolutionError(f"Invalid FX rate for {currency}: {value!r}")
    return rate


def resolve_effective_fx(raw_fx_df: pd.DataFrame, batch_date: date) -> EffectiveFxSnapshot:
    _require_columns(raw_fx_df)
    frame = raw_fx_df.loc[:, list(RAW_FX_COLUMNS)].copy()
    frame["_rate_date"] = frame["rate_date"].map(
        lambda value: _as_date(value, "rate_date")
    )
    eligible = frame.loc[frame["_rate_date"].map(lambda d: d <= batch_date)].copy()
    if eligible.empty:
        raise FxResolutionError(
            f"No FX publication at or before batch_date={batch_date.isoformat()}"
        )

    latest_date = max(eligible["_rate_date"])
    selected = eligible.loc[eligible["_rate_date"] == latest_date].copy()
    selected["_currency"] = selected["currency"].map(
        lambda value: str(value).strip().upper()
    )
    if len(selected) != 2 or set(selected["_currency"]) != {"USD", "EUR"}:
        raise FxResolutionError(
            f"Latest FX publication is not an exact USD/EUR pair: {latest_date}"
        )

    rates = {}
    for record in selected.to_dict(orient="records"):
        raw_date = _as_date(record["rate_date_raw"], "rate_date_raw")
        if raw_date != latest_date:
            raise FxResolutionError(
                f"rate_date_raw mismatch: raw={raw_date} canonical={latest_date}"
            )
        currency = record["_currency"]
        rates[currency] = _positive_rate(record["mid_rate"], currency)

    staleness_days = (batch_date - latest_date).days
    return EffectiveFxSnapshot(
        fx_rate_date=latest_date,
        usd_thb_rate=rates["USD"],
        eur_thb_rate=rates["EUR"],
        is_carried_forward=latest_date < batch_date,
        staleness_days=staleness_days,
    )
```

- [ ] **Step 4: Run the initial resolver tests and observe GREEN**

```powershell
pytest tests/pipeline/test_fx_resolution.py -v
```

Expected: both initial tests PASS.

- [ ] **Step 5: Add RED tests for malformed latest publications**

Append to `tests/pipeline/test_fx_resolution.py`:

```python
def test_no_prior_publication_fails():
    with pytest.raises(FxResolutionError, match="No FX publication"):
        resolve_effective_fx(
            pd.DataFrame(pair(date(2025, 7, 23))),
            date(2025, 7, 22),
        )


def test_latest_publication_missing_eur_fails_without_falling_back():
    rows = pair(date(2025, 7, 20))
    rows.append(fx_row(date(2025, 7, 21), "USD", "32.00", row_number=1))
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_latest_publication_missing_usd_fails_without_falling_back():
    rows = pair(date(2025, 7, 20))
    rows.append(fx_row(date(2025, 7, 21), "EUR", "37.00", row_number=2))
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_duplicate_currency_fails():
    d = date(2025, 7, 21)
    rows = pair(d) + [fx_row(d, "USD", "32.60", row_number=3)]
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_unsupported_extra_currency_fails():
    d = date(2025, 7, 21)
    rows = pair(d) + [fx_row(d, "JPY", "0.22", row_number=3)]
    with pytest.raises(FxResolutionError, match="exact USD/EUR pair"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


@pytest.mark.parametrize("rate", ["", "N/A", "NaN", "Infinity", "0", "-1"])
def test_invalid_or_nonpositive_rate_fails(rate):
    d = date(2025, 7, 21)
    rows = [
        fx_row(d, "USD", rate, row_number=1),
        fx_row(d, "EUR", "37.25", row_number=2),
    ]
    with pytest.raises(FxResolutionError, match="Invalid FX rate"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))


def test_rate_date_raw_mismatch_fails():
    d = date(2025, 7, 21)
    rows = [
        fx_row(
            d,
            "USD",
            "32.50",
            rate_date_raw="2025-07-20",
            row_number=1,
        ),
        fx_row(d, "EUR", "37.25", row_number=2),
    ]
    with pytest.raises(FxResolutionError, match="rate_date_raw mismatch"):
        resolve_effective_fx(pd.DataFrame(rows), date(2025, 7, 22))
```

- [ ] **Step 6: Run the expanded resolver suite**

```powershell
pytest tests/pipeline/test_fx_resolution.py tests/pipeline/test_bigquery_numeric.py -v
```

Expected: all selected tests PASS because the implementation validates only the latest selected publication and never falls back.

- [ ] **Step 7: Commit Task 4**

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

- [ ] **Step 1: Write RED conversion tests**

Create `tests/pipeline/test_currency_fact.py`:

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
        "amount": Decimal(amount) if isinstance(amount, str) else amount,
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
    for txn in ("T-THB", "T-USD", "T-EUR"):
        assert by_txn.loc[txn, "amount_thb"] == Decimal("320")
        assert by_txn.loc[txn, "amount_usd"] == Decimal("10")
        assert by_txn.loc[txn, "amount_eur"] == Decimal("8")
    assert fact["source_row_id"].tolist() == ["row-thb", "row-usd", "row-eur"]
    assert fact["amount"].tolist() == [Decimal("320"), Decimal("10"), Decimal("8")]
    assert fact["currency"].tolist() == ["THB", "USD", "EUR"]
    assert fact["fx_rate_date"].tolist() == [date(2025, 7, 21)] * 3
    assert fact["is_carried_forward"].tolist() == [True] * 3
    assert fact["staleness_days"].tolist() == [1] * 3
    assert len(fact) == len(accepted)


def test_derived_division_is_normalized_to_scale_nine():
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
```

- [ ] **Step 2: Run conversion tests and observe RED**

```powershell
pytest tests/pipeline/test_currency_fact.py -v
```

Expected: collection/import failure because `pipeline.currency_fact` does not exist.

- [ ] **Step 3: Implement the successful conversion path**

Create `pipeline/currency_fact.py`:

```python
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, DecimalException, localcontext

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

VALID_CURRENCIES = frozenset({"THB", "USD", "EUR"})


def _as_batch_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise CurrencyFactError(f"Invalid accepted batch_date: {value!r}") from exc


def _validate_accepted(accepted_df: pd.DataFrame, batch_date: date) -> None:
    missing = [
        column for column in ACCEPTED_INPUT_COLUMNS if column not in accepted_df.columns
    ]
    if missing:
        raise CurrencyFactError(f"Missing accepted columns: {missing!r}")

    normalized_dates = accepted_df["batch_date"].map(_as_batch_date)
    if not normalized_dates.map(lambda value: value == batch_date).all():
        raise CurrencyFactError("Accepted batch_date does not match target batch_date")

    source_ids = accepted_df["source_row_id"].map(
        lambda value: "" if value is None else str(value).strip()
    )
    if source_ids.eq("").any() or source_ids.duplicated().any():
        raise CurrencyFactError("Accepted source_row_id must be nonblank and unique")

    for position, row in accepted_df.reset_index(drop=True).iterrows():
        amount = row["amount"]
        if not isinstance(amount, Decimal) or not amount.is_finite() or amount < 0:
            raise CurrencyFactError(
                f"Invalid accepted amount for source_row_id={row['source_row_id']}: {amount!r}"
            )
        try:
            normalized = normalize_bigquery_numeric(amount)
        except BigQueryNumericError as exc:
            raise CurrencyFactError(
                f"Invalid accepted amount for source_row_id={row['source_row_id']}"
            ) from exc
        if normalized != amount:
            raise CurrencyFactError(
                f"Accepted amount requires unexpected normalization at row={position}"
            )
        if row["currency"] not in VALID_CURRENCIES:
            raise CurrencyFactError(
                f"Invalid accepted currency for source_row_id={row['source_row_id']}: "
                f"{row['currency']!r}"
            )


def _convert_amount(
    amount: Decimal,
    currency: str,
    usd_rate: Decimal,
    eur_rate: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    with localcontext() as context:
        context.prec = 80
        if currency == "THB":
            amount_thb = amount
        elif currency == "USD":
            amount_thb = amount * usd_rate
        else:
            amount_thb = amount * eur_rate
        amount_usd = amount_thb / usd_rate
        amount_eur = amount_thb / eur_rate
    return (
        normalize_bigquery_numeric(amount_thb),
        normalize_bigquery_numeric(amount_usd),
        normalize_bigquery_numeric(amount_eur),
    )


def build_currency_fact(
    accepted_df: pd.DataFrame,
    batch_date: date,
    fx: EffectiveFxSnapshot,
    fact_created_at: datetime,
) -> pd.DataFrame:
    _validate_accepted(accepted_df, batch_date)
    try:
        usd_rate = normalize_bigquery_numeric(fx.usd_thb_rate)
        eur_rate = normalize_bigquery_numeric(fx.eur_thb_rate)
    except BigQueryNumericError as exc:
        raise CurrencyFactError("FX snapshot contains nonrepresentable rate") from exc
    if usd_rate <= 0 or eur_rate <= 0:
        raise CurrencyFactError("FX snapshot rates must be positive")

    records = []
    for row in accepted_df.loc[:, list(ACCEPTED_INPUT_COLUMNS)].to_dict(orient="records"):
        try:
            amount_thb, amount_usd, amount_eur = _convert_amount(
                row["amount"], row["currency"], usd_rate, eur_rate
            )
        except (BigQueryNumericError, DecimalException, ZeroDivisionError) as exc:
            raise CurrencyFactError(
                f"Currency conversion failed for source_row_id={row['source_row_id']}"
            ) from exc
        records.append(
            {
                **row,
                "amount_thb": amount_thb,
                "amount_usd": amount_usd,
                "amount_eur": amount_eur,
                "fx_rate_date": fx.fx_rate_date,
                "usd_thb_rate": usd_rate,
                "eur_thb_rate": eur_rate,
                "is_carried_forward": fx.is_carried_forward,
                "staleness_days": fx.staleness_days,
                "fact_created_at": fact_created_at,
            }
        )

    fact = pd.DataFrame(records, columns=list(FACT_COLUMNS))
    if len(accepted_df) != len(fact):
        raise CurrencyFactError(
            f"Fact count reconciliation failed: accepted={len(accepted_df)} fact={len(fact)}"
        )
    accepted_ids = set(accepted_df["source_row_id"])
    fact_ids = set(fact["source_row_id"])
    if accepted_ids != fact_ids:
        raise CurrencyFactError("Fact source_row_id reconciliation failed")
    return fact
```

- [ ] **Step 4: Run initial conversion tests and observe GREEN**

```powershell
pytest tests/pipeline/test_currency_fact.py -v
```

Expected: initial conversion tests PASS.

- [ ] **Step 5: Add RED tests for impossible post-F05 states and overflow**

Append to `tests/pipeline/test_currency_fact.py`:

```python
@pytest.mark.parametrize("currency", ["JPY", "", None])
def test_impossible_accepted_currency_fails(currency):
    accepted = pd.DataFrame([accepted_row("T", "1", currency, "row-1")])
    with pytest.raises(CurrencyFactError, match="accepted currency"):
        build_currency_fact(accepted, BATCH_DATE, FX, WHEN)


def test_non_decimal_accepted_amount_fails():
    accepted = pd.DataFrame([accepted_row("T", 1.5, "THB", "row-1")])
    with pytest.raises(CurrencyFactError, match="accepted amount"):
        build_currency_fact(accepted, BATCH_DATE, FX, WHEN)


def test_negative_accepted_amount_fails():
    accepted = pd.DataFrame([accepted_row("T", "-1", "THB", "row-1")])
    with pytest.raises(CurrencyFactError, match="accepted amount"):
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


def test_derived_numeric_overflow_fails():
    huge_fx = EffectiveFxSnapshot(
        fx_rate_date=BATCH_DATE,
        usd_thb_rate=Decimal("99999999999999999999999999999"),
        eur_thb_rate=Decimal("40"),
        is_carried_forward=False,
        staleness_days=0,
    )
    accepted = pd.DataFrame([
        accepted_row("T", "99999999999999999999999999999", "USD", "row-1")
    ])
    with pytest.raises(CurrencyFactError, match="Currency conversion failed"):
        build_currency_fact(accepted, BATCH_DATE, huge_fx, WHEN)
```

- [ ] **Step 6: Run the expanded conversion suite**

```powershell
pytest tests/pipeline/test_currency_fact.py tests/pipeline/test_bigquery_numeric.py tests/pipeline/test_fx_resolution.py -v
```

Expected: all selected tests PASS.

- [ ] **Step 7: Commit Task 5**

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
- Consumes: adapter methods `query_partition_rows`, `query_rows_through_date`, `query_source_row_ids`, `append_rows`, `query_partition_row_count`; `resolve_effective_fx`; `build_currency_fact`.
- Produces: `CurrencyFactLoadError`; `CurrencyFactLoadSummary`; `build_and_load_currency_fact(batch_date: date, bigquery_adapter, fact_created_at: datetime | None = None) -> CurrencyFactLoadSummary`.

- [ ] **Step 1: Write the stateful fake and RED persistence tests**

Create `tests/pipeline/test_currency_fact_load.py`:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from pipeline.currency_fact import build_currency_fact
from pipeline.currency_fact_load import (
    CurrencyFactLoadError,
    build_and_load_currency_fact,
)
from pipeline.fx_resolution import EffectiveFxSnapshot


BATCH_DATE = date(2025, 7, 22)
WHEN = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
FX = EffectiveFxSnapshot(
    fx_rate_date=BATCH_DATE,
    usd_thb_rate=Decimal("32"),
    eur_thb_rate=Decimal("40"),
    is_carried_forward=False,
    staleness_days=0,
)


def accepted_row(txn, amount, currency, source_row_id, row_number):
    return {
        "txn": txn,
        "transaction_dt": datetime(2025, 7, 22, 9, 30),
        "amount": Decimal(amount),
        "currency": currency,
        "region": "bkk",
        "source_file": "transactions/business_date=2025-07-22/sales_bkk_20250722.csv.gz",
        "source_checksum": "abc",
        "source_row_number": row_number,
        "source_row_id": source_row_id,
        "batch_date": BATCH_DATE,
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
        "classified_at": datetime(2026, 9, 2, 1, 0, tzinfo=timezone.utc),
    }


def fx_row(currency, rate, row_number):
    return {
        "rate_date_raw": BATCH_DATE.isoformat(),
        "currency": currency,
        "mid_rate": rate,
        "rate_unit": "THB",
        "source_provider": "BOT",
        "source_url": "https://example.test/fx",
        "source_file": "fx/2025/07/fx_20250722.csv",
        "source_checksum": "fx-abc",
        "source_row_number": row_number,
        "source_row_id": f"fx-{currency}",
        "rate_date": BATCH_DATE,
        "ingested_at": datetime(2026, 9, 2, tzinfo=timezone.utc),
    }


def row_matches_date(row, partition_date):
    return row["batch_date"] in (partition_date, partition_date.isoformat())


class StatefulFactFake:
    def __init__(self):
        self.accepted_rows = [
            accepted_row("T1", "320", "THB", "row-1", 1),
            accepted_row("T2", "10", "USD", "row-2", 2),
        ]
        self.fx_rows = [fx_row("USD", "32", 1), fx_row("EUR", "40", 2)]
        self.fact_rows = []

    def query_partition_rows(
        self, dataset_id, table_id, partition_field, partition_date, columns
    ):
        assert (dataset_id, table_id, partition_field) == (
            "bahtflow_analytics",
            "transactions_accepted",
            "batch_date",
        )
        return [
            {column: row[column] for column in columns}
            for row in self.accepted_rows
            if row_matches_date(row, partition_date)
        ]

    def query_rows_through_date(
        self, dataset_id, table_id, date_field, through_date, columns
    ):
        assert (dataset_id, table_id, date_field) == (
            "bahtflow_raw",
            "fx_rates",
            "rate_date",
        )
        return [
            {column: row[column] for column in columns}
            for row in self.fx_rows
            if row["rate_date"] <= through_date
        ]

    def query_source_row_ids(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        assert (dataset_id, table_id, partition_field) == (
            "bahtflow_analytics",
            "fct_transactions",
            "batch_date",
        )
        return {
            row["source_row_id"]
            for row in self.fact_rows
            if row_matches_date(row, partition_date)
        }

    def append_rows(self, dataset_id, table_id, rows, schema):
        assert (dataset_id, table_id) == (
            "bahtflow_analytics",
            "fct_transactions",
        )
        self.fact_rows.extend(rows)
        return len(rows)

    def query_partition_row_count(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        assert partition_field == "batch_date"
        if (dataset_id, table_id) == (
            "bahtflow_analytics",
            "transactions_accepted",
        ):
            return sum(
                1 for row in self.accepted_rows if row_matches_date(row, partition_date)
            )
        assert (dataset_id, table_id) == (
            "bahtflow_analytics",
            "fct_transactions",
        )
        return sum(
            1 for row in self.fact_rows if row_matches_date(row, partition_date)
        )


def test_first_run_builds_fact_and_rerun_inserts_zero():
    fake = StatefulFactFake()

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


def test_retry_with_one_persisted_fact_appends_only_missing_row():
    fake = StatefulFactFake()
    accepted = pd.DataFrame(fake.accepted_rows)
    generated = build_currency_fact(accepted, BATCH_DATE, FX, WHEN)
    first_record = generated.iloc[0].to_dict()
    first_record["batch_date"] = first_record["batch_date"].isoformat()
    fake.fact_rows.append(first_record)

    summary = build_and_load_currency_fact(
        batch_date=BATCH_DATE,
        bigquery_adapter=fake,
        fact_created_at=WHEN,
    )

    assert summary.fact_inserted_rows == 1
    assert summary.fact_partition_rows == 2
    assert summary.reconciled is True


class MismatchedCountFake(StatefulFactFake):
    def query_partition_row_count(
        self, dataset_id, table_id, partition_field, partition_date
    ):
        count = super().query_partition_row_count(
            dataset_id, table_id, partition_field, partition_date
        )
        if (dataset_id, table_id) == (
            "bahtflow_analytics",
            "fct_transactions",
        ):
            return count + 1
        return count


def test_persisted_fact_count_mismatch_fails():
    fake = MismatchedCountFake()
    with pytest.raises(
        CurrencyFactLoadError,
        match="Persisted currency fact reconciliation failed",
    ):
        build_and_load_currency_fact(
            batch_date=BATCH_DATE,
            bigquery_adapter=fake,
            fact_created_at=WHEN,
        )
```

- [ ] **Step 2: Run persistence tests and observe RED**

```powershell
pytest tests/pipeline/test_currency_fact_load.py -v
```

Expected: collection/import failure because `pipeline.currency_fact_load` does not exist.

- [ ] **Step 3: Implement the complete one-batch loader**

Create `pipeline/currency_fact_load.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd

from pipeline.bigquery_contract import (
    ACCEPTED_DATASET_ID,
    ACCEPTED_TABLE_ID,
    ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
    FACT_DATASET_ID,
    FACT_TABLE_ID,
    FACT_TRANSACTIONS_PARTITION_FIELD,
    FACT_TRANSACTIONS_SCHEMA,
)
from pipeline.currency_fact import ACCEPTED_INPUT_COLUMNS, build_currency_fact
from pipeline.fx_resolution import RAW_FX_COLUMNS, resolve_effective_fx
from pipeline.pandas_intake import anti_filter_existing


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


def _json_safe_value(value):
    if isinstance(value, list):
        return list(value)
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _frame_to_records(frame: pd.DataFrame) -> list[dict]:
    return [
        {key: _json_safe_value(value) for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]


def build_and_load_currency_fact(
    *,
    batch_date: date,
    bigquery_adapter,
    fact_created_at: datetime | None = None,
) -> CurrencyFactLoadSummary:
    invocation_time = fact_created_at or datetime.now(timezone.utc)
    accepted_rows = bigquery_adapter.query_partition_rows(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
        ACCEPTED_INPUT_COLUMNS,
    )
    accepted_frame = pd.DataFrame(
        accepted_rows,
        columns=list(ACCEPTED_INPUT_COLUMNS),
    )
    fx_rows = bigquery_adapter.query_rows_through_date(
        "bahtflow_raw",
        "fx_rates",
        "rate_date",
        batch_date,
        RAW_FX_COLUMNS,
    )
    fx_frame = pd.DataFrame(fx_rows, columns=list(RAW_FX_COLUMNS))
    snapshot = resolve_effective_fx(fx_frame, batch_date)
    fact = build_currency_fact(
        accepted_frame,
        batch_date,
        snapshot,
        invocation_time,
    )

    existing_ids = bigquery_adapter.query_source_row_ids(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    fact_new = anti_filter_existing(fact, existing_ids)
    inserted = bigquery_adapter.append_rows(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        _frame_to_records(fact_new),
        FACT_TRANSACTIONS_SCHEMA,
    )

    accepted_partition_rows = bigquery_adapter.query_partition_row_count(
        ACCEPTED_DATASET_ID,
        ACCEPTED_TABLE_ID,
        ACCEPTED_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    fact_partition_rows = bigquery_adapter.query_partition_row_count(
        FACT_DATASET_ID,
        FACT_TABLE_ID,
        FACT_TRANSACTIONS_PARTITION_FIELD,
        batch_date,
    )
    reconciled = accepted_partition_rows == fact_partition_rows
    if not reconciled:
        raise CurrencyFactLoadError(
            "Persisted currency fact reconciliation failed: "
            f"accepted={accepted_partition_rows} fact={fact_partition_rows}"
        )

    return CurrencyFactLoadSummary(
        batch_date=batch_date.isoformat(),
        accepted_rows=len(accepted_frame),
        fx_rate_date=snapshot.fx_rate_date.isoformat(),
        is_carried_forward=snapshot.is_carried_forward,
        staleness_days=snapshot.staleness_days,
        fact_rows=len(fact),
        fact_inserted_rows=inserted,
        accepted_partition_rows=accepted_partition_rows,
        fact_partition_rows=fact_partition_rows,
        reconciled=True,
    )
```

- [ ] **Step 4: Run persistence + upstream F06 suites**

```powershell
pytest tests/pipeline/test_bigquery_numeric.py tests/pipeline/test_fx_resolution.py tests/pipeline/test_currency_fact.py tests/pipeline/test_currency_fact_load.py -v
```

Expected: all selected tests PASS, including first run, unchanged rerun, pre-populated partial retry, and persisted mismatch failure.

- [ ] **Step 5: Commit Task 6**

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

Create `tests/scripts/test_build_currency_fact.py`:

```python
from types import SimpleNamespace

import scripts.build_currency_fact as cli


def test_main_prints_currency_fact_summary_in_stable_order(monkeypatch, capsys):
    summary = SimpleNamespace(
        batch_date="2025-07-22",
        accepted_rows=2,
        fx_rate_date="2025-07-21",
        is_carried_forward=True,
        staleness_days=1,
        fact_rows=2,
        fact_inserted_rows=2,
        accepted_partition_rows=2,
        fact_partition_rows=2,
        reconciled=True,
    )
    monkeypatch.setattr(cli, "run_currency_fact", lambda batch_date: summary)

    cli.main(["--batch-date", "2025-07-22"])

    assert capsys.readouterr().out.splitlines() == [
        "batch_date=2025-07-22",
        "accepted_rows=2",
        "fx_rate_date=2025-07-21",
        "is_carried_forward=True",
        "staleness_days=1",
        "fact_rows=2",
        "fact_inserted_rows=2",
        "accepted_partition_rows=2",
        "fact_partition_rows=2",
        "reconciled=True",
    ]
```

- [ ] **Step 2: Run CLI test and observe RED**

```powershell
pytest tests/scripts/test_build_currency_fact.py -v
```

Expected: collection/import failure because `scripts.build_currency_fact` does not exist.

- [ ] **Step 3: Implement the CLI**

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

- [ ] **Step 4: Run CLI and focused F06 tests**

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
- Consumes: completed F06 code and the existing live GCP project.
- Produces: measured acceptance evidence, documented F06 runbook, and a clean feature diff ready for the finishing workflow.

- [ ] **Step 1: Switch local checkout to F06 and run the complete local gate**

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

Required: pytest has zero failures; compile/config/diff/status/credential checks are clean or quiet.

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

Required: when absent, the first run reports `table=bahtflow_analytics.fct_transactions status=created`; the second reports `status=verified`. If the table already exists from a prior acceptance attempt, `verified` on both runs is acceptable.

- [ ] **Step 4: Record the primary acceptance pre-state for `2025-07-22`**

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

Required source evidence: `accepted_before=8803`. Record `fact_before` and latest raw FX rows exactly as observed. If `fact_before` is nonzero, do not claim the next run is a fresh first run.

- [ ] **Step 5: Execute the primary live currency-fact batch**

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

If Step 4 showed `fact_before=0`, require `fact_inserted_rows=8803`. If rows already existed, require only the measured number of missing IDs to be inserted.

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

The effective FX date/rates/staleness must be deterministic for the same immutable inputs.

- [ ] **Step 7: Inspect one THB, USD, and EUR fact row plus source-row set equality**

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
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("currency", "STRING", currency)
        ]
    )
    print(
        currency.lower() + "_sample=",
        [tuple(row) for row in c.query(sample_sql, job_config=cfg).result()],
    )
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
print(
    "source_row_id_set_difference=",
    next(iter(c.query(set_diff_sql).result()))[0],
)
'@ | docker compose --profile gcp run --rm -T gcp-toolbox python -
```

Required: all three currency samples exist; converted amounts and FX lineage are populated; `source_row_id_set_difference=0`.

- [ ] **Step 8: Attempt optional live carry-forward evidence without expanding scope**

Use this discovery query only against already-loaded accepted/raw FX state:

```powershell
@'
from pipeline.config import load_gcp_settings
from google.cloud import bigquery

s = load_gcp_settings()
c = bigquery.Client(project=s.project_id)
base = s.project_id
sql = f"""
WITH accepted_dates AS (
  SELECT DISTINCT batch_date
  FROM `{base}.bahtflow_analytics.transactions_accepted`
),
fx_dates AS (
  SELECT DISTINCT rate_date
  FROM `{base}.bahtflow_raw.fx_rates`
)
SELECT a.batch_date
FROM accepted_dates a
LEFT JOIN fx_dates f ON f.rate_date = a.batch_date
WHERE f.rate_date IS NULL
ORDER BY a.batch_date
LIMIT 5
"""
print("carry_forward_candidates=", [row[0] for row in c.query(sql).result()])
'@ | docker compose --profile gcp run --rm -T gcp-toolbox python -
```

If a candidate exists and its prior raw FX pair is already loaded, run the F06 CLI for that date and require `is_carried_forward=True`, `staleness_days>0`, and reconciliation. If no candidate exists, record unit-test carry-forward evidence and defer historical end-to-end proof to F07. Do not load extra historical accepted batches solely for F06.

- [ ] **Step 9: Update README with only measured F06 evidence**

Add `## Feature 06: FX + Currency Fact` after Feature 05. Include the two CLI commands, latest-published semantics, exact USD/EUR-pair validation, no fallback from a malformed latest publication, Decimal/NUMERIC scale-9 policy, THB-bridge formulas, FX lineage fields, append-only `source_row_id` idempotency, measured `2025-07-22` acceptance evidence, and rerun-zero evidence. Include live carry-forward claims only if Step 8 was actually executed successfully.

- [ ] **Step 10: Commit and push the README evidence**

```powershell
git add README.md
git commit -m "docs: add Feature 06 currency fact runbook"
git push origin feat/06-fx-currency-fact
```

- [ ] **Step 11: Run the fresh final local gate after the README commit**

Repeat every command in Step 1. Required: fresh zero-failure/clean evidence.

- [ ] **Step 12: Review `main...feat/06-fx-currency-fact` before finishing**

```powershell
git diff --stat main...feat/06-fx-currency-fact
git diff --check main...feat/06-fx-currency-fact
```

Reject the feature if review finds any of these:

- BigQuery SQL containing `MAX(rate_date)`, business currency selection, conversion arithmetic, or FX fallback logic;
- any `float` conversion path;
- future FX selection;
- fallback from a malformed latest publication to an older valid publication;
- `MERGE`, truncate, replace, delete, or cross-partition destructive writes;
- a persisted `fx_effective_daily` table;
- second-stage transaction quarantine;
- F07 Airflow DAG wiring or F08 mart/publication scope;
- documentation claims not supported by executed evidence.

Only after live evidence, the fresh final gate, and diff review are clean should the feature enter `superpowers:finishing-a-development-branch` for the user's integration choice. Preserve the feature branch after local merge because the user has already established the preference to keep feature branches until all features are finished.
