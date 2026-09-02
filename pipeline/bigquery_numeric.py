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
