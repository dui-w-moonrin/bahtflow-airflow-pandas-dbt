from decimal import Decimal
import importlib

import pytest


def _numeric_module():
    return importlib.import_module("pipeline.bigquery_numeric")


def test_parse_bigquery_numeric_text_preserves_valid_decimal():
    numeric = _numeric_module()
    assert numeric.parse_bigquery_numeric_text(" 35.123456789 ") == Decimal(
        "35.123456789"
    )


def test_normalize_bigquery_numeric_uses_half_even_at_scale_nine():
    numeric = _numeric_module()
    assert numeric.normalize_bigquery_numeric(Decimal("1.2345678905")) == Decimal(
        "1.234567890"
    )
    assert numeric.normalize_bigquery_numeric(Decimal("1.2345678915")) == Decimal(
        "1.234567892"
    )


@pytest.mark.parametrize("value", ["", "N/A", "NaN", "Infinity", "-Infinity"])
def test_parse_bigquery_numeric_text_rejects_invalid_or_nonfinite(value):
    numeric = _numeric_module()
    with pytest.raises(numeric.BigQueryNumericError):
        numeric.parse_bigquery_numeric_text(value)


def test_normalize_bigquery_numeric_rejects_more_than_29_integer_digits():
    numeric = _numeric_module()
    with pytest.raises(numeric.BigQueryNumericError, match="integer digits"):
        numeric.normalize_bigquery_numeric(
            Decimal("123456789012345678901234567890")
        )
