import importlib
from datetime import datetime, timezone

import pytest


def _function():
    module = importlib.import_module("pipeline.orchestration_date")
    return module.batch_date_from_logical_date


def test_logical_date_converts_to_bangkok_before_taking_date():
    logical = datetime(2025, 7, 21, 18, 30, tzinfo=timezone.utc)
    assert _function()(logical).isoformat() == "2025-07-22"


def test_missing_logical_date_fails():
    with pytest.raises(ValueError, match="logical date is required"):
        _function()(None)


def test_naive_logical_date_fails():
    with pytest.raises(ValueError, match="timezone-aware"):
        _function()(datetime(2025, 7, 22, 0, 0))
