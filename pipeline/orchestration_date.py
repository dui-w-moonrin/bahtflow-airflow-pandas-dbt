from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo


BAHTFLOW_TIMEZONE = ZoneInfo("Asia/Bangkok")


def batch_date_from_logical_date(logical_date: datetime | None) -> date:
    if logical_date is None:
        raise ValueError("Airflow logical date is required")
    if logical_date.tzinfo is None or logical_date.utcoffset() is None:
        raise ValueError("Airflow logical date must be timezone-aware")
    return logical_date.astimezone(BAHTFLOW_TIMEZONE).date()
