from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class GcpConfigError(ValueError):
    pass


@dataclass(frozen=True)
class GcpSettings:
    project_id: str
    bucket_name: str
    location: str
    runtime_service_account: str


_REQUIRED_ENV = {
    "BAHTFLOW_GCP_PROJECT": "project_id",
    "BAHTFLOW_GCS_BUCKET": "bucket_name",
    "BAHTFLOW_GCP_LOCATION": "location",
    "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT": "runtime_service_account",
}


def load_gcp_settings(env: Mapping[str, str] | None = None) -> GcpSettings:
    source = os.environ if env is None else env
    values: dict[str, str] = {}

    for env_name, field_name in _REQUIRED_ENV.items():
        raw = source.get(env_name)
        if raw is None or not raw.strip():
            raise GcpConfigError(f"Missing or blank required setting: {env_name}")
        values[field_name] = raw.strip()

    return GcpSettings(**values)
