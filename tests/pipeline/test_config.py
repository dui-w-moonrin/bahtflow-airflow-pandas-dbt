import pytest

from pipeline.config import GcpConfigError, GcpSettings, load_gcp_settings


VALID_ENV = {
    "BAHTFLOW_GCP_PROJECT": "bahtflow-dev",
    "BAHTFLOW_GCS_BUCKET": "bahtflow-dev-landing",
    "BAHTFLOW_GCP_LOCATION": "asia-southeast1",
    "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT": "bahtflow-runtime@bahtflow-dev.iam.gserviceaccount.com",
}


def test_load_gcp_settings_returns_validated_settings():
    settings = load_gcp_settings(VALID_ENV)

    assert settings == GcpSettings(
        project_id="bahtflow-dev",
        bucket_name="bahtflow-dev-landing",
        location="asia-southeast1",
        runtime_service_account="bahtflow-runtime@bahtflow-dev.iam.gserviceaccount.com",
    )


@pytest.mark.parametrize("missing_key", VALID_ENV)
def test_load_gcp_settings_rejects_missing_required_values(missing_key):
    env = VALID_ENV.copy()
    env.pop(missing_key)

    with pytest.raises(GcpConfigError, match=missing_key):
        load_gcp_settings(env)


def test_load_gcp_settings_rejects_blank_values():
    env = VALID_ENV | {"BAHTFLOW_GCS_BUCKET": "   "}

    with pytest.raises(GcpConfigError, match="BAHTFLOW_GCS_BUCKET"):
        load_gcp_settings(env)
