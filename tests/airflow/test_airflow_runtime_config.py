from pathlib import Path


ROOT = Path(__file__).parents[2]
DOCKERFILE = ROOT / "docker" / "airflow.Dockerfile"
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"


def test_custom_airflow_dockerfile_contract():
    assert DOCKERFILE.is_file(), "custom Airflow Dockerfile must exist"
    source = DOCKERFILE.read_text(encoding="utf-8")
    assert "FROM apache/airflow:3.3.1-python3.12" in source
    assert "COPY requirements-gcp.txt /tmp/requirements-gcp.txt" in source
    assert "pip install --no-cache-dir -r /tmp/requirements-gcp.txt" in source


def test_compose_custom_airflow_runtime_contract():
    source = COMPOSE.read_text(encoding="utf-8")
    for required in (
        "dockerfile: docker/airflow.Dockerfile",
        "${BAHTFLOW_AIRFLOW_IMAGE_NAME:-bahtflow-airflow:3.3.1}",
        "AIRFLOW__CORE__DAGS_FOLDER: /opt/bahtflow/airflow/dags",
        "PYTHONPATH: /opt/bahtflow",
        "GOOGLE_APPLICATION_CREDENTIALS: /var/secrets/google/application_default_credentials.json",
        "- ./:/opt/bahtflow",
        "target: /var/secrets/google/application_default_credentials.json",
        "read_only: true",
    ):
        assert required in source

    for name in (
        "BAHTFLOW_GCP_PROJECT",
        "BAHTFLOW_GCS_BUCKET",
        "BAHTFLOW_GCP_LOCATION",
        "BAHTFLOW_RUNTIME_SERVICE_ACCOUNT",
    ):
        assert f"{name}: ${{{name}}}" in source


def test_env_example_uses_custom_airflow_image_variable():
    source = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert "BAHTFLOW_AIRFLOW_IMAGE_NAME=bahtflow-airflow:3.3.1" in source
    assert "AIRFLOW_IMAGE_NAME=apache/airflow:3.3.1-python3.12" not in source
