from pathlib import Path
import re


DAG_PATH = Path(__file__).parents[2] / "airflow" / "dags" / "bahtflow_daily.py"

EXPECTED_TASK_IDS = {
    "start",
    "discover_tx_files",
    "validate_tx_files",
    "load_tx_raw",
    "discover_fx",
    "validate_fx",
    "load_fx_raw",
    "dbt_transform",
    "dbt_test",
    "reconcile",
    "finish",
}

EXPECTED_DEPENDENCIES = {
    "start >> [discover_tx_files, discover_fx]",
    "discover_tx_files >> validate_tx_files >> load_tx_raw",
    "discover_fx >> validate_fx >> load_fx_raw",
    "[load_tx_raw, load_fx_raw] >> dbt_transform",
    "dbt_transform >> dbt_test >> reconcile >> finish",
}


def _source() -> str:
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_file_exists():
    assert DAG_PATH.is_file()


def test_dag_uses_airflow3_empty_operator_contract():
    source = _source()
    assert "from airflow.sdk import DAG" in source
    assert (
        "from airflow.providers.standard.operators.empty import EmptyOperator"
        in source
    )

    task_ids = set(re.findall(r'EmptyOperator\(task_id="([^"]+)"\)', source))
    assert task_ids == EXPECTED_TASK_IDS


def test_dag_schedule_and_timezone_are_explicit():
    source = _source()
    assert 'schedule="@daily"' in source
    assert "catchup=False" in source
    assert 'tz="Asia/Bangkok"' in source


def test_dag_dependency_contract_matches_pipeline_shape():
    normalized = "\n".join(line.strip() for line in _source().splitlines())
    for dependency in EXPECTED_DEPENDENCIES:
        assert dependency in normalized
