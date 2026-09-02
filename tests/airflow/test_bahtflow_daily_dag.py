from pathlib import Path


DAG_PATH = Path(__file__).parents[2] / "airflow" / "dags" / "bahtflow_daily.py"


def _source() -> str:
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_file_exists():
    assert DAG_PATH.is_file()


def test_dag_uses_airflow3_taskflow_api():
    source = _source()
    assert "from airflow.sdk import dag, get_current_context, task" in source
    assert "@dag(" in source
    assert "@task" in source
    assert "dbt_transform" not in source
    assert "dbt_test" not in source


def test_dag_operational_contract():
    source = _source()
    for text in (
        'schedule="@daily"',
        "catchup=False",
        "max_active_runs=1",
        '"retries": 2',
        "timedelta(minutes=2)",
        'tz="Asia/Bangkok"',
    ):
        assert text in source


def test_dag_has_exact_f07_task_ids():
    source = _source()
    for task_id in (
        "resolve_batch_date",
        "preflight",
        "load_tx_raw",
        "load_fx_raw",
        "classify_transactions",
        "build_currency_fact",
        "finish",
    ):
        assert f'task_id="{task_id}"' in source


def test_dag_uses_pipeline_services_without_dataframe_business_logic():
    source = _source()
    for symbol in (
        "load_transaction_raw_batch",
        "load_fx_raw_batch",
        "classify_and_load_batch",
        "build_and_load_currency_fact",
        "run_preflight",
    ):
        assert symbol in source
    assert "pd.DataFrame" not in source
    assert "resolve_effective_fx" not in source


def test_dependency_edges_match_f07():
    normalized = "\n".join(line.strip() for line in _source().splitlines())
    assert "ready >> [tx_raw, fx_raw]" in normalized
    assert "tx_raw >> classified" in normalized
    assert "[classified, fx_raw] >> fact" in normalized
    assert "fact >> finish" in normalized
