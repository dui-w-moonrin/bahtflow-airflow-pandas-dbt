import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="bahtflow_daily",
    description="BahtFlow daily orchestration skeleton",
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    max_active_runs=4,
    tags=["bahtflow", "skeleton"],
) as dag:
    start = EmptyOperator(task_id="start")

    discover_tx_files = EmptyOperator(task_id="discover_tx_files")
    validate_tx_files = EmptyOperator(task_id="validate_tx_files")
    load_tx_raw = EmptyOperator(task_id="load_tx_raw")

    discover_fx = EmptyOperator(task_id="discover_fx")
    validate_fx = EmptyOperator(task_id="validate_fx")
    load_fx_raw = EmptyOperator(task_id="load_fx_raw")

    dbt_transform = EmptyOperator(task_id="dbt_transform")
    dbt_test = EmptyOperator(task_id="dbt_test")
    reconcile = EmptyOperator(task_id="reconcile")
    finish = EmptyOperator(task_id="finish")

    start >> [discover_tx_files, discover_fx]
    discover_tx_files >> validate_tx_files >> load_tx_raw
    discover_fx >> validate_fx >> load_fx_raw
    [load_tx_raw, load_fx_raw] >> dbt_transform
    dbt_transform >> dbt_test >> reconcile >> finish
