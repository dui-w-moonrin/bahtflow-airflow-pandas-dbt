from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, get_current_context, task

from pipeline.bigquery_adapter import BigQueryAdapter
from pipeline.classification_load import classify_and_load_batch
from pipeline.config import load_gcp_settings
from pipeline.currency_fact_load import build_and_load_currency_fact
from pipeline.gcs_adapter import GcsAdapter
from pipeline.orchestration_date import batch_date_from_logical_date
from pipeline.preflight import run_preflight
from pipeline.raw_load import load_fx_raw_batch, load_transaction_raw_batch


@dag(
    dag_id="bahtflow_daily",
    description="BahtFlow daily Pandas pipeline through currency fact",
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bahtflow", "pandas", "bigquery"],
)
def bahtflow_daily():
    @task(task_id="resolve_batch_date")
    def resolve_batch_date() -> str:
        context = get_current_context()
        logical_date = context["dag_run"].logical_date
        return batch_date_from_logical_date(logical_date).isoformat()

    @task(task_id="preflight")
    def preflight(batch_date: str) -> dict:
        date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        summary = run_preflight(
            settings=settings,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        )
        return asdict(summary)

    @task(task_id="load_tx_raw")
    def load_tx_raw(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        summary = load_transaction_raw_batch(
            batch_date=d,
            bucket_name=settings.bucket_name,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        )
        return asdict(summary)

    @task(task_id="load_fx_raw")
    def load_fx_raw(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        summary = load_fx_raw_batch(
            batch_date=d,
            bucket_name=settings.bucket_name,
            gcs_adapter=GcsAdapter(settings.project_id),
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        )
        return asdict(summary)

    @task(task_id="classify_transactions")
    def classify_transactions_task(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        summary = classify_and_load_batch(
            batch_date=d,
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        )
        return asdict(summary)

    @task(task_id="build_currency_fact")
    def build_currency_fact_task(batch_date: str) -> dict:
        d = date.fromisoformat(batch_date)
        settings = load_gcp_settings()
        summary = build_and_load_currency_fact(
            batch_date=d,
            bigquery_adapter=BigQueryAdapter(settings.project_id),
        )
        return asdict(summary)

    batch_date = resolve_batch_date()
    ready = preflight(batch_date)
    tx_raw = load_tx_raw(batch_date)
    fx_raw = load_fx_raw(batch_date)
    classified = classify_transactions_task(batch_date)
    fact = build_currency_fact_task(batch_date)
    finish = EmptyOperator(task_id="finish")

    ready >> [tx_raw, fx_raw]
    tx_raw >> classified
    [classified, fx_raw] >> fact
    fact >> finish


bahtflow_daily()
