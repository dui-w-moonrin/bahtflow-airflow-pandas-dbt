from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import scripts.load_raw_batch as cli
from pipeline.raw_load import RawLoadSummary


def test_cli_wires_adapters_and_prints_summary(monkeypatch, capsys):
    batch_date = date(2025, 7, 22)
    settings = SimpleNamespace(project_id="proj", bucket_name="bucket")
    summary = RawLoadSummary(
        batch_date="2025-07-22",
        tx_files=5,
        tx_source_rows=8978,
        tx_inserted_rows=8978,
        tx_partition_rows=8978,
        fx_status="LOADED",
        fx_source_rows=2,
        fx_inserted_rows=2,
        fx_partition_rows=2,
    )
    calls = {}

    class FakeGcsAdapter:
        def __init__(self, project_id):
            calls["gcs_project_id"] = project_id

    class FakeBigQueryAdapter:
        def __init__(self, project_id):
            calls["bq_project_id"] = project_id

    def fake_load_raw_batch(**kwargs):
        calls["load_kwargs"] = kwargs
        return summary

    monkeypatch.setattr(
        cli,
        "_parse_args",
        lambda: SimpleNamespace(batch_date=batch_date),
    )
    monkeypatch.setattr(cli, "load_gcp_settings", lambda: settings)
    monkeypatch.setattr(cli, "GcsAdapter", FakeGcsAdapter)
    monkeypatch.setattr(cli, "BigQueryAdapter", FakeBigQueryAdapter)
    monkeypatch.setattr(cli, "load_raw_batch", fake_load_raw_batch)

    cli.main()

    assert calls["gcs_project_id"] == "proj"
    assert calls["bq_project_id"] == "proj"
    assert calls["load_kwargs"]["batch_date"] == batch_date
    assert calls["load_kwargs"]["bucket_name"] == "bucket"
    assert isinstance(calls["load_kwargs"]["gcs_adapter"], FakeGcsAdapter)
    assert isinstance(calls["load_kwargs"]["bigquery_adapter"], FakeBigQueryAdapter)
    assert capsys.readouterr().out.splitlines() == [
        "batch_date=2025-07-22",
        "tx_files=5",
        "tx_source_rows=8978",
        "tx_inserted_rows=8978",
        "tx_partition_rows=8978",
        "fx_status=LOADED",
        "fx_source_rows=2",
        "fx_inserted_rows=2",
        "fx_partition_rows=2",
    ]
