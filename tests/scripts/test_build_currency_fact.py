from types import SimpleNamespace
import importlib


def _cli_module():
    return importlib.import_module("scripts.build_currency_fact")


def test_main_prints_currency_fact_summary_in_stable_order(monkeypatch, capsys):
    cli = _cli_module()
    summary = SimpleNamespace(
        batch_date="2025-07-22",
        accepted_rows=2,
        fx_rate_date="2025-07-21",
        is_carried_forward=True,
        staleness_days=1,
        fact_rows=2,
        fact_inserted_rows=2,
        accepted_partition_rows=2,
        fact_partition_rows=2,
        reconciled=True,
    )
    monkeypatch.setattr(cli, "run_currency_fact", lambda batch_date: summary)

    cli.main(["--batch-date", "2025-07-22"])

    assert capsys.readouterr().out.splitlines() == [
        "batch_date=2025-07-22",
        "accepted_rows=2",
        "fx_rate_date=2025-07-21",
        "is_carried_forward=True",
        "staleness_days=1",
        "fact_rows=2",
        "fact_inserted_rows=2",
        "accepted_partition_rows=2",
        "fact_partition_rows=2",
        "reconciled=True",
    ]
