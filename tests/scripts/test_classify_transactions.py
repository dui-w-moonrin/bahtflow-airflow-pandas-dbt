from pipeline.classification_load import ClassificationLoadSummary
from scripts import classify_transactions as cli


def test_cli_prints_classification_summary(monkeypatch, capsys):
    summary = ClassificationLoadSummary(
        batch_date="2025-07-22",
        raw_rows=4,
        accepted_rows=2,
        quarantine_rows=2,
        accepted_inserted_rows=2,
        quarantine_inserted_rows=2,
        accepted_partition_rows=2,
        quarantine_partition_rows=2,
        reconciled=True,
    )
    monkeypatch.setattr(cli, "run_classification", lambda batch_date: summary)

    cli.main(["--batch-date", "2025-07-22"])

    assert capsys.readouterr().out.splitlines() == [
        "batch_date=2025-07-22",
        "raw_rows=4",
        "accepted_rows=2",
        "quarantine_rows=2",
        "accepted_inserted_rows=2",
        "quarantine_inserted_rows=2",
        "accepted_partition_rows=2",
        "quarantine_partition_rows=2",
        "reconciled=True",
    ]
