from scripts.bootstrap_classification import bootstrap_classification


class RecordingAdapter:
    def __init__(self):
        self.calls = []

    def ensure_partitioned_table(self, dataset_id, table_id, schema, partition_field):
        self.calls.append((dataset_id, table_id, schema, partition_field))
        return "verified"


def test_bootstrap_classification_targets_exactly_two_f05_tables():
    adapter = RecordingAdapter()
    statuses = bootstrap_classification(adapter)

    assert [name for name, _ in statuses] == [
        "bahtflow_analytics.transactions_accepted",
        "bahtflow_ops.transactions_quarantine",
    ]
    assert [status for _, status in statuses] == ["verified", "verified"]
    assert [
        (dataset, table, partition)
        for dataset, table, _schema, partition in adapter.calls
    ] == [
        ("bahtflow_analytics", "transactions_accepted", "batch_date"),
        ("bahtflow_ops", "transactions_quarantine", "batch_date"),
    ]
