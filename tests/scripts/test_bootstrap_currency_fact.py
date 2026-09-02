import importlib


def _bootstrap_module():
    return importlib.import_module("scripts.bootstrap_currency_fact")


class RecordingAdapter:
    def __init__(self):
        self.calls = []

    def ensure_partitioned_table(self, dataset_id, table_id, schema, partition_field):
        self.calls.append((dataset_id, table_id, schema, partition_field))
        return "verified"


def test_bootstrap_currency_fact_targets_only_f06_fact_table():
    bootstrap = _bootstrap_module()
    adapter = RecordingAdapter()

    statuses = bootstrap.bootstrap_currency_fact(adapter)

    assert statuses == [("bahtflow_analytics.fct_transactions", "verified")]
    assert [
        (dataset, table, partition)
        for dataset, table, _schema, partition in adapter.calls
    ] == [("bahtflow_analytics", "fct_transactions", "batch_date")]
