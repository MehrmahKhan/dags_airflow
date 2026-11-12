"""Basic check that the DAG file loads and has the three expected tasks."""

from airflow.models import DagBag


def test_dag_loads_without_errors():
    dag_bag = DagBag(include_examples=False)
    assert dag_bag.import_errors == {}


def test_dag_has_three_tasks():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag(dag_id="currency_rates_etl")

    assert dag is not None
    assert len(dag.tasks) == 3

    task_ids = []
    for task in dag.tasks:
        task_ids.append(task.task_id)

    assert "extract_currency_rates" in task_ids
    assert "transform_exchange_rates" in task_ids
    assert "load_rates_into_mongodb" in task_ids
