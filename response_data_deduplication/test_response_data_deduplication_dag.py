from jobboards_table_update_udag import dag, TASK_ID


def test_dag() -> None:
    assert dag is not None
    assert dag.dag_id == TASK_ID
    assert dag.has_task(TASK_ID)
    assert len(dag.tasks) == 1
