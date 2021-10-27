"""
DAG operations:
- Add new partitions from previous day to the job boards table
- Run the aggregations (counts of job boards)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta


TASK_ID = '.udag_response_data_deduplication'

default_args = {
    'owner': 'JR',
    'depends_on_past': False,
    'start_date': '2021-10-25',
    'email': ['intelligence@profinda.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG(
    TASK_ID,
    default_args=default_args,
    catchup=True,
    schedule_interval='0 2 * * *',
    concurrency=1,
    max_active_runs=1,
)


def execute_response_data_deduplication() -> BashOperator:
    commands = [
        'bash /home/airflow/.circleci_scripts/pull_udag_image.sh response_data_deduplication_udag',
        'docker run -e "EXECUTION_DATE={{ execution_date }}" '
        '765330055732.dkr.ecr.eu-west-1.amazonaws.com/microdags:response_data_deduplication_udag python main.py '
    ]
    return BashOperator(task_id=TASK_ID, bash_command=' && '.join(commands), dag=dag)


execute_response_data_deduplication()


if __name__ == '__main__':
    dag.cli()
