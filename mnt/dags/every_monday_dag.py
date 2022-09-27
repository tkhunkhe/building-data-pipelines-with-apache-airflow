from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

default_args = {
  "owner": "kwan"
}

# every monday
with DAG(
  "every_monday_dag",
  default_args=default_args,
  start_date=timezone.datetime(2022,9,27),
  schedule_interval='0 0 * * 1'
) as dag:
  t1 = EmptyOperator(task_id="t1")
  t2 = EmptyOperator(task_id="t2")
  t1 >> t2