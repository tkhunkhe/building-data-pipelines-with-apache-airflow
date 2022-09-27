from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

default_args = {
  "owner": "kwan"
}

# everyday
with DAG(
  "everyday_dag",
  default_args=default_args,
  start_date=timezone.datetime(2022,9,27),
  schedule_interval='@daily'
) as dag:
  t1 = EmptyOperator(task_id="t1")
  t2 = EmptyOperator(task_id="t2")
  t1 >> t2