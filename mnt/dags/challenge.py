from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

default_args = {
  "owner": "kwan"
}

def _say_hello():
  print('hello')

def _print_log_messages():
  logging.debug('this is a debug message')
  logging.info('this is an info message')
  logging.warning('this is a warning message')
  logging.error('this is an error message')
  logging.critical('this is a critical message')

with DAG(
  "challenge",
  default_args=default_args,
  start_date=timezone.datetime(2022,9,27),
  schedule_interval='*/30 * * * *'
) as dag:
  start = EmptyOperator(task_id="start")
  echo_hello = BashOperator(task_id="echo_hello", bash_command='echo hello')
  say_hello = PythonOperator(task_id="say_hello", python_callable=_say_hello)
  print_log_messages = PythonOperator(task_id="print_log_messages", python_callable=_print_log_messages)
  end = EmptyOperator(task_id="end")
  
  start >> echo_hello >> say_hello >> print_log_messages >> end
