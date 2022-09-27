import logging
import csv

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
  "owner": "kwan"
}

DATA_FOLDER = "/opt/airflow/dags"


def _query_db():
    pg_hook = PostgresHook(
        postgres_conn_id="breakfast_db_conn",
        schema="breakfast"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        SELECT * FROM product
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    with open(f"{DATA_FOLDER}/product-lookup-table.csv", "w") as f:
        writer = csv.writer(f)
        columns = [(
            "UPC",
            "DESCRIPTION",
            "MANUFACTURER",
            "CATEGORY",
            "SUB_CATEGORY",
            "PRODUCT_SIZE",
        )]
        writer.writerows(columns)
        writer.writerows(results)

    logging.info("Extracted the data successfully")

with DAG("product_lookup_pipeline",
  default_args=default_args,
  start_date=timezone.datetime(2020,1,1),
  schedule_interval='@daily',
  catchup=False
) as dag:
  start = EmptyOperator(task_id="start")
  end = EmptyOperator(task_id="end")
  query_db = PythonOperator(task_id="query_db", python_callable=_query_db)

  start >> query_db >> end