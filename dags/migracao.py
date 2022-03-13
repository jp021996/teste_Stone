from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
import logging

import datetime

BIGQUERYCONN = 'Big_Query_Conn'

def migrate_data_function(date):
    bq_hook = BigQueryHook(bigquery_conn_id=BIGQUERYCONN)
    conn = bq_hook.get_conn()
    cursor = conn.cursor()
    print(f'Data, {date}')
    QUERY = f"""
        SELECT * FROM "bigquery-public-data.crypto_ethereum.tokens" WHERE CAST( block_timestamp as DATE) = "{date}"
    """
    result = cursor.execute(QUERY)
    print(result)



with DAG(
    dag_id="tranfer-data-BigQuery-Postgresql",
    start_date=datetime.datetime.now() - datetime.timedelta(days=7),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    migrate_data = PythonOperator(
        task_id='Migrate-Data',
        python_callable=migrate_data_function,
        op_args= ['{{ds}}']
    )

    start >> migrate_data >> end