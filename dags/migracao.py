from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook


import logging

import datetime

BIGQUERYCONN = 'Big_Query_Conn'
POSTGRESQLCONN = 'PostgreSQL'

def migrate_data_function(date):
    pg_hook = PostgresHook(postgres_conn_id =POSTGRESQLCONN)
    cursor = pg_hook.get_conn().cursor()
    query = """
        CREATE table IF NOT EXISTS postgres.public.tokens
        id serial PRIMARY KEY,
        address VARCHAR (255) NOT NULL,
        symbol VARCHAR (50),
        name VARCHAR (50),
        decimals FLOAT,
        total_supply FLOAT,
        block_timestamp TIMESTAMP NOT NULL,
        block_number INT NOT NULL,
        block_hash VARCHAR (255) NOT NULL
    """
    cursor.excute(query)
    source = cursor.fetchall()
    # bq_hook = BigQueryHook(gcp_conn_id=BIGQUERYCONN)
    # client = bq_hook.get_client()
    # logging.info(f'Data: {date}')
    # QUERY = f"""
    #     SELECT * FROM `bigquery-public-data.crypto_ethereum.tokens` WHERE CAST( block_timestamp as DATE) = '{date}'
    # """
    # data = client.query(QUERY)
    # for row in data:
    #     logging.info(row)   
    



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