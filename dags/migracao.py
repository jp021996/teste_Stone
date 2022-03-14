from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import re


import logging

import datetime

BIGQUERYCONN = 'Big_Query_Conn'
POSTGRESQLCONN = 'PostgreSQL_Conn'

def create_table(pg_hook):
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs: 
            query = """
                CREATE table IF NOT EXISTS postgres.public.tokens (
                    contract_id serial PRIMARY KEY,
                    address VARCHAR (255) NOT NULL,
                    symbol VARCHAR (50),
                    name VARCHAR (50),
                    decimals FLOAT,
                    total_supply FLOAT,
                    block_timestamp TIMESTAMP NOT NULL,
                    block_number INT NOT NULL,
                    block_hash VARCHAR (255) NOT NULL
                )
            """
            logging.info('Creating table if it doesnt exist')
            curs.execute(query)
            conn.commit()
            # query = """
            #         SELECT *
            #     FROM pg_catalog.pg_tables
            #     WHERE schemaname != 'pg_catalog' AND 
            #         schemaname != 'information_schema';
            # """
            # curs.execute(query)
            # source = curs.fetchall()
            # logging.info(source)

def insert_data(pg_hook, row):
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs:
            logging.info('Inserting row of data in postgresql')
            data = [item if item != None else 'NULL' for item in row]
            index = [0,1,2,5,7]
            for i in index:
                data[i] = data[i].replace("'", "''")
            # pattern = r'\((.*?)\)'
            # data[5] = re.match(pattern, data[5])
            data[5] = data[5].isoformat()
            query = f"""
                INSERT INTO postgres.public.tokens 
                (address, symbol, name, decimals, total_supply, block_timestamp, block_number, block_hash)
                VALUES ('{data[0]}', '{data[1]}', '{data[2]}', {data[3]}, {data[4]}, '{data[5]}', {data[6]}, '{data[7]}')
            """
            logging.info(query)
            curs.execute(query)
            # query = """
            #         SELECT *
            #     FROM pg_catalog.pg_tables
            #     WHERE schemaname != 'pg_catalog' AND 
            #         schemaname != 'information_schema';
            # """
            # curs.execute(query)
            conn.commit()

def migrate_data_function(date):
    pg_hook = PostgresHook(POSTGRESQLCONN)
    
    create_table(pg_hook)
    bq_hook = BigQueryHook(gcp_conn_id=BIGQUERYCONN)
    client = bq_hook.get_client()
    logging.info(f'Data: {date}')
    QUERY = f"""
        SELECT * FROM `bigquery-public-data.crypto_ethereum.tokens` WHERE CAST( block_timestamp as DATE) = '{date}'
    """
    logging.info(f'Getting data from BigQuery')
    data = client.query(QUERY)
    for row in data:
        logging.info(row) 
        insert_data(pg_hook, row)
    



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


