from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


import logging

import datetime

BIGQUERYCONN = 'Big_Query_Conn'
POSTGRESQLCONN = 'PostgreSQL_Conn'

def create_table(pg_hook:object) -> None:
    '''Create table if it not exists

    Args:
        pg_hook (object): hook of the database
    '''
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

def insert_data(pg_hook:object, row:object) -> None:
    '''Insert data in database

    Args:
        pg_hook (object): hook of the database
        row (object): row of data
    '''
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs:
            logging.info('Inserting row of data in database')
            data = [item if item != None else 'NULL' for item in row]
            #Scaping aspostrophe, because some names and symbols can have it
            index = [1,2]
            for i in index:
                data[i] = str(data[i]).replace("'", "''")
            data[5] = data[5].isoformat()
            query = f"""
                INSERT INTO postgres.public.tokens 
                (address, symbol, name, decimals, total_supply, block_timestamp, block_number, block_hash)
                VALUES ('{data[0]}', '{data[1]}', '{data[2]}', {data[3]}, {data[4]}, '{data[5]}', {data[6]}, '{data[7]}')
            """
            logging.info(query)
            curs.execute(query)
            conn.commit()

def migrate_data_function(date:str) -> None:
    '''Main function that get the data from BigQuery and insert it into the database.

    Args:
        date (str): _description_
    '''
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
    #8 days in the past to make the backfill for 8 days
    start_date=datetime.datetime.now() - datetime.timedelta(days=9),
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


