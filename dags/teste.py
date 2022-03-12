from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
import datetime

with DAG('my_dag', start_date=datetime(2016, 1, 1)) as dag:
    task_1 = DummyOperator('task_1')
    task_2 = DummyOperator('task_2')
    task_1 >> task_2 # Define dependencies