from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests

dag = DAG(
    dag_id='bancodedados',
    description='Dag exemplos de banco de dados',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
)


def print_result(**context):
    ti = context['ti']
    task_instance = ti.xcom_pull(task_ids='query_data')  # type: ignore
    print("Query Result:")
    if not task_instance:
        print("No results returned from query_data")
        return
    for row in task_instance:
        print(row)


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',  # Connection Postgres criada na UI
    sql='CREATE TABLE IF NOT EXISTS teste (id int);',
    dag=dag)
    
    
insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres',  
    sql='INSERT INTO teste (id) VALUES (1);',
    dag=dag)

query_data = PostgresOperator(
    task_id='query_data',
    postgres_conn_id='postgres',  
    sql='SELECT * FROM teste;',
    do_xcom_push=True,
    dag=dag)


print_result_task = PythonOperator(
    task_id='print_result_task', 
    python_callable=print_result,
    dag=dag)


create_table >> insert_data >> query_data >> print_result_task