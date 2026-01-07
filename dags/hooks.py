from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG(
    dag_id='hooks',
    description='Dag exemplos de Hooks',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
)


def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('CREATE TABLE IF NOT EXISTS teste_hooks (id int);', autocommit=True)
    
def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('INSERT INTO teste_hooks (id) VALUES (1);', autocommit=True)
    
def query_data(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    records = pg_hook.get_records('SELECT * FROM teste_hooks;')
    context['ti'].xcom_push(key='query_result', value=records)
    return records

def print_data(**context):
    ti = context['ti']
    records = ti.xcom_pull(key='query_result', task_ids='query_data_task')
    print("Query Result:")
    if not records:
        print("No results returned from query_data")
        return
    for row in records:
        print(row)
        
create_table_task = PythonOperator(
    task_id='create_table_task',    
    python_callable=create_table,
    dag=dag)

insert_data_task = PythonOperator(
    task_id='insert_data_task', 
    python_callable=insert_data,
    dag=dag)

query_data_task = PythonOperator(
    task_id='query_data_task',  
    python_callable=query_data,
    provide_context=True, # to access context
    dag=dag)

print_data_task = PythonOperator(
    task_id='print_data_task',
    python_callable=print_data,
    provide_context=True,
    dag=dag)

create_table_task >> insert_data_task >> query_data_task >> print_data_task
    