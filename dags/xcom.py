from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta      


dag = DAG(
    'exemplo_xcom', # nome da DAG sem caracteres especiais ou espaços
    description='Dag xcom', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valorxcom1', value=10200)

# Task executadas em paralelo
task1 = PythonOperator(task_id='tsk1', python_callable=task_write, dag=dag) # exit 1 = falha


def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(key='valorxcom1')
    print(f'O valor lido do XCom é: {valor}')
    
task2 = PythonOperator(task_id='tsk2', python_callable=task_read, dag=dag) # exit 1 = falha

task1 >> task2
