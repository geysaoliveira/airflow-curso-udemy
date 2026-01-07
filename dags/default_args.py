from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'depend_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email' : ['geysa.oliviera.marinho@gmail.com','geysa3@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'defaultargs', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Default Args', # descrição da DAG
    default_args=default_args,
    schedule_interval='@hourly', # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False, # se a DAG deve executar para datas passadas ou não
    default_view='graph', # visualização padrão da DAG (graph, tree, duration, gantt, landing_times
    tags=['porcesso','pipeline'] # tags para facilitar a busca da DAG
)

# Task executadas em paralelo
task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag, retries=3) # exit 1 = falha
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag) # exit 1 = falha
task3 = BashOperator(task_id='tsk3', bash_command='sleep 5', dag=dag) # exit 1 = falha



task1 >> task2 >> task3
