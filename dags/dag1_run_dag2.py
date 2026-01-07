from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      
from airflow.utils.task_group import TaskGroup 

dag = DAG(
    'dag_run_da2', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Run Dag 2', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executadas em paralelo
task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag) # exit 1 = falha
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag)


task1 >> task2
