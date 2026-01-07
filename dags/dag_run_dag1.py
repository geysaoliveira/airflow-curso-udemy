from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      
from airflow.operators.dagrun_operator import TriggerDagRunOperator

dag = DAG(
    'dag_run_da1', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Run Dag 1', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executadas em paralelo
task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag) # exit 1 = falha
task2 = TriggerDagRunOperator(task_id='tsk2', trigger_dag_id="dag_run_da2", dag=dag)


task1 >> task2
