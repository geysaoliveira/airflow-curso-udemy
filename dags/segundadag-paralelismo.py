from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      

dag = DAG(
    'segundad_ag', # nome da DAG sem caracteres especiais ou espaços
    description='Dag com paralelismo', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executada antes das outras duas
task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag)

# Task executadas em paralelo
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 5', dag=dag)

task1 >> [task2,task3]