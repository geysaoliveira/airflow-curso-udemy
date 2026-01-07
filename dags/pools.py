from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta      


dag = DAG(
    'pool', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Pool Airflow', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executadas em paralelo
# task1 = BashOperator(task_id='tsk1', bash_command='sleep 1', dag=dag)
# task2 = BashOperator(task_id='tsk2', bash_command='sleep 1', dag=dag)
# task3 = BashOperator(task_id='tsk3', bash_command='sleep 1', dag=dag)
# task4 = BashOperator(task_id='tsk4', bash_command='sleep 1', dag=dag)

# As tasks abaixo foram configuradas para usar o pool 'meupool', que limita a 2 tasks simultâneas
task1 = BashOperator(task_id='tsk1', bash_command='sleep 1', dag=dag, pool='meupool')
task2 = BashOperator(task_id='tsk2', bash_command='sleep 1', dag=dag, pool='meupool', priority_weight=5)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 1', dag=dag, pool='meupool')
task4 = BashOperator(task_id='tsk4', bash_command='sleep 1', dag=dag, pool='meupool', priority_weight=10)
