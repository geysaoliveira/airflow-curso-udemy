from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    'dummy', # nome da DAG sem caracteres especiais ou espaços
    description='Dag dummy', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)


task1 = BashOperator(task_id='tsk1', bash_command='sleep 1', dag=dag) # exit 1 = falha
task2 = BashOperator(task_id='tsk2', bash_command='sleep 1', dag=dag) # exit 1 = falha
task3 = BashOperator(task_id='tsk3', bash_command='sleep 1', dag=dag) # exit 1 = falha
task4 = BashOperator(task_id='tsk4', bash_command='sleep 1', dag=dag) # exit 1 = falha
task5 = BashOperator(task_id='tsk5', bash_command='sleep 1', dag=dag) # exit 1 = falha


# O airflow não suporta setas entre mais de duas tasks (entre listas)
# [task1,task2,task3] >> [task4,task5]

# Para resolver isso, pode se incluir uma task dummy (vazia) no meio
# ou usar o bitshift (>> ou <<) várias vezes

task_dummy = DummyOperator(task_id='taskdummy', dag=dag)

[task1,task2,task3] >> task_dummy
task_dummy >> [task4,task5]