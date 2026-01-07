from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      
from airflow.utils.task_group import TaskGroup ######<<<<<<<

dag = DAG(
    'dag_group', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Group', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executadas em paralelo
task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag) # exit 1 = falha
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 5', dag=dag)
task4 = BashOperator(task_id='tsk4', bash_command='sleep 5', dag=dag)
task5 = BashOperator(task_id='tsk5', bash_command='sleep 5', dag=dag)
task6 = BashOperator(task_id='tsk6', bash_command='sleep 5', dag=dag)

tsk_group = TaskGroup(group_id='tsk_group', dag=dag)  ######<<<<<<<

task7 = BashOperator(task_id='tsk7', bash_command='sleep 5', dag=dag, task_group=tsk_group)  ######<<<<<<<
task8 = BashOperator(task_id='tsk8', bash_command='sleep 5', dag=dag, task_group=tsk_group)  ######<<<<<<<

# task executada após as duas anteriores
task9 = BashOperator(task_id='tsk9', bash_command='sleep 5', dag=dag,
                     trigger_rule ='all_failed', #regra: só executa se todas as anteriores falharem
                     task_group=tsk_group)  ######<<<<<<<

task1 >> task2
task3 >> task4
[task2,task4] >> task5 >> task6
task6 >> tsk_group  ######<<<<<<< 