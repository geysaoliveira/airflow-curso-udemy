from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      

dag = DAG(
    'dag_complexa', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Complexa', # descrição da DAG
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
task7 = BashOperator(task_id='tsk7', bash_command='sleep 5', dag=dag)
task8 = BashOperator(task_id='tsk8', bash_command='sleep 5', dag=dag)

# task executada após as duas anteriores
task9 = BashOperator(task_id='tsk9', bash_command='sleep 5', dag=dag,
                     trigger_rule ='all_failed') #regra: só executa se todas as anteriores falharem

task1 >> task2
task3 >> task4
[task2,task4] >> task5 >> task6
task6 >> [task7,task8,task9]