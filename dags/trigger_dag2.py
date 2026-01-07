from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      

dag = DAG(
    'trigger_dag2', # nome da DAG sem caracteres especiais ou espaços
    description='Trigger', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executadas em paralelo
task1 = BashOperator(task_id='tsk1', bash_command='exit 1', dag=dag) # exit 1 = falha
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag)

# task executada após as duas anteriores
task3 = BashOperator(task_id='tsk3', bash_command='sleep 5', dag=dag,
                     trigger_rule ='one_failed') #regra: ao menos uma das anteriores falhar
# Se as dags 1 e 2 não falharem, a dag3 não será executada
# Se uma das dags 1 ou 2 falharem, a dag3 será executada

[task1,task2] >> task3