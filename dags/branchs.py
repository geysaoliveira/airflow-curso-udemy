from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta      
import random


dag = DAG(
    'branchtest', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Branch Test', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

def gera_numero_aleatorio(**context):
    numero = random.randint(1, 100)
    print(f'Número gerado: {numero}')
    return numero

gera_numero_aleatorio_task = PythonOperator(
    task_id='gera_numero_aleatorio_task',
    python_callable=gera_numero_aleatorio,
    dag=dag
)

def avalia_numero_aleatorio(**context):
    ti = context['ti']
    numero = ti.xcom_pull(task_ids='gera_numero_aleatorio_task') # o push é feito implicitamente na task gera_numero_aleatorio_task
    if numero % 2 == 0:
        return 'task_par'
    else:
        return 'task_impar'


branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=avalia_numero_aleatorio,
    provide_context=True, # necessário para passar o contexto para a função
    dag=dag
)


task_par = BashOperator(task_id='task_par', bash_command='echo "Número par!"', dag=dag)
task_impar = BashOperator(task_id='task_impar', bash_command='echo "Número ímpar!"', dag=dag)

gera_numero_aleatorio_task >> branch_task 
branch_task >> task_par
branch_task >> task_impar