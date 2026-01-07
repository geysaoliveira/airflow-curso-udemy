from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta      
from airflow.models import Variable


dag = DAG(
    'variaveis_airflow', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Variaveis Airflow', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

def print_variable(**context):
    minha_variavel = Variable.get("minhavar", default_var="Valor padrão se a variável não existir")
    print(f'O valor da variável é: {minha_variavel}')

task1 = PythonOperator(task_id='tsk1', python_callable=print_variable, dag=dag) # exit 1 = falha

task1 
