from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta      
import pandas as pd

dag = DAG(
    'producer', # nome da DAG sem caracteres especiais ou espaços
    description='Dag producer', # descrição da DAG
    schedule_interval=None, # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

mydataset = Dataset('/opt/airflow/data/Churn_new.csv')

def my_file():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new.csv', sep=';', index=False)
    print('Arquivo gerado com sucesso!')
    
    
# outlets é uma lista de datasets que essa task atualiza ou gera
# Dessa forma, qualquer DAG que tenha esse dataset como entrada será acionada
t1 = PythonOperator(
    task_id='task1', python_callable=my_file, dag=dag, outlets=[mydataset]
)