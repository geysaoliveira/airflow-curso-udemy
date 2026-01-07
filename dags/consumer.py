from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta      
import pandas as pd
import statistics as sts

mydataset = Dataset('/opt/airflow/data/Churn_new.csv')

dag = DAG(
    'consumer', # nome da DAG sem caracteres especiais ou espaços
    description='Dag consumer', # descrição da DAG
    schedule=[mydataset], # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

def my_file():
    dataset = pd.read_csv('/opt/airflow/data/Churn_new.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new2.csv', sep=';')
    print('Arquivo Churn_new2 gerado com sucesso! Apenas para ter certeza que funcionou')
    
    
# provide_context=True é necessário para passar o contexto para a função
t1 = PythonOperator(
    task_id='task1', python_callable=my_file, dag=dag, provide_context=True
)