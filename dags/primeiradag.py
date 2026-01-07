from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta      
# from airflow.utils.dates import days_ago
# from airflow.operators.python import PythonOperator
# import requests
# import json
# import pandas as pd
# import numpy as np
# import os
# import logging
# import psycopg2
# from sqlalchemy import create_engine
# from sqlalchemy.types import Integer, String, Date, Float
# from airflow.hooks.base import BaseHook
# from airflow.models import Variable
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.email import EmailOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.operators.subdag import SubDagOperator
# from airflow.utils.task_group import TaskGroup
# from airflow.utils.trigger_rule import TriggerRule
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook


dag = DAG(
    'primeiradag', # nome da DAG sem caracteres especiais ou espaços
    description='Minha primeira DAG', # descrição da DAG
    schedule_interval=timedelta(minutes=60), # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# Task executadas sequencialmente
task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 5', dag=dag)


task1 >> task2 >> task3