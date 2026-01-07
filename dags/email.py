from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta      
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'depend_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email' : ['geysa.oliveira.marinho@gmail.com','geysa3@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'email', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Default Args', # descrição da DAG
    default_args=default_args,
    schedule_interval=None, # intervalo de execução da DAG
    #start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False, # se a DAG deve executar para datas passadas ou não
    default_view='graph', # visualização padrão da DAG (graph, tree, duration, gantt, landing_times
    tags=['email','pipeline'] # tags para facilitar a busca da DAG
)

# Task executadas em paralelo
task1 = BashOperator(task_id='tsk1', bash_command='sleep 1', dag=dag) # exit 1 = falha
task2 = BashOperator(task_id='tsk2', bash_command='sleep 1', dag=dag) # exit 1 = falha
task3 = BashOperator(task_id='tsk3', bash_command='sleep 1', dag=dag) # exit 1 = falha
task4 = BashOperator(task_id='tsk4', bash_command='exit 1', dag=dag) # exit 1 = falha
task5 = BashOperator(task_id='tsk5', bash_command='sleep 1', dag=dag, trigger_rule='none_failed') # exit 1 = falha
task6 = BashOperator(task_id='tsk6', bash_command='sleep 1', dag=dag, trigger_rule='none_failed') # exit 1 = falha

# send_mail = EmailOperator(task_id='send_email', 
#                           to='geysa.oliveira.marinho@gmail.com',
#                             subject='Airflow DAG {{ dag.dag_id }} - Task {{ task.task_id }}: {{ task.state }}',
#                             html_content="""<h3>Detalhes da Execução da DAG</h3>
#                             <ul>"""
#                             """<li>DAG: {{ dag.dag_id }}</li>"""
#                             """<li>Task: {{ task.task_id }}</li>"""
#                             """<li>Data de Execução: {{ ts }}</li>"""
#                             """<li>Status: {{ task.state }}</li>"""
#                             """</ul>""",
#                             dag=dag,
#                             trigger_rule='one_failed'  # Envia email se qualquer tarefa falhar
#                           )

send_mail = EmailOperator(
    task_id='send_email', 
    to='geysa.oliveira.marinho@gmail.com',
    subject='Airflow DAG {{ dag.dag_id }} - Task {{ task.task_id }}: {{ ti.state }}',
    html_content="""
        <h3>Detalhes da Execução da DAG</h3>
        <ul>
            <li>DAG: {{ dag.dag_id }}</li>
            <li>Task: {{ task.task_id }}</li>
            <li>Data de Execução: {{ ts }}</li>
            <li>Status: {{ ti.state }}</li>
        </ul>
    """,
    dag=dag,
    trigger_rule='one_failed'  # Envia email se qualquer tarefa falhar
)
                            
[task1,task2] >> task3 >> task4
task4 >> [task5,task6,send_mail]
