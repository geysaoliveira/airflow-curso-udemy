from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x
from airflow.providers.http.sensors.http import HttpSensor
import requests

dag = DAG(
    dag_id='httpsensor',
    description='Dag HTTP Sensor',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
)

def query_api():
    url = "https://rickandmortyapi.com/api/character"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    characters = data.get("results", [])
    print(f"Fetched {len(characters)} characters from the API.")
    # O valor retornado vai para XCom automaticamente
    return {"characters_count": len(characters)}

check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='rickandmorty_api',    # Connection HTTP criada na UI
    endpoint='/api/character',          # comece com '/'
    method='GET',
    # se quiser, pode enviar headers aqui ou na Connection (Extra)
    headers={'Accept': 'application/json', 'User-Agent': 'airflow'},
    poke_interval=5,                    # checa a cada 5s
    timeout=60,                         # tempo total do sensor antes de falhar
    response_check=lambda r: r.status_code == 200 and "results" in r.json(),
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=query_api,
    dag=dag,
)

check_api >> process_data
