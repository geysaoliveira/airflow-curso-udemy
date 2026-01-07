from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta      
import pandas as pd
import statistics as sts
# import openpyxl
# from openpyxl.styles import Font, Alignment



dag = DAG(
    'pythonoperator', # nome da DAG sem caracteres especiais ou espaços
    description='Dag Python Operator - tratar dados', # descrição da DAG
    schedule_interval=None, # intervalo de execução da DAG
    start_date=datetime(2024, 6, 1), # data de início da DAG
    catchup=False # se a DAG deve executar para datas passadas ou não
)

# def csv_to_xlsx_format(**context):
#     # Lê o CSV limpo
#     df = pd.read_csv('/opt/airflow/data/churn_clear.csv', sep=';')
#     xlsx_path = '/opt/airflow/data/churn_clear.xlsx'
#     df.to_excel(xlsx_path, index=False)

#     # Abre o arquivo XLSX para aplicar formatação
#     wb = openpyxl.load_workbook(xlsx_path)
#     ws = wb.active

#     # Formatação: cabeçalho em negrito e centralizado
#     for cell in ws[1]:
#         cell.font = Font(bold=True)
#         cell.alignment = Alignment(horizontal='center')

#     # Ajusta largura das colunas
#     for column_cells in ws.columns:
#         length = max(len(str(cell.value)) for cell in column_cells)
#         ws.column_dimensions[column_cells[0].column_letter].width = length + 2

    # wb.save(xlsx_path)
    
def data_cleaner(**context):
    # Lê o arquivo CSV
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=";")
    
    dataset.columns = ["Id", "Score", "Estado", "Genero", "Idade", "Patrimonio", 
                       "Saldo", "Produtos", "TemCartCredito", "Ativo", "Salario", "Saiu"] #Saiu: deixou ou não a empresa
    
    # Trata valores ausentes na coluna 'Salario' e 'Genero'
    mediana_salario = sts.median(dataset['Salario'])
    dataset['Salario'].fillna(mediana_salario, inplace=True)
    dataset['Genero'].fillna('Masculino', inplace=True)
    
    # Trata valores inválidos na coluna 'Idade'
    mediana_idade = sts.median(dataset['Idade'])
    dataset.loc[(dataset['Idade'] < 0) | (dataset['Idade'] > 120), 'Idade'] = mediana_idade
    dataset['Idade'].fillna(mediana_idade, inplace=True)
    
    # Remove linhas duplicadas com base na coluna 'Id', mantendo a primeira ocorrência
    dataset.drop_duplicates(subset="Id", keep="first", inplace=True)
    
    # Remove linhas com valores ausentes
    df_cleaned = dataset.dropna()
    
    dataset.to_csv('/opt/airflow/data/churn_clear.csv', sep=";", index=False)
    
    
    # Converte a coluna 'temperatura' para numérico, forçando erros a NaN
    # df_cleaned['temperatura'] = pd.to_numeric(df_cleaned['temperatura'], errors='coerce')
    
    
    # Salva o DataFrame limpo em um novo arquivo CSV
    # df_cleaned.to_csv('/opt/airflow/data/temperaturas_limpo.csv', index=False)
    
    # print("Dados limpos e salvos em 'temperaturas_limpo.csv'")
    
t1 = PythonOperator(
    task_id='t1', python_callable=data_cleaner, dag=dag
)

# tentar depois, não funcionou - não importa a biblioteca openpyxl
# t2 = PythonOperator(
#     task_id='convert_xlsx', python_callable=csv_to_xlsx_format, dag=dag
# )

#t2