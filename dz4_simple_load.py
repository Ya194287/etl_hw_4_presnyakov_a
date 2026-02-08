from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Полная загрузка
def load_all():
    print("Загрузка данных")
    
    # Читаем файл из ДЗ-3
    df = pd.read_csv('/tmp/iot_cleaned.csv')
    
    # Создаем папку для результатов
    os.makedirs('/tmp/full_load', exist_ok=True)
    
    # Сохраняем все данные
    df.to_csv('/tmp/full_load/all_data.csv', index=False)
    
    print(f"Загружено {len(df)} строк")
    print("Файл: /tmp/full_load/all_data.csv")

# Инкрементальная загрузка (только новые данные)
def load_new():
    print("Загружаем данные")
    
    # Читаем файл из ДЗ-3
    df = pd.read_csv('/tmp/iot_cleaned.csv')
    
    # Берем только часть данных (например, за декабрь 2018)
    # В данных есть даты 2018 года, берем последний месяц
    df['date'] = pd.to_datetime(df['date_str'])
    new_data = df[df['date'] >= '2018-12-01']
    
    # Создаем папку для новых данных
    os.makedirs('/tmp/new_load', exist_ok=True)
    
    # Сохраняем только новые данные
    new_data.to_csv('/tmp/new_load/recent_data.csv', index=False)
    
    print(f"Загружено {len(new_data)} новых строк")
    print("Файл: /tmp/new_load/recent_data.csv")

# Создание DAG:

# DAG 1 - Полная загрузка
dag_full = DAG(
    'dz4_full_load',
    start_date=datetime(2025,1,1),
    schedule='@monthly'
)

# DAG 2 - инкрементальная загрузка
dag_inc = DAG(
    'dz4_incremental_load', 
    start_date=datetime(2025,1,1),
    schedule='0 2 * * *'  
)

# Создание задач
task_full = PythonOperator(
    task_id='load_all_data',
    python_callable=load_all,
    dag=dag_full
)

task_inc = PythonOperator(
    task_id='load_new_data',
    python_callable=load_new,
    dag=dag_inc
)
