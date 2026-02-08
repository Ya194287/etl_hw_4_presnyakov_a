from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import kagglehub

def transform_iot_data():
    # Скачиваем датасет
    print("Скачиваем датасет")
    path = kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")
    
    # Ищем CSV файл
    import os
    for file in os.listdir(path):
        if file.endswith('.csv'):
            csv_path = os.path.join(path, file)
            break
    
    # Загружаем данные
    df = pd.read_csv(csv_path)
    print(f"Загружено строк: {len(df)}")
    
    # Оставляем только In (внутренняя температура)
    df = df[df['out/in'] == 'In'].copy()
    print(f"После фильтрации 'In': {len(df)} строк")
    
    # Преобразуем дату
    df['date'] = pd.to_datetime(df['noted_date'], dayfirst=True).dt.date
    df['date_str'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Очищаем температуру по процентилям
    lower = df['temp'].quantile(0.05)
    upper = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= lower) & (df['temp'] <= upper)].copy()
    print(f"После очистки по процентилям: {len(df)} строк")
    
    # Находим самые жаркие и холодные дни
    daily_avg = df.groupby('date_str')['temp'].mean().reset_index()
    daily_avg.columns = ['date', 'avg_temp']
    
    hottest = daily_avg.sort_values('avg_temp', ascending=False).head(5)
    coldest = daily_avg.sort_values('avg_temp', ascending=True).head(5)
    
    print("\n5 самых жарких дней:")
    print(hottest)
    print("\n5 самых холодных дней:")
    print(coldest)
    
    # Сохраняем результаты
    df.to_csv('/tmp/iot_cleaned.csv', index=False)
    hottest.to_csv('/tmp/hottest_days.csv', index=False)
    coldest.to_csv('/tmp/coldest_days.csv', index=False)
    
    print("\nСохранено:")
    print("- /tmp/iot_cleaned.csv")
    print("- /tmp/hottest_days.csv")
    print("- /tmp/coldest_days.csv")

# Создаем DAG
dag = DAG(
    'dz3_iot_transform',
    start_date=datetime(2025, 1, 1),
    schedule=None
)

# Задача
task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_iot_data,
    dag=dag
)
