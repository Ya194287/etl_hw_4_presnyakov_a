# ДЗ-4: Загрузка данных в целевую систему

## Задание
Настроить два процесса загрузки для преобразованных данных:
1. Полная загрузка исторических данных
2. Инкрементальная загрузка изменений за последние дни

## Выполнение

### 1. Преобразование данных (ДЗ-3)
- DAG: `dz3_iot_transform`
- Файл: `dz_3_iot_kaggle_transform.py`
- Результат: `/tmp/iot_cleaned.csv`

### 2. Полная загрузка
- DAG: `dz4_full_load`
- Файл: `dz4_simple_load.py` (функция `load_all`)
- Результат: `/tmp/full_load/all_data.csv`
- Запуск: раз в мес

### 3. Инкрементальная загрузка
- DAG: `dz4_incremental_load`
- Файл: `dz4_simple_load.py` (функция `load_new`)
- Результат: `/tmp/new_load/recent_data.csv`
- Запуск: ежедневно в 2:00

## Структура файлов
- `dz4_simple_load.py` - основной код ДЗ-4
- `dz_3_iot_kaggle_transform.py` - код ДЗ-3 (для истории)
- `full_load_sample.csv` - пример полной загрузки
- `incremental_sample.csv` - пример инкрементальной загрузки

+ Скриншоты Airflow

