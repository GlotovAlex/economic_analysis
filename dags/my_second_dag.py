import requests
import io
import logging
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine, text

from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Функция для получения данных по API
def fetch_data_from_api(**kwargs):
    # Получаем execution_date из контекста
    execution_date = kwargs['execution_date']
    
    # Формируем дату как строку, например 'YYYY-MM-DD'
    date_str = execution_date.strftime("%d/%m/%Y")

    logger.info(f"execution_date : {execution_date}")
    
    logger.info(f"Fetching data for date: {date_str}")
    
    # Пример URL API с параметром даты
    url = f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
    
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")
    
    xml_data = response.text  # или .text / .content — зависит от ответа 

    df = pd.read_xml(io.StringIO(xml_data), parser='etree').query('Name == "Доллар США"')['Value']
    
    data = float(df.values[0].replace(',', '.'))
    
    # Передаём данные дальше через XCom
    return data

# Функция для записи в БД
def save_data_to_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data_task')

    # Получаем execution_date из контекста
    execution_date = kwargs['execution_date']

    logger.info(f"Fetching value USD: {data}")

    # Получаем connection из Airflow по его conn_id
    connection = BaseHook.get_connection('pg_database')

    # Формируем URL подключения
    db_url = f"postgresql://{connection.login}:{connection.password}@" \
             f"{connection.host}:{connection.port}/{connection.schema}"

    engine = create_engine(db_url)
   
    with engine.connect() as conn:
        conn.execute(
            text(f"""INSERT INTO economic.usd_to_rub_rates (date, value)
                 VALUES ('{execution_date.date()}', {data})""")
        )
        # conn.commit()

    logger.info("Data saved successfully.")

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2000, 1, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(seconds=10),
}

with DAG(
    dag_id='second_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=True
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_api,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id='save_data_task',
        python_callable=save_data_to_db,
        provide_context=True,
    )

fetch_task >> save_task