import requests
import io
import logging
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine, text

from airflow.hooks.postgres_hook import PostgresHook
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
    
    response.raise_for_status()
    
    xml_data = response.text 

    df = pd.read_xml(io.StringIO(xml_data), parser='etree').query('Name == "Доллар США"')['Value']
    
    data = float(df.values[0].replace(',', '.'))
    
    # Передаём данные дальше через XCom
    kwargs['ti'].xcom_push(key='rate', value=data)
    # return data

# Функция для записи в БД
def save_data_to_db(**kwargs):
    ti = kwargs['ti']
    # Получаем данные их XCom
    data = ti.xcom_pull(task_ids='fetch_data_task', key='rate')

    # Получаем execution_date из контекста
    execution_date = kwargs['execution_date']

    logger.info(f"Fetching value USD: {data}")

    # Получаем connection из Airflow по его conn_id
    hook = PostgresHook(postgres_conn_id='pg_database')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 1 FROM economic.usd_to_rub_rates WHERE date = %s
    """, (execution_date.date(),))

    if cursor.fetchone():
        logger.warning("Rate already exists for date %s", execution_date.date())
    else:
        cursor.execute("""
            INSERT INTO economic.usd_to_rub_rates (date, value)
            VALUES (%s, %s)
        """, (execution_date.date(), data))

    logger.info("Saving exchange rate %.2f for date %s", data, execution_date.date())

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2000, 1, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(seconds=10),
}

with DAG(
    dag_id='usd_rate_fetcher',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=True
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_api,
    )

    save_task = PythonOperator(
        task_id='save_data_task',
        python_callable=save_data_to_db,
    )

fetch_task >> save_task



# import xml.etree.ElementTree as ET

# root = ET.fromstring(xml_data)
# for valute in root.findall('Valute'):
#     name = valute.find('Name').text
#     if name == 'Доллар США':
#         value = float(valute.find('Value').text.replace(',', '.'))
#         return value