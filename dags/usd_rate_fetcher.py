import requests
import io
import logging
import datetime as dt
import pandas as pd

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

# Логгер
logger = logging.getLogger(__name__)

# Определение DAG с помощью декоратора @dag
@dag(
    dag_id='usd_rate_fetcher',
    schedule_interval='@daily',
    start_date=dt.datetime(2000, 1, 1),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': dt.timedelta(seconds=10),
    },
    tags=['exchange_rates'],
)
def usd_rate_dag():

    @task()
    def fetch_data_from_api(execution_date: str):

        execution_date = dt.datetime.fromisoformat(execution_date)

        date_str = execution_date.strftime("%d/%m/%Y")
        logger.info(f"Fetching data for date: {date_str}")

        url = f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
        response = requests.get(url)
        response.raise_for_status()

        xml_data = response.text
        df = pd.read_xml(io.StringIO(xml_data), parser='etree').query('Name == "Доллар США"')['Value']
        data = float(df.values[0].replace(',', '.'))

        return data
    
    @task()
    def save_data_to_db(data, execution_date: str):

        execution_date = dt.datetime.fromisoformat(execution_date)

        logger.info(f"Received USD rate: {data} for date {execution_date.date()}")

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

        conn.commit()

    # Установка зависимостей
    fetched_data = fetch_data_from_api(execution_date="{{ execution_date }}")
    save_data_to_db(fetched_data, execution_date="{{ execution_date }}")


# Регистрация DAG
usd_rate_dag = usd_rate_dag()