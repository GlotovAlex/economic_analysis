import yfinance as yf
import logging
import datetime as dt
import pandas as pd
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

# Логгер
logger = logging.getLogger(__name__)

# Определение DAG с помощью декоратора @dag
@dag(
    dag_id='gold_value',
    schedule_interval='@daily',
    start_date=dt.datetime(2000, 8, 31),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': dt.timedelta(seconds=10),
    },
    tags=['commodities'],
)

def gold_value_dag():

    @task()
    def fetch_data_from_api(execution_date: str):
        """
        Получает данные по стоимости золота на момент закрытия в долларах
        """

        from_date = dt.datetime.fromisoformat(execution_date) + relativedelta(days=-1)
        to_date = dt.datetime.fromisoformat(execution_date)

        # Дата выполнения
        execution_date = dt.datetime.fromisoformat(execution_date)
        
        logger.info(f'Получение данных за даты с {from_date} по {to_date}')

        data = yf.download("GC=F", start=from_date.strftime("%Y-%m-%d"), end=to_date.strftime("%Y-%m-%d"))

        if len(data) == 1:
            values = (str(data.index[0].date()), round(data['Close'].values[0][0], 4))

            logger.info(f'Стоимость золота на дату {values[0]} ${values[1]}')

            return values

        else:         
            logger.warning(f'На дату {from_date} запрос вернул {len(data)} строк')

            return None
    
    @task()
    def save_data_to_db(values):
        """
        Сохраняет данные в БД
        :param data: кортеж вида (date:str, value:float)
        """

        # Проверка на наличие полученных данных
        if not values:
            logger.info('Нет данных для записи в БД')
            return 

        date, value = values

        hook = PostgresHook(postgres_conn_id='pg_database')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Проверка существования записи
        cursor.execute("""
            SELECT 1 FROM economic.gold_cost 
            WHERE date = %s
        """, (date,))

        if cursor.fetchone():
            logger.warning(f"Данные на дату {date} уже содержатся в БД")
        else:
            cursor.execute("""
                INSERT INTO economic.gold_cost (date, value)
                VALUES (%s, %s)
            """, (date, value))
            logger.info("Записано значение на дату %s", date)

        conn.commit()

    # Установка зависимостей
    fetched_data = fetch_data_from_api(execution_date="{{ execution_date }}")
    save_data_to_db(fetched_data)

# Регистрация DAG
rate_dag = gold_value_dag()