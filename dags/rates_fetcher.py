import requests
import logging
import datetime as dt
import xml.etree.ElementTree as ET

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

# Логгер
logger = logging.getLogger(__name__)


# Определение DAG с помощью декоратора @dag
@dag(
    dag_id='rates_fetcher',
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

def rate_dag():

    @task()
    def fetch_data_from_api(execution_date: str):
        """
        Получает данные по указанным валютам за одну дату
        """

        logger.info(f'Запрос данных для валют {Variable.get("currencies")}')

        # Получение переменной
        try:
            currencies = Variable.get("currencies")
        except Exception as e:
            raise ValueError("Ошибка получения переменной 'currencies'") from e

        # Дата выполнения
        execution_date = dt.datetime.fromisoformat(execution_date)
        
        # Словарь для сбора данных
        data = {
            'date': execution_date.date(),
            'content': list()
        }

        # Получение данных через API за указанную дату
        date_str = execution_date.strftime("%d/%m/%Y")
        logger.info(f"Запрос данных осуществляется на дату: {date_str}")

        url = f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
        response = requests.get(url)
        response.raise_for_status()

        xml_data = response.text

        root = ET.fromstring(xml_data)
        for valute in root.findall('Valute'):
            CharCode = valute.find('CharCode').text
            if CharCode in currencies:
                value = float(valute.find('VunitRate').text.replace(',', '.'))
                data['content'].append({CharCode: value})

        return data
    
    @task()
    def save_data_to_db(data):
        """
        Сохраняет данные по нескольким валютам за одну дату в БД
        :param data: словарь с ключами 'date' и 'content'
        """

        execution_date = data['date']
        rates = data['content']

        logger.info(f"Получены курсы валют: {data['content']} на дату {execution_date}")

        hook = PostgresHook(postgres_conn_id='pg_database')
        conn = hook.get_conn()
        cursor = conn.cursor()

        for rate_dict in rates:
            
            currency, value = next(iter(rate_dict.items()))

            # Проверка существования записи
            cursor.execute("""
                SELECT 1 FROM economic.currency_rates 
                WHERE date = %s AND currency = %s
            """, (execution_date, currency))

            if cursor.fetchone():
                logger.warning("Курс по %s на дату %s уже содержится в БД", currency, execution_date)
            else:
                cursor.execute("""
                    INSERT INTO economic.currency_rates (date, currency, value)
                    VALUES (%s, %s, %s)
                """, (execution_date, currency, value))
                logger.info("Записано значение %.2f для %s на %s", value, currency, execution_date)

        conn.commit()

    # Установка зависимостей
    fetched_data = fetch_data_from_api(execution_date="{{ logical_date }}")
    save_data_to_db(fetched_data)


# Регистрация DAG
dag = rate_dag()