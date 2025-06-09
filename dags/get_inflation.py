import requests
import logging
import datetime as dt
import pandas as pd
import xml.etree.ElementTree as ET
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

# Логгер
logger = logging.getLogger(__name__)


# Определение DAG с помощью декоратора @dag
@dag(
    dag_id='get_inflation_key_rate',
    schedule_interval='@monthly',
    start_date=dt.datetime(2013, 3, 1),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': dt.timedelta(seconds=10),
    },
    tags=['other'],
    max_active_runs=1,
)

def rate_dag():

    @task()
    def fetch_data_from_api(execution_date: str):
        """
        Получает данные по инфляции, целевой инфляции и ключевой ставке ЦБ РФ
        """

        from_date = dt.datetime.fromisoformat(execution_date) + relativedelta(months=-2)
        to_date = dt.datetime.fromisoformat(execution_date) + relativedelta(months=-1, days=-1)
        
        logger.info(f'Получение данных за даты с {from_date} по {to_date}')

        # Рабочий URL сервиса
        url = "http://www.cbr.ru/secinfo/secinfo.asmx"

        # Заголовки
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '"http://web.cbr.ru/Inflation"',
            "User-Agent": "Mozilla/5.0"  # Имитация браузера
            }
        
        body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Inflation xmlns="http://web.cbr.ru/">
      <DateFrom>{from_date.strftime('%Y-%m-%d')}</DateFrom>
      <DateTo>{to_date.strftime('%Y-%m-%d')}</DateTo>
    </Inflation>
  </soap:Body>
</soap:Envelope>"""

        logger.info(f'Тело запроса\n{body}')

        response = requests.post(url, data=body, headers=headers)
    
        result_xml = response.content

        # Пространства имён
        ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'cb': 'http://web.cbr.ru/',
            'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1',
            'xs': 'http://www.w3.org/2001/XMLSchema'
        }

        # Парсим XML
        root = ET.fromstring(result_xml)

        # Находим секцию InflationResult -> diffgram -> Infl
        inflation_result = root.find('.//cb:InflationResult', ns)
        diffgram = inflation_result.find('.//diffgr:diffgram', ns)
        infl = diffgram.find('Infl', ns)  # Внутри diffgram лежит Infl

        # Собираем данные
        rows = []
        for ri in infl.findall('RI'):
            dts = ri.find('DTS').text if ri.find('DTS') is not None else None
            key_rate = ri.find('KeyRate').text if ri.find('KeyRate') is not None else None
            inf_val = ri.find('infVal').text if ri.find('infVal') is not None else None
            aim_val = ri.find('AimVal').text if ri.find('AimVal') is not None else None

            rows.append({
                'Дата': dts,
                'Ключевая ставка, % годовых': float(key_rate) if key_rate else None,
                'Инфляция, % г/г': float(inf_val) if inf_val else None,
                'Цель по инфляции, %': float(aim_val) if aim_val else None
            })

        return rows
    
    @task()
    def save_data_to_db(data):
        """
        Сохраняет данные в БД
        :param data: словарь c данными из датафрейма
        """

        logging.info(f'Полученные данные {data}')

        values_to_db = [
            (
                dt.datetime.strptime(row['Дата'], '%m.%Y'),
                row['Ключевая ставка, % годовых'],
                row['Цель по инфляции, %'],
                row['Инфляция, % г/г']
            )
            for row in data
        ]

        dates = [el[0] for el in values_to_db]

        logger.info(
            f"Получены данные на даты с {min(dates).strftime('%Y-%m-%d')}"
            f" по {max(dates).strftime('%Y-%m-%d')}"
            )

        hook = PostgresHook(postgres_conn_id='pg_database')
        conn = hook.get_conn()
        cursor = conn.cursor()

        for date, key_rate, inflation_target, inflation in values_to_db:

            # Проверка существования записи
            cursor.execute("""
                SELECT 1 FROM economic.inflation_key_rate 
                WHERE date = %s
            """, (date,))

            if cursor.fetchone():
                logger.warning(f"Данные на дату {date} уже содержатся в БД")
            else:
                cursor.execute("""
                    INSERT INTO economic.inflation_key_rate (date, key_rate, inflation_target, inflation)
                    VALUES (%s, %s, %s, %s)
                """, (date, key_rate, inflation_target, inflation))
                logger.info("Записано значение на дату %s", date)

        conn.commit()

    # Установка зависимостей
    fetched_data = fetch_data_from_api(execution_date="{{ execution_date }}")
    save_data_to_db(fetched_data)


# Регистрация DAG
rate_dag = rate_dag()