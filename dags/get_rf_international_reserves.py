import requests
import logging
import pandas as pd
import json
import xml.etree.ElementTree as ET
import datetime as dt
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

# Логгер
logger = logging.getLogger(__name__)

# Определение DAG с помощью декоратора @dag
@dag(
    dag_id='rf_inter_reserves',
    schedule_interval='@monthly',
    start_date=dt.datetime(2000, 1, 1),
    catchup=True,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': dt.timedelta(seconds=10),
    },
    tags=['reserves'],
    max_active_runs=1,
)

def inter_reserves():

    @task()
    def fetch_inter_reserves_data(execution_date: str):

        from_date = dt.datetime.fromisoformat(execution_date) + relativedelta(months=-1)
        to_date = dt.datetime.fromisoformat(execution_date) + relativedelta(days=-1)

        logger.info(f'Получение данных за даты с {from_date} по {to_date}')

        # Рабочий URL сервиса
        url = "http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"

        # Заголовки
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '"http://web.cbr.ru/mrrf7D"',
            "User-Agent": "Mozilla/5.0"  # Имитация браузера
            }
        
        body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <mrrf7D xmlns="http://web.cbr.ru/">
      <fromDate>{from_date.strftime('%Y-%m-%d')}</fromDate>
      <ToDate>{to_date.strftime('%Y-%m-%d')}</ToDate>
    </mrrf7D>
  </soap:Body>
</soap:Envelope>"""

        logger.info(f'Тело запроса {body}')

        response = requests.post(url, data=body, headers=headers)
        response.raise_for_status()

        result_xml = response.content
        
        # if response.status_code == 200:
        #     return response.content
        # else:
        #     raise Exception(f"Request failed with status {response.status_code}: {response.text}")

        ns_soap = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                'cb': 'http://web.cbr.ru/',
                'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1'}

        # Находим секцию diffgram
        diffgram = ET.fromstring(result_xml).find('.//cb:mrrf7DResult/diffgr:diffgram/mmrf7d', ns_soap)

        # Собираем данные в список словарей
        rows = []
        for mr in diffgram.findall('mr'):
            D0 = mr.find('D0').text
            val = mr.find('val').text
            rows.append({'Дата': D0, 'Значение': float(val)})

        # Создаем DataFrame
        df = pd.DataFrame(rows)

        logging.info(f'Датафрейм создан {df.to_dict()}')



        # При желании можно преобразовать колонку "Дата" в datetime
        # df['Дата'] = pd.to_datetime(df['Дата'])

        return df.to_dict()
    
    @task()
    def save_data_to_db(data):
        """
        Сохраняет данные по нескольким в БД
        :param data: словарь c данными из датафрейма
        """

        values_to_db = [
            (dt.datetime.fromisoformat(data['Дата'][i]).date(),
             data['Значение'][i]) for i in range(len(data['Дата']))
            ]

        logger.info(
            f"Получены данные на даты с {values_to_db[0][0].strftime('%Y-%m-%d')} по {values_to_db[-1][0].strftime('%Y-%m-%d')}"
            )

        hook = PostgresHook(postgres_conn_id='pg_database')
        conn = hook.get_conn()
        cursor = conn.cursor()

        for date, value in values_to_db:

            # СОЗДАТЬ ТАБЛИЦУ И ДОПИСАТЬ ЭКСПОРТ

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
    
    fetched_data = fetch_inter_reserves_data(execution_date="{{ execution_date }}")

# Регистрация DAG
dag = inter_reserves()