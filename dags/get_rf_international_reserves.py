import requests
import logging
import pandas as pd
import json
import xml.etree.ElementTree as ET
import datetime as dt

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
)

def inter_reserves():

    @task()
    def fetch_bauction_data(from_date: dt.datetime, to_date: dt.datetime):
        
        body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <mrrf7D xmlns="http://web.cbr.ru/">
      <fromDate>{from_date.strftime('%Y-%m-%d')}</fromDate>
      <ToDate>{to_date.strftime('%Y-%m-%d')}</ToDate>
    </mrrf7D>
  </soap:Body>
</soap:Envelope>"""

        response = requests.post(url, data=body, headers=headers)
        
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

        # При желании можно преобразовать колонку "Дата" в datetime
        df['Дата'] = pd.to_datetime(df['Дата'])

        return df