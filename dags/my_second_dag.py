import requests
import io
import datetime as dt
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2000, 1, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(days=1),
}

def get_data(**kwargs):

    url = f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={date_today}"
    xml_data = requests.get(url).text
    pd.read_xml(io.StringIO(xml_data), parser='etree')




with DAG(dag_id='first_dag',
         schedule_interval='@daily',
         default_args=default_args) as dag:

    dice = PythonOperator(
        task_id='random_dice',
        python_callable=random_dice,
        dag=dag,
    )