from airflow import DAG
import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from sqlalchemy import Float


timestamp = datetime.date.today()
timestamp_url = timestamp.strftime("%d-%m-%Y").replace('-', '/')
URL = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={timestamp_url}'
# URL = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req=06/02/2024'


def parse_file(url, table_name):
    response = requests.get(url)

    # Проверка, успешен ли запрос
    if response.status_code == 200:
        # Парсинг XML
        root = ET.fromstring(response.content)
        data = []
        for valute in root.findall('Valute'):
            record = {
                'numcode': valute.find('NumCode').text,
                'code_iso': valute.find('CharCode').text,
                'name': valute.find('Name').text,
                'value': valute.find('Value').text.replace(',', '.'),
                'date': timestamp
            }
            data.append(record)

        df = pd.DataFrame(data)

        rub_row = pd.DataFrame([{
            'numcode': '643',
            'code_iso': 'RUB',
            'name': 'Российский рубль',
            'value': 1.0,
            'date': timestamp
        }])

        df = pd.concat([df, rub_row], ignore_index=True)
        postgres_hook = PostgresHook("postgres-db")
        engine = postgres_hook.get_sqlalchemy_engine()
        df.to_sql(table_name, engine, schema="stage",
                  if_exists='replace', index=False,
                  dtype={'value': Float()})


default_args = {
    'owner': 'saufa',
    'start_date': datetime.datetime(2025, 1, 4),
    'retries': 1

}

with DAG(
    'currency_dag',
    default_args=default_args,
    description='Пытаемся грузить в справочник валюты.',
    catchup=False,
    schedule_interval='@daily'
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    parse_file = PythonOperator(
        task_id="currency",
        python_callable=parse_file,
        op_kwargs={"table_name": "currency", 'url': URL}
    )

    end = EmptyOperator(
        task_id='end'
    )

(start >> parse_file >> end)
