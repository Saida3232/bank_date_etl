import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

TABLE_NAME = 'dm_f101_round_f'
SCHEMA_NAME = 'DM'

db_params = {
    'dbname': 'demo',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5433
}

connection = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
engine = create_engine(connection)


try:
    df = pd.read_sql_table(TABLE_NAME, engine, 'dm')
    df.to_csv(f'{TABLE_NAME}.csv', sep=',', index=False)
    logging.info("CSV файл успешно создан!")

    df_read = pd.read_csv(f'{TABLE_NAME}.csv', sep=',')
    df_read.to_sql('dm_f101_round_f_v2', engine, schema='dm',
                   if_exists='append', index=False)
    logging.info("Данные успешно загружены в таблицу!")

except Exception as e:
    print(f'Произошла ошибка: {e}')
