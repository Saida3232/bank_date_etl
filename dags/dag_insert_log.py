from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable
import pandas as pd
from datetime import datetime

PATH = Variable.get('my_path')

conf.set('core', 'template_searchpath', PATH)


def insert_data(table_name):
    df = pd.read_csv(PATH + f"/{table_name}.csv",
                     delimiter=";")
    df.columns = [col.lower() for col in df.columns]
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage",
              if_exists="append", index=False)


def insert_data_md_exchange_rate_d(table_name):
    df = pd.read_csv(PATH + f"/{table_name}.csv",
                     delimiter=";")
    df.columns = [col.lower() for col in df.columns]
    df = df.drop_duplicates(subset=['currency_rk', 'data_actual_date'])
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage",
              if_exists="append", index=False)


def insert_data_in_currency(table_name):
    df = pd.read_csv(PATH + f"/{table_name}.csv",
                     delimiter=";", encoding='cp1252')
    df.columns = [col.lower() for col in df.columns]
    df = df.drop_duplicates(subset=['currency_rk', 'data_actual_date'])
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage",
              if_exists="append", index=False)


default_args = {
    "owner": "saida",
    "start_date": datetime(2024, 2, 25),
    "retries": 1
}


with DAG(
    "dag_insert_log",
    default_args=default_args,
    description="Загрузка данных в stage",
    catchup=False,
    schedule_interval="0 0 * * *"
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id='md_account_d',
        python_callable=insert_data,
        op_kwargs={'table_name': 'md_account_d'}
    )
    md_currency_d = PythonOperator(
        task_id='md_currency_d',
        python_callable=insert_data_in_currency,
        op_kwargs={'table_name': 'md_currency_d'}
    )
    md_exchange_rate_d = PythonOperator(
        task_id='md_exchange_rate_d',
        python_callable=insert_data_md_exchange_rate_d,
        op_kwargs={'table_name': 'md_exchange_rate_d'}
    )
    md_ledger_account_s = PythonOperator(
        task_id='md_ledger_account_s',
        python_callable=insert_data,
        op_kwargs={'table_name': 'md_ledger_account_s'}
    )

    split = EmptyOperator(
        task_id='split'
    )

    trancate_data_in_table_f_posting = SQLExecuteQueryOperator(
        task_id='trancate_data_ft_posting_data',
        conn_id='postgres-db',
        sql='truncate ds.ft_balance_f;'
    )

    call_get_posting_data_by_date = SQLExecuteQueryOperator(
        task_id='call_get_posting_data_by_date',
        conn_id='postgres-db',
        sql='CALL dm.get_posting_data_by_date()'
    )

    sql_ft_balance = SQLExecuteQueryOperator(
        task_id='sql_ft_balance',
        conn_id='postgres-db',
        sql='CALL ds.stage_to_ds_ft_balance_f();'
    )
    sql_ft_posting_f = SQLExecuteQueryOperator(
        task_id='sql_ft_posting_f',
        conn_id='postgres-db',
        sql='CALL ds.stage_to_ds_ft_posting_f();'
    )
    sql_md_account_d = SQLExecuteQueryOperator(
        task_id='sql_md_account_d',
        conn_id='postgres-db',
        sql='CALL ds.stage_to_md_account_d();'
    )
    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id='sql_md_currency_d',
        conn_id='postgres-db',
        sql='CALL ds.stage_to_md_currency_d();'
    )
    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id='sql_md_ledger_account_s',
        conn_id='postgres-db',
        sql='CALL ds.stage_to_md_ledger_account_s();'
    )
    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id='sql_md_exchange_rate_d',
        conn_id='postgres-db',
        sql='CALL ds.stage_to_md_exchange_rate_d();'
    )
    end = EmptyOperator(
        task_id="end"
    )

    (
        start
        >> trancate_data_in_table_f_posting
        >> [ft_balance_f,
            ft_posting_f,
            md_account_d,
            md_currency_d,
            md_exchange_rate_d,
            md_ledger_account_s
            ]
        >> split
        >> [sql_ft_balance,
            sql_ft_posting_f,
            sql_md_account_d,
            sql_md_currency_d,
            sql_md_ledger_account_s,
            sql_md_exchange_rate_d
            ]
        >> call_get_posting_data_by_date
        >> end
    )
