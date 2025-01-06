from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ETL import (
    extract_stock_info, download_stock_prices,
    fetch_exchange_rate, calculate_indicators,
    upload_to_bigquery
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def etl_task():
    logging.info("Starting ETL pipeline via Airflow.")
    stock_info = extract_stock_info()
    stock_price = download_stock_prices(stock_info)
    rate_data = fetch_exchange_rate()
    stock_indicator = calculate_indicators(stock_price)

    upload_to_bigquery(stock_info, 'stock_info')
    upload_to_bigquery(stock_price, 'stock_price')
    upload_to_bigquery(stock_indicator, 'stock_indicator')
    upload_to_bigquery(rate_data, 'rate_data')
    logging.info("ETL pipeline completed.")

with DAG(
    'etl_dag',
    default_args=default_args,
    description='Stock ETL DAG',
    schedule_interval='0 5 * * *',
    start_date=datetime(2025, 1, 6),
    catchup=False,
) as dag:
    etl_run = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=etl_task
    )
