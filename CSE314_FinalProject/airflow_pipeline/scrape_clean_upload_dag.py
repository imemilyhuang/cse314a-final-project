from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.scrape_clean_upload import scrape_data, clean_data, upload_data

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_clean_upload_dag',
    default_args=default_args,
    description='Scrape internships, clean data, and upload to Snowflake',
    schedule_interval=None,
    start_date=datetime(2025, 4, 28),
    catchup=False,
    tags=['scraping', 'snowflake']
)

scrape_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_data',
    python_callable=upload_data,
    dag=dag,
)

scrape_task >> clean_task >> upload_task
