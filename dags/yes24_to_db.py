from datetime import datetime
from airflow import DAG
from airflow.operators.python import (PythonVirtualenvOperator, PythonOperator)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import os

os.environ['LC_ALL'] = 'C'

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='test')
    s3_hook.load_string(
        string_data="test",
        key="sunwoo-test/test.txt",
        bucket_name="sunwoo_test",
        replace=True
    )

def get_data():
        from crawling.all_crawler import all_scrape_data
        get_data = all_scrape_data()
        print(get_data)

with DAG(
'yes24_to_MongoDB',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='yes24 DAG',
schedule_interval='@daily',
start_date=datetime(2024, 11, 21),
catchup=True,
tags=['yes24'],
) as dag:

    s3_sensor = S3KeySensor(
        task_id='check_file',
        bucket_name='sunwoo_test',
        bucket_key="sunwoo-test/test.txt",
        aws_conn_id='test',
        timeout=18 * 60 * 60,
        poke_interval=120,
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    extract = PythonVirtualenvOperator(
            task_id='extract',
            python_callable=get_data,
            requirements=[
                "git+https://github.com/Team1-TU-tech/crawling.git@0.3.3/dev/yes24",
                ],
            system_site_packages=True
            )

    extract
    upload_task >> s3_sensor
