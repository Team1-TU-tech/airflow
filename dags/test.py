from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import os


def get_data():
        from interpark.test import extract_container_html
        get_data = extract_container_html
        print(get_data)

with DAG(
'interpark_to_s3',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='interpark DAG',
schedule_interval='@daily',
start_date=datetime(2024, 11, 21),
catchup=True,
tags=['interpark'],
) as dag:
    extract = PythonVirtualenvOperator(
            task_id='extract',
            python_callable=get_data,
            requirements=[
                "git+https://github.com/hahahellooo/interpark.git@0.3/new_crawling",
                ],
            system_site_packages=False
            )


    extract
