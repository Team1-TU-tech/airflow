from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import os

os.environ['LC_ALL'] = 'C'

def get_data():
        from crawling.crawler import scrap_data
        get_data = scrap_data()
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
    extract = PythonVirtualenvOperator(
            task_id='extract',
            python_callable=get_data,
            requirements=[
                "git+https://github.com/Team1-TU-tech/crawling.git@0.3.2/dev/yes24",
                ],
            system_site_packages=False
            )


    extract
