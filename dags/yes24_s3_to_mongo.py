from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import os

os.environ['LC_ALL'] = 'C'


def mongodb():
    #from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from ocr.img_to_text import s3_to_mongodb

    #hook = S3Hook(aws_conn_id='yes24') 
    
    print('*' * 30)
    print(f"Sending to Mongodb")
    print('*' * 30)
    to_mongo = s3_to_mongodb()

with DAG(
'yes24_s3_to_MongoDB',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='s3에 있는 yes24 데이터를 mongodb로 보내는 DAG',
schedule_interval='@daily',
start_date=datetime(2024, 11, 27),
catchup=True,
max_active_runs=3,  # 동시에 3개 크롤링 실행
tags=['yes24'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    s3_to_mongodb = PythonVirtualenvOperator(
            task_id='s3_to_mongodb',
            python_callable=mongodb,
            requirements=[
                "git+https://github.com/Team1-TU-tech/ocr.git@0.1.1/dev/easyocr",
                ],
            system_site_packages=True,
            trigger_rule='all_success',
            )


    notify_success = BashOperator(
            task_id='notify.success',
            bash_command="""
                echo "notify.success"
                curl -X POST -H 'Authorization: Bearer mo6ux0e446tQ5tcw6gsvHbdAdPdehM0NYvD3XixCjxf' -F 'message=saved success' https://notify-api.line.me/api/notify
            """,
            trigger_rule="all_done"
            )

    notify_fail = BashOperator(
            task_id='notify.fail',
            bash_command="""
                echo "notify.fail"
                curl -X POST -H 'Authorization: Bearer mo6ux0e446tQ5tcw6gsvHbdAdPdehM0NYvD3XixCjxf' -F 'message=try again' https://notify-api.line.me/api/notify
            """,
            trigger_rule='one_failed'
            )

    start >> s3_to_mongodb
    s3_to_mongodb >> notify_success >> end
    s3_to_mongodb >> notify_fail >> end
