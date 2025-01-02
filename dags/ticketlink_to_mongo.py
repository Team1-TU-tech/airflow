from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import os

os.environ['LC_ALL'] = 'C'


def mongodb():
    from ocr.img_to_text import s3_to_mongodb
    from ocr.utils import get_last_id_from_redis, update_last_id_in_redis

    print('*' * 30)
    print(f"Sending to Mongodb")
    print('*' * 30)
    last_id = get_last_id_from_redis('ticketlink_mongo')
    to_mongo = s3_to_mongodb(start_id=last_id)

    if to_mongo:
        last_processed_id = int(to_mongo)
        update_last_id_in_redis('ticketlink_mongo', last_processed_id)
        print(f"마지막 ID: {last_processed_id}")
    else:
        print("값이 존재하지 않습니다.")
        return


with DAG(
'ticketlink_s3_to_MongoDB',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='s3에 있는 ticketlink 데이터를 mongodb로 보내는 DAG',
schedule_interval='@daily',
start_date=datetime(2024, 12, 17),
catchup=True,
max_active_runs=3,  # 동시에 3개 크롤링 실행
tags=['ticketlink'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    s3_to_mongodb = PythonVirtualenvOperator(
            task_id='s3_to_mongodb',
            python_callable=mongodb,
            requirements=[
                "git+https://github.com/Team1-TU-tech/ocr.git",
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
            trigger_rule="all_success"
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
