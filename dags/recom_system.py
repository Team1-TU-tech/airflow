from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import os

os.environ['LC_ALL'] = 'C'


def recommender():
    from ml.main import main

    print('*' * 30)
    print(f"Sending to Mongodb")
    print('*' * 30)

    recommendation = main()


with DAG(
'recommender_MongoDB',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='mongodb에 저장된 공연데이터로 추천시스템을 만들어 유사 공연을 다시 mongodb로 보내는 DAG',
schedule_interval='@daily',
start_date=datetime(2024, 12, 30),
catchup=True,
max_active_runs=3,  # 동시에 3개 크롤링 실행
tags=['recommendation'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    recommender = PythonVirtualenvOperator(
            task_id='recommendation_to_mongodb',
            python_callable=recommender,
            requirements=[
                "git+https://github.com/Team1-TU-tech/ml.git",
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

    start >> recommender
    recommender >> notify_success >> end
    recommender >> notify_fail >> end
