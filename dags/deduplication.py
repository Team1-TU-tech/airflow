from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId
from datetime import timedelta, datetime
import requests

load_dotenv()

def deduplication():

    # MongoDB에 연결
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client["tut"]  # 데이터베이스 이름
    collection = db["data"]  # 컬렉션 이름

    # 1단계: 중복된 duplicatekey을 찾고, hosts 배열의 크기가 2인 문서는 제외하기
    pipeline = [
        # 중복된 duplicatekey을 그룹화하여 해당 문서들의 _id와 hosts 배열을 수집
        {
            "$sort": {
                "hosts": -1 # hosts 배열 크기 기준 내림차순 정렬
                }
        },
        {
            "$group": {
                "_id": "$duplicatekey",  # duplicatekey을 기준으로 그룹화
                "count": { "$sum": 1 },  # 중복된 문서 수 계산
                "documents": { "$push": "$_id" }, # 해당 그룹의 모든 _id를 수집
                "hosts": { "$first": "$hosts" }  # 첫 번째 문서의 hosts 배열을 가져옴
            }
        },
        # 2단계: 중복된 duplicatekey만 찾기 (count > 1)
        {
            "$match": {
                "count": { "$gt": 1 }  # 중복된 duplicatekey만 필터링
            }
        },
]

    # 중복된 duplicatekey을 가진 문서들의 _id를 찾음
    duplicates = collection.aggregate(pipeline)

    # 2단계: 중복된 문서 중 hosts 배열의 크기가 2인 문서는 제외
    ids_to_delete = []
    for doc in duplicates:
        documents = doc["documents"]

        # documents 배열에서 첫 번째 문서를 제외하고 나머지 문서들 삭제 대상으로 추가
        if len(documents) > 1:
            ids_to_delete.extend(documents[1:])

    # 3단계: 중복된 문서들 삭제
    if ids_to_delete:
        # deleteMany를 사용하여 중복된 문서들 삭제
        result = collection.delete_many({
            "_id": { "$in": [ObjectId(id) for id in ids_to_delete] }
        })
        print(f"{result.deleted_count} 문서가 삭제되었습니다.")
    else:
        print("삭제할 문서가 없습니다.")

def success_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"중복 데이터 정리 완료 👍"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    print("#"*35)
    print("airflow 작업완료")
    print("#"*35)
    return True

def fail_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"중복 데이터 정리 🔥실패🔥"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    print("#"*35)
    print("airflow 작업실패")
    print("#"*35)
    return True

with DAG(
'deduplication',
default_args={
'email_on_failure': False,
'email_on_retry': False,
'execution_timeout': timedelta(minutes=5),
'retries': 1,
'retry_delay':timedelta(minutes=3),
},
description='deduplication DAG',
start_date=datetime(2024, 12, 20),
schedule_interval='@daily',
catchup=False,
tags=['deduplication', 'mongoDB']
) as dag:

    start = EmptyOperator(
            task_id='start'
            )

    end = EmptyOperator(
            task_id='end',
            trigger_rule ="one_success"
            )

    remove_duplication = PythonOperator(
            task_id='remove.duplication',
            python_callable=deduplication,
            )

    success_noti = PythonOperator(
            task_id='success.noti',
            python_callable=success_noti
            )

    fail_noti = PythonOperator(
            task_id='fail.noti',
            python_callable=fail_noti,
            trigger_rule="one_failed"
            )

    start >> remove_duplication >> [success_noti, fail_noti] >> end
