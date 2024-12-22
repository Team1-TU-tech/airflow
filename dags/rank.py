from bson import ObjectId
from pymongo import MongoClient
from collections import Counter
from typing import List
from io import BytesIO
import boto3
import pandas as pd
import datetime
from datetime import timedelta, datetime
import os
from dotenv import load_dotenv
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

load_dotenv()

def get_logs(bucket_name: str = "t1-tu-data", directory: str = 'view_detail_log/') -> List[str]:
    try:
        s3_hook = S3Hook(aws_conn_id='data')
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=directory)
        parquet_files = [file for file in files if file.endswith('.parquet')]

        if not parquet_files:
            print("No Parquet files found.")
            return []

        logs = []
        for file_key in parquet_files:
            try:
                print(f"Reading file: s3://{bucket_name}/{file_key}")
                obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                file_content = obj.get()["Body"].read()  # S3 객체 내용 읽기
                parquet_data = pd.read_parquet(BytesIO(file_content))  # BytesIO로 감싸기
                logs.append(parquet_data)
            except Exception as e:
                print(f"Error reading file {file_key}: {e}")

        all_logs = pd.concat(logs, ignore_index=True)
        ticket_ids = all_logs['ticket_id'].tolist()

        return ticket_ids

    except Exception as e:
        print(f"Error reading from S3: {e}")
        return []

def save_popular_to_db(ticket_ids: List[str]):
    try:
        # MongoDB 연결
        client = MongoClient(os.getenv('MONGO_URI'))
        db = client["tut"]
        popular_collection = db['popular']
        print("MongoDB connection OK")

        # 티켓 데이터 카운트
        ticket_counter = Counter(ticket_ids)
        sorted_tickets = [{"ticket_id": ticket_id, "count": count} for ticket_id, count in ticket_counter.most_common()]
        print("Extract sorted_tickets done")

        # 컬렉션에 데이터가 있는지 확인
        if popular_collection.count_documents({}) > 0:
            # 데이터가 하나라도 있으면 모든 문서를 삭제
            delete_result = popular_collection.delete_many({})
            print(f"Deleted {delete_result.deleted_count} documents from popular collection.")
        else:
            print("No existing documents to delete in popular collection.")

        # 새로운 데이터를 삽입
        if sorted_tickets:
            print(f'{sorted_tickets}')
            insert_result = popular_collection.insert_many(sorted_tickets)
            print(f"Inserted {len(insert_result.inserted_ids)} documents into popular collection.")
        else:
            print("No tickets to insert into popular collection.")

        print("Popular collection updated.")
    except Exception as e:
        print(f"Error updating popular collection: {e}")
    finally:
        # MongoDB 연결 종료
        if client:
            client.close()
            print("MongoDB connection closed.")

# DAG에서 사용할 함수
def get_logs_save_to_db(bucket_name: str, directory: str):
    ticket_ids = get_logs(bucket_name, directory)
    if ticket_ids:
        return save_popular_to_db(ticket_ids)

def success_noti():                                                                    
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"rank DB에 저장 완료 👍"}    
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)    
    print("#"*35)  
    print("airflow 작업완료")    
    print("#"*35)   
    return True

def fail_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"rank DB에 저장 🔥실패🔥"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    print("#"*35)
    print("airflow 작업실패")
    print("#"*35)
    return True

with DAG(
'save_rank_to_mongo',
default_args={
'email_on_failure': False,
'email_on_retry': False,
'execution_timeout': timedelta(minutes=5),
'retries': 1,
'retry_delay':timedelta(minutes=3),
},
description='save rank  DAG',
start_date=datetime(2024, 12, 20),
schedule_interval='@daily',
catchup=False,
tags=['rank','S3','FastAPI', 'mongoDB']
) as dag:

    start = EmptyOperator(
            task_id='start'
            )

    end = EmptyOperator(
            task_id='end',
            trigger_rule ="one_success"
            )

    save_rank = PythonOperator(
            task_id='save.rank',
            python_callable=get_logs_save_to_db,
            op_kwargs={'bucket_name': "t1-tu-data", 'directory': "view_detail_log/"}
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

    start >> save_rank >> [success_noti, fail_noti] >> end
