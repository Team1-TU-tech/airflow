from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import subprocess

#def get_data():
#        from interpark.selling_page_test import transform_raw
#        from pymongo import MongoClient
#        import logging
        

#def insert_data_mongo():
#    client = MongoClient(MONGO_URI) 
    # 데이터베이스와 컬렉션 선택
#    db = client["test"]  # 'test' 데이터베이스
#    collection = db["test"]  # 'ticket' 컬렉션
#    data = transform_raw()
#    for item in data:
#        collection.insert_one(data)
#   logging.info("MongoDB에 데이터 저장 성공: %s", result.inserted_id)

def run_producer():
    subprocess.run(["python", "/home/hahahellooo/final/airflow/kafka/producer.py"])

def run_consumer():
    subprocess.run(["python", "/home/hahahellooo/final/airflow/kafka/consumer.py"])

with DAG(
'interpark_kafka',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 3,
'retry_delay':timedelta(minutes=1),
},
description='interpark DAG',
start_date=datetime(2024, 11, 21),
catchup=False,
tags=['interpark','kafka']
) as dag:
    
    producer = PythonOperator(
            task_id='producer',
            python_callable=run_producer
            )

    consumer = PythonOperator(
            task_id='consumer',
            python_callable=run_consumer
            )

    producer >> consumer
