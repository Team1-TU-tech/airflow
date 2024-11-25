from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import os

def get_data(MONGO_URI):
        from interpark.selling_page_test import transform_raw
        from pymongo import MongoClient
        import logging
        
        client = MongoClient(MONGO_URI) 
        
        # 데이터베이스와 컬렉션 선택
        db = client["test"]  # 'test' 데이터베이스
        collection = db["test"]  # 'ticket' 컬렉션
        data = transform_raw()
        result = collection.insert_one(data)

        logging.info("MongoDB에 데이터 저장 성공: %s", result.inserted_id) 

with DAG(
'interpark_to_mongodb',
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
tags=['interpark','mongdb']
) as dag:

    mongo_uri = os.getenv("MONGO_URI")
    mongo_uri = Variable.get("MONGO_URI")

    extract = PythonVirtualenvOperator(
            task_id='extract',
            python_callable=get_data,
            requirements=[
                "git+https://github.com/hahahellooo/interpark.git@0.3/new_crawling",
                ],
            system_site_packages=False,
            op_kwargs={"mongo_uri": mongo_uri},           
            )


    extract
