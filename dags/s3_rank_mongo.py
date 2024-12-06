from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_s3 import S3ToS3Operator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from collections import Counter

# MongoDB 연결
mongo_hook = MongoHook(conn_id="mongodb_default")  # Airflow 연결으로 MongoDB 설정

# S3에서 로그 읽기
def get_logs_from_s3(bucket_name: str, file_key: str) -> list:
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    parquet_data = response['Body'].read()

    # pandas로 Parquet 파일을 읽어 DataFrame으로 변환
    df = pd.read_parquet(pd.compat.BytesIO(parquet_data))  # pyarrow 또는 fastparquet 필요
    logs = df.to_dict(orient="records")  # DataFrame을 dict 형식으로 변환
    return logs

# 상위 클릭된 action rank 저장
def save_rank_to_mongo(top_products: list):
    collection = mongo_hook.get_conn().top_products  # MongoDB collection 참조
    collection.delete_many({})  # 기존 데이터를 삭제하고 새로 저장
    collection.insert_many(top_products)

# 로그 처리 및 MongoDB에 저장하는 작업
def extract_rank(**kwargs):
    logs = get_logs_from_s3(bucket_name="t1-tu-data", file_key=f'logs/api_logs.parquet')
    action_counter = Counter()
    
    # action 항목 카운트
    for log in logs:
        action = log.get('action')
        action_counter[action] += 1

    # 상위 'top' action 추출
    top_products = [{"action": action, "clicks": count} for action, count in action_counter.most_common(5)]
    save_rank_to_mongo(top_products)  # MongoDB에 저장

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 7),  # 시작 날짜 설정
}

with DAG(
    's3_logs_and_save_rank',
    default_args=default_args,
    description='매일 S3 로그를 처리하고 상위 클릭 상품을 MongoDB에 저장',
    schedule_interval='0 0 * * *',  # 매일 자정에 실행
    catchup=False,  # DAG의 과거 날짜에 대한 실행을 건너뜀
) as dag:
    # 로그 처리 및 MongoDB 저장 작업
    save_rank = PythonOperator(
        task_id='save_rank',
        python_callable=extract_rank,
        provide_context=True,  # 태스크에서 context를 제공
    )

