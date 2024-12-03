from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
import time
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator
from kafka import KafkaConsumer
from pymongo import MongoClient
import requests
import json

def s3_to_kafka():
    from interpark.read_s3_parsing import html_parsing, extract_data, convert_to_datetime_format
    from interpark.region import get_location
    data = html_parsing()
    print("데이터 불러오기 완료")

    from kafka import KafkaProducer
    import json
    producer = KafkaProducer(
            bootstrap_servers = ['kafka1:9093','kafka2:9094', 'kafka3:9095'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
    topic = 'interpark_data'
    print("==========================================")
    try:
        # ticket_data 형식에 맞게 메시지 작성
        message = {
                "title": data.get("title"),
                "category": data.get("category"),
                "location": data.get("location"),
                "region": data.get("region"),
                "price": data.get("price"),
                "start_date": data.get("start_date"),
                "end_date": data.get("end_date"),
                "show_time": data.get("show_time"),
                "running_time": data.get("running_time"),
                "casting": data.get("casting"),
                "rating": data.get("rating"),
                "description": data.get("description"),
                "poster_url": data.get("poster_url"),
                "open_date": data.get("open_date"),
                "pre_open_date": data.get("pre_open_date"),
                "artist": data.get("artist"),
                "hosts": data.get("hosts")
                }
            
        producer.send(topic, message)
        print("카프카로 전송 완료")
    except ValueError as ve:
        print(f"데이터가 없습니다. 오류: {ve}")
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")

    # 모든 데이터 전송 후 flush
    producer.flush()

def consumer_to_mongo():
    attempt = 0
    connected = False
    retry_count = 3

    try:
        client = MongoClient("mongodb+srv://hahahello777:akXSTBrO5Q5OkWb3@cluster0.5vlv3.mongodb.net/test?retryWrites=true&w=majority&appName=Cluster0")  # MongoDB 연결
        db = client['test']  # 데이터베이스 이름
        collection = db['test']  # 컬렉션 이름
        print("MongoDB 연결 성공")
    except Exception as e:
        print(f"MongoDB 연결 실패: {e}")
        return

    # kafka 연결
    while attempt < retry_count and not connected:
        try:
            consumer = KafkaConsumer(
                'interpark_data',
                bootstrap_servers= ['kafka1:9093','kafka2:9094','kafka3:9095'],
                auto_offset_reset="earliest",
                group_id='interpark_mongo',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=3000,
            )

            print("kafka 연결 성공")
            connected = True

            # 컨슈머 연결되면 s3로 전송
            empty_count = 0  # 메시지가 없을 때 카운트할 변수
            while True:
                msg = consumer.poll(timeout_ms=1000)
                # 메시지가 없으면 기다림
                if not msg:
                    empty_count += 1
                    print(f"{empty_count}: 메세지가 없습니다.")
                    # 3번 연속으로 메시지가 없으면 종료
                    if empty_count >= 3:
                        print("메시지가 3번 연속으로 없어 종료합니다.")
                        break
                    continue

                else:
                    empty_count = 0  # 메시지가 있으면 카운트 초기화
                    for message in msg.values():
                        for data in message:
                            try:
                                # Kafka에서 메시지를 가져오기
                                data = data.value
                                print(f"Kiafka에서 받은 데이터: {data}")

                                collection.insert_one(data)
                                print(f"mongodb에 데이터 저장 성공")
                            except Exception as e:
                                print(f"데이터 처리 중 오류 발생: {e}")

        except Exception as e:
            print(f"kafka 연결 실패: {e}")
            attempt += 1
            print(f"{attempt}/{retry_count} 번째 시도 중...")
            time.sleep(5)  # 5초 대기 후 재시도
   
    # Kafka Consumer 종료
    print("Consumer 종료")
    consumer.close()

def success_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"MongoDB에 데이터 저장 완료 👍"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    print("#"*35)
    print("airflow 작업완료")
    print("#"*35)
    return True

def fail_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"MongoDB에 데이터 저장 🔥실패🔥"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    print("#"*35)
    print("airflow 작업실패")
    print("#"*35)
    return True

with DAG(
's3_to_kafka_to_mongo',
default_args={
'email_on_failure': False,
'email_on_retry': False,
'execution_timeout': timedelta(minutes=10),
'retries': 3,
'retry_delay':timedelta(minutes=3),
},
description='interpark data DAG',
start_date=datetime(2024, 11, 30),
schedule_interval='@daily',
catchup=False,
tags=['interpark','kafka','s3', 'mongoDB']
) as dag:

    start = EmptyOperator(
            task_id='start'
            )

    end = EmptyOperator(
            task_id='end',
            trigger_rule ="one_success"
            )

    s3_to_kafka = PythonVirtualenvOperator(
            task_id='s3.to.kafka',
            python_callable=s3_to_kafka,
            requirements=[
                "git+https://github.com/hahahellooo/interpark.git@0.5/mongo"
                ],
            system_site_packages=True
            )

    kafka_to_mongo = PythonOperator(
            task_id='kafka.to.mongo',
            python_callable=consumer_to_mongo
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

    start >> s3_to_kafka >> kafka_to_mongo
    kafka_to_mongo >> [success_noti, fail_noti] >> end
