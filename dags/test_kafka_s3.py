from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
import io, time
from kafka import KafkaConsumer
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json
import base64

def producer_to_kafka():
    from interpark.raw_ticket_page import extract_ticket_html

    get_data = extract_ticket_html()
    print("데이터 불러오기 완료")
    
    from kafka import KafkaProducer
    import json
    import io
    import base64
    producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    topic = 'raw_interpark_data'
    for data in get_data:
        try:
            # 데이터를 문자열로 가정하고 io.StringIO로 처리
            soup = data["data"]  # 크롤링 데이터의 HTML 내용

            # BeautifulSoup 객체를 HTML 문자열로 변환
            if hasattr(soup, "prettify"):
                html_content = soup.prettify()  # 예쁘게 정리된 HTML
            else:
                html_content = str(soup)  # 일반 문자열로 변환

            if not html_content.strip():  # HTML 데이터가 비어 있는지 확인
                raise ValueError("HTML 데이터가 비어 있습니다.")
            
            # HTML을 바이트로 인코딩하여 파일 객체로 저장
            file_obj = io.BytesIO(html_content.encode('utf-8'))
            # 바이트 데이터를 Base64로 인코딩하여 JSON 직렬화 가능하도록 변환 
            encoded_content = base64.b64encode(file_obj.getvalue()).decode('utf-8')
            # kafka 메세지 생성
            message  = {'title': f'test_{data["num"]}_{data["ticket_num"]}.html',
                        'save_path': f'interpark/test_{data["num"]}_{data["ticket_num"]}.html',
                        'contents' : encoded_content
                        }  
            
            producer.send(topic, value=message)  
            print(f"카프카로 전송 완료:{message}")

        except ValueError as ve:
            print(f"데이터가 없습니다. 오류: {ve}")
        except Exception as e:
            print(f"예상치 못한 오류 발생: {e}")

    # 모든 데이터 전송 후 flush
    producer.flush()


def kafka_to_s3():
    import json
    retry_count = 3
    attempt = 0

    # kafka 연결
    while attempt < retry_count:
        try:
            consumer = KafkaConsumer(
                'raw_interpark_data',
                bootstrap_servers='kafka:9092',
                auto_offset_reset="earliest",
                group_id='interpark_s3',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000,
            )

            if consumer:
                print("kafka 연결 성공")
                break
            else:
                attempt += 1
                print(f"kafka 연결 실패: {retry_count}번째 연결 시도 중....")
                time.sleep(5)
                continue

        except Exception as e:
            print(f"kafka 연결 실패: {e}")

    # 컨슈머 연결되면 s3로 전송
    empty_count = 0  # 메시지가 없을 때 카운트할 변수

    while True:
        msg = consumer.poll(timeout_ms=1000)

        # 메시지가 없으면 기다림
        if msg is None:
            empty_count += 1
            print(f"{empty_count}: 메세지가 없습니다.")
            # 5번 연속으로 메시지가 없으면 종료
            if empty_count >= 5:
                print("메시지가 5번 연속으로 없어 종료합니다.")
                break
            continue
        else:
            empty_count = 0  # 메시지가 있으면 카운트 초기화

        for message in msg.values():
            for data in message:
                try:
                    # Kafka에서 메시지를 가져오기
                    data = data.value
                    print(f"Kafka에서 받은 데이터: {data}")

                    # Base64로 인코딩된 'contents'를 디코딩하여 원래 바이트 데이터를 복원
                    decoded_content = base64.b64decode(data['contents'])

                    # 바이트 데이터를 BytesIO 객체로 복원
                    file_obj = io.BytesIO(decoded_content)

                    # S3로 업로드
                    key = data['save_path']
                    bucket_name = 't1-tu-data'

                    hook = S3Hook(aws_conn_id='interpark')  # s3 연결
                    hook.get_conn().put_object(
                        Bucket=bucket_name,
                        Key=key,
                        Body=file_obj
                    )
                    print(f"S3에 업로드 완료: {bucket_name}/{key}")
                    break
                except Exception as e:
                    print(f"S3 업로드 실패: {e}")
                    continue


def success_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"airflow 작업 완료👍"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    return response

def fail_noti():
    url = "https://notify-api.line.me/api/notify"
    data = {"message":"airflow 작업 🔥실패🔥"}
    headers={"Authorization": 'Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4'}
    response = requests.post(url, data, headers=headers)
    return response

with DAG(
'kafka_to_S3',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'execution_timeout': timedelta(minutes=10),
'retries': 3,
'retry_delay':timedelta(minutes=3),
},
description='interpark DAG',
start_date=datetime(2024, 11, 21),
schedule_interval='@daily',
catchup=False,
tags=['interpark','kafka','s3']
) as dag:

    start = EmptyOperator(
            task_id='start'
            )

    end = EmptyOperator(
            task_id='end'
            )

    producer_to_kafka = PythonVirtualenvOperator(
            task_id='producer.to.kafka',
            python_callable=producer_to_kafka,
            requirements=[
                "git+https://github.com/hahahellooo/interpark.git@0.4/s3"
                ],
            system_site_packages=True,
            )
    
    kafka_to_s3 = PythonOperator(
            task_id='kafka.to.s3',
            python_callable=kafka_to_s3
            )
    
    success_noti = PythonOperator(
            task_id='success.noti',
            python_callable=success_noti,
            trigger_rule=TriggerRule.ALL_SUCCESS
            )

    fail_noti = PythonOperator(
            task_id='fail.noti',
            python_callable=fail_noti,
            trigger_rule=TriggerRule.ONE_FAILED
            )

    start >> producer_to_kafka >> kafka_to_s3
    kafka_to_s3 >> [success_noti, fail_noti] >> end
