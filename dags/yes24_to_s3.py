from datetime import datetime
from airflow import DAG
from airflow.operators.python import (PythonVirtualenvOperator, PythonOperator)
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import os

os.environ['LC_ALL'] = 'C'

def sending_to_s3():
    print('*' * 30)
    print(f"Processing Task")
    print('*' * 30)

    from crawling.utils import get_last_id_from_redis, update_last_id_in_redis
    from crawling.links import get_link
    
    # 링크 가져오기
    last_id = get_last_id_from_redis()
    links = get_link(start_id=last_id)
    if links:
        #URL에서 마지막 ID 추출
        last_url = links[-1]  # 마지막 URL
        last_processed_id = int(last_url.split('/')[-1])
        update_last_id_in_redis(last_processed_id) #Redis에 저장
        print(f"마지막 ID: {last_processed_id}")
    else:
        print("링크 리스트가 비어 있습니다.")
        return

    # 데이터 수집
    from crawling.all_crawler import all_scrap
    data = all_scrap(links)
    print("크롤링 데이터 수집 완료")
    
    from kafka import KafkaProducer
    import json, io
    # Kafka 전송
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:9092', 'kafka2:9093', 'kafka3:9094'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    topic = 'yes24-data' #topic 지정
    
    for item in data:
        try:
            # 데이터를 문자열로 가정하고 io.StringIO로 처리
            soup = item["data"]  # 크롤링 데이터의 HTML 내용

            # BeautifulSoup 객체를 HTML 문자열로 변환
            if hasattr(soup, "prettify"):
                html_content = soup.prettify()  # 예쁘게 정리된 HTML
            else:
                html_content = str(soup)  # 일반 문자열로 변환

            file_obj = io.BytesIO(html_content.encode('utf-8'))
            
            # Kafka 메시지 생성
            message = {
                "id": item['title'],
                "data_path": f"s3://t1-tu-data/yes24/{item['title']}.html",
                "contents": file_obj.getvalue()  # HTML 데이터를 contents로 전송
            }

            producer.send(topic, value=message)
            producer.flush()
            print(f"Kafka 메시지 전송 완료 {message}")

        except Exception as e:
            print(f"메시지 전송 실패: {e}")


def upload_to_s3():
    print('*' * 30)
    print(f"Kafka Consuming")
    print('*' * 30)

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import io, time
    from kafka import KafkaConsumer

    connected = False
    while not connected:
        try:
            # Kafka Consumer 설정
            consumer = KafkaConsumer(
                'yes24-data',
                bootstrap_servers=['kafka1:9092', 'kafka2:9093', 'kafka3:9094'],
                auto_offset_reset='earliest',  # 새로운 메시지부터 소비
                group_id='s3-upload-group',  # 동일한 그룹으로 소비
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            connected = True
            print("Kafka 연결 성공")

        except Exception as e:
            print(f"Kafka 연결 실패, 5초 후 재시도: {e}")
            time.sleep(5)
    
    # S3 클라이언트 설정
    hook = S3Hook(aws_conn_id='yes24') 
     # 메시지를 10초 동안 기다림
    timeout_seconds = 10
    start_time = time.time()

    while True:
        # Kafka Consumer에서 메시지를 폴링
        messages = consumer.poll(timeout_ms=timeout_seconds * 5000)  # 타임아웃을 밀리초 단위로 설정
        
        if messages:
            for message in consumer:
                data = message.value  
                file_key = f"{data['id']}.html"  # 파일 키 정의
                s3_key = f"yes24/{file_key}"  # S3에서 사용할 파일 경로

                # Kafka 메시지에서 contents를 가져와서 S3에 업로드
                try:
                    file_obj = io.BytesIO(data['contents'])  # contents는 바이너리 데이터임
                    hook.get_conn().put_object(
                        Bucket='t1-tu-data',
                        Key=s3_key,
                        Body=file_obj
                    )
                    print(f"S3에 파일 업로드 완료: {s3_key}")
                    # 업로드된 파일 확인
                    s3_client.head_object(Bucket='t1-tu-data', Key=s3_key)
                    print(f"S3에 파일이 존재합니다: {s3_key}")
                    break  # 업로드 및 확인이 성공하면 종료
            
                except Exception as e:
                    print(f"S3 업로드 실패: {e}")
                    continue
        else:
            print(f"메시지가 없습니다. {timeout_seconds}초 후 다시 시도합니다.")
            # 메시지가 없으면 10초 후 다시 시도 (타임아웃 후)
            if time.time() - start_time > timeout_seconds:
                print("타임아웃 발생, 종료.")
                break  # 타임아웃 시 종료
            else:
                continue  # 계속 대기
    

with DAG(
'yes24_to_MongoDB',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='yes24 DAG',
schedule_interval='@daily',
start_date=datetime(2024, 11, 25),
catchup=True,
max_active_runs=3,  # 동시에 3개 크롤링 실행
tags=['yes24'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    sending_to_s3 = PythonVirtualenvOperator(
            task_id='sending_to_s3',
            python_callable=sending_to_s3,
            requirements=[
                "git+https://github.com/Team1-TU-tech/crawling.git@0.3.5/dev/yes24",
                "git+https://github.com/dpkp/kafka-python.git",
                "redis"
                ],
            system_site_packages=True,
            trigger_rule='all_success',
            )


    upload_to_s3 = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
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

    start >> sending_to_s3 >> upload_to_s3
    upload_to_s3 >> notify_success >> end
    upload_to_s3 >> notify_fail >> end
