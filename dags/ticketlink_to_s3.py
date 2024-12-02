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

    from crawling.valid_links import main, get_last_id_from_redis, update_last_id_in_redis
 
    # 링크 가져오기
    last_id = get_last_id_from_redis('ticketlink')
    crawling_list = main()
    if crawling_list:
        #URL에서 마지막 ID 추출
        last_processed_id = crawling_list[-1]['ID']  # 마지막 항목에서 csoonID 추출
        update_last_id_in_redis('ticketlink', last_processed_id) #Redis에 저장
        print(f"마지막 ID: {last_processed_id}")
    else:
        print("링크 리스트가 비어 있습니다.")
        return

    print("크롤링 데이터 수집 완료")
    
    from kafka import KafkaProducer
    import json, io, base64
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # Kafka Producer 설정
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:9092', 'kafka2:9093', 'kafka3:9094'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    )
    topic = 'ticketlink-data'  # Kafka Topic

    for item in crawling_list:
        try:
            # 데이터를 문자열로 가정하고 io.StringIO로 처리
            soup = item["HTML"]  # 크롤링 데이터의 HTML 내용

            # BeautifulSoup 객체를 HTML 문자열로 변환
            if hasattr(soup, "prettify"):
                html_content = soup.prettify()  # 예쁘게 정리된 HTML
            else:
                html_content = str(soup)  # 일반 문자열로 변환

           
            # S3Hook을 사용하여 AWS S3에 파일 업로드
            hook = S3Hook(aws_conn_id='yes24')
            timeout_seconds = 10  # 10초 대기

            file_key = f"{item['ID']}.html"  # 파일 키 정의
            s3_key = f"ticketlink/{file_key}"
            file_obj = io.BytesIO(html_content.encode('utf-8'))
            
            # Base64로 인코딩된 HTML 콘텐츠를 S3에 업로드
            hook.get_conn().put_object(
                Bucket='t1-tu-data',
                Key=s3_key,
                Body=file_obj  # HTML을 그대로 업로드
            )
            print(f"S3에 파일 업로드 완료: {s3_key}")

            # Kafka로 메시지 전송
            message = {
                "id": item['ID'],
                "data_path": f"s3://t1-tu-data/ticketlink/{item['ID']}.html",
                "contents": html_content
            }

            # 메시지가 정상적으로 직렬화되는지 확인
            print(f"Kafka 전송 메시지: {message}")

            # 메시지를 utf-8로 인코딩하여 전송
            producer.send(topic, value=message)
            print(f"Kafka 메시지 전송 완료 {message}")

        except Exception as e:
            print(f"메시지 전송 실패: {e}")


with DAG(
'ticketlink_to_s3',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='ticketlink DAG',
schedule_interval='@daily',
start_date=datetime(2024, 11, 28),
catchup=True,
max_active_runs=3,  # 동시에 3개 크롤링 실행
tags=['ticketlink'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    sending_to_s3 = PythonVirtualenvOperator(
            task_id='sending_to_s3',
            python_callable=sending_to_s3,
            requirements=[
                "git+https://github.com/Team1-TU-tech/crawling.git@0.3.1/dev/ticketlink",
                "git+https://github.com/dpkp/kafka-python.git",
                "redis"
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

    start >> sending_to_s3
    sending_to_s3 >> notify_success >> end
    sending_to_s3 >> notify_fail >> end
