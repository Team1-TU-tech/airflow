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
    last_id = get_last_id_from_redis('yes24')
    links = get_link(start_id=last_id)
    if links:
        #URL에서 마지막 ID 추출
        last_url = links[-1]  # 마지막 URL
        last_processed_id = int(last_url.split('/')[-1])
        update_last_id_in_redis('yes24', last_processed_id) #Redis에 저장
        print(f"마지막 ID: {last_processed_id}")
    else:
        print("링크 리스트가 비어 있습니다.")
        return

    # 데이터 수집
    from crawling.all_crawler import all_scrap
    import io, time
    data = all_scrap(links)
    
    print("크롤링 데이터 수집 완료")
    
    
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
            

            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
            # S3 클라이언트 설정
            hook = S3Hook(aws_conn_id='data') 
            timeout_seconds = 10
        
        
            file_key = f"{item['title']}.html"  # 파일 키 정의
            s3_key = f"yes24/{file_key}"  # S3에서 사용할 파일 경로
                    
            hook.get_conn().put_object(
                Bucket='t1-tu-data',
                Key=s3_key,
                Body=file_obj
            )
            print(f"S3에 파일 업로드 완료: {s3_key}")
            
        except Exception as e:
            print(f"S3에 파일 업로드 실패: {e}")
            continue

    print("모든 파일 업로드 완료")

with DAG(
'yes24_to_s3',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='yes24 티켓 정보를 크롤링해 S3에 적재하는 DAG',
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
                "git+https://github.com/Team1-TU-tech/crawling.git@yes24",
                #"git+https://github.com/dpkp/kafka-python.git",
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
