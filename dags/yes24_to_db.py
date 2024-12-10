from datetime import datetime
from airflow import DAG
from airflow.operators.python import (PythonVirtualenvOperator, PythonOperator)
import os

os.environ['LC_ALL'] = 'C'


def upload_to_s3():
    from crawling.all_crawler import all_scrap
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import io
    get_data = all_scrap()
    
    for data in get_data:
        hook = S3Hook(aws_conn_id='yes24')
        bucket_name = 't1-tu-data'
        key = f'yes24/{data["title"]}.html'

        # 데이터를 메모리에서 직접 업로드
        try:
            # 데이터를 문자열로 가정하고 io.StringIO로 처리
            soup = data["data"]  # 크롤링 데이터의 HTML 내용
            
            # BeautifulSoup 객체를 HTML 문자열로 변환
            if hasattr(soup, "prettify"):
                html_content = soup.prettify()  # 예쁘게 정리된 HTML
            else:
                html_content = str(soup)  # 일반 문자열로 변환

            file_obj = io.BytesIO(html_content.encode('utf-8'))

            # S3에 업로드
            hook.get_conn().put_object(
                Bucket=bucket_name,
                Key=key,
                Body=file_obj
            )
            print(f"S3에 업로드 완료: {bucket_name}/{key}")
        except Exception as e:
            print(f"S3 업로드 실패: {e}")
            raise 

    

    

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
start_date=datetime(2024, 11, 21),
catchup=True,
max_active_runs=3,  # 동시에 3개 크롤링 실행
tags=['yes24'],
) as dag:

    upload_to_s3 = PythonVirtualenvOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            requirements=[
                "git+https://github.com/Team1-TU-tech/crawling.git@0.3.4/dev/yes24",
                ],
            system_site_packages=True,
            trigger_rule='all_success'
            )

    upload_to_s3
