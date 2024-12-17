from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import os

def upload_to_s3():
    from interpark.raw_ticket_page import extract_ticket_html
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import io
    get_data = extract_ticket_html()
    for data in get_data:
        if data is None:
            raise ValueError("extract_open_html()에서 None이 반환되었습니다.")
        hook = S3Hook(aws_conn_id='data')
        bucket_name = 't1-tu-data'
        print(f"{data['num']}_{data['ticket_num']}")
        key = f'interpark/{data["num"]}_{data["ticket_num"]}.html'
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
'ticket_interpark_to_S3',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 3,
'retry_delay':timedelta(minutes=3),
},
description='interpark DAG',
start_date=datetime(2024, 11, 21),
schedule_interval='@daily',
catchup=False,
tags=['interpark','s3']
) as dag:

    upload_to_s3 = PythonVirtualenvOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            requirements=[
                "git+https://github.com/hahahellooo/interpark.git@0.5/mongo"
                ],
            system_site_packages=True,
            )


    upload_to_s3
