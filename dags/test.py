from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import os


def get_data():
        from interpark.test import extract_container_html
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import io
        extracted_data = extract_container_html()
        if extracted_data is None:
            raise ValueError("extract_container_html()에서 None이 반환되었습니다.")

        # HTML 데이터를 변환
        if hasattr(extracted_data, "prettify"):  # BeautifulSoup 객체인지 확인
            html_content = extracted_data.prettify()
        else:
            html_content = str(extracted_data)

        if not html_content.strip():  # HTML 데이터가 비어 있는지 확인
            raise ValueError("HTML 데이터가 비어 있습니다.")

        # 바이트 형태로 변환
        file_obj = io.BytesIO(html_content.encode('utf-8'))

        # S3에 업로드
        hook = S3Hook(aws_conn_id='interpark')
        bucket_name = 't1-tu-data'
        key = f'interpark/test.html'

        hook.load_bytes(
            bytes_data=file_obj.getvalue(),
            bucket_name=bucket_name,
            key=key,
            replace=True
            )
        print(f"S3에 업로드 완료: {bucket_name}/{key}")
with DAG(
'interpark_to_s3',
default_args={
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
},
description='interpark DAG',
start_date=datetime(2024, 11, 21),
catchup=False,
tags=['interpark'],
) as dag:
    extract = PythonVirtualenvOperator(
            task_id='extract',
            python_callable=get_data,
            requirements=[
                "git+https://github.com/hahahellooo/interpark.git@0.3/new_crawling",
                ],
            system_site_packages=True
            )


    extract

