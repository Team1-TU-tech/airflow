FROM apache/airflow:2.10.3

USER root

COPY install.sh /
RUN bash /install.sh

USER airflow
# boto3 설치
RUN pip install boto3
