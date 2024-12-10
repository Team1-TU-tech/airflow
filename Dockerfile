FROM apache/airflow:2.10.3

USER root

COPY install.sh /
RUN bash /install.sh

USER airflow
RUN pip install boto3

