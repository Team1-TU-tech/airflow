FROM apache/airflow:2.10.3

USER root
RUN apt update && apt install -y git

USER airflow
