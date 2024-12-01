FROM apache/airflow:2.10.3

USER root

COPY install.sh /
RUN bash /install.sh

USER airflow
# boto3 설치
RUN pip install boto3
RUN pip install git+https://github.com/dpkp/kafka-python.git
RUN pip install pymongo
