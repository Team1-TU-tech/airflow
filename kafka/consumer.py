import json
import time
from kafka import KafkaConsumer
import boto3

def upload_to_s3_from_kafka(topic_name, bucket_name):
    connected = False
    while not connected:
        try:
            # Kafka Consumer 설정
            consumer = KafkaConsumer(
                topic_name,
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
    s3_client = boto3.client('s3')

    # Kafka에서 메시지를 계속해서 소비
    for message in consumer:
        data = message.value
        file_key = f"{data['title']}.html"
        s3_key = f"yes24/{file_key}"

        # 데이터 업로드 시 재시도 로직 추가
        retries = 3
        while retries > 0:
            try:
                # S3에 데이터 업로드
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=f"Data path: {data['data_path']}"  # 실제 HTML 데이터를 전달해야 함
                )
                print(f"S3에 파일 업로드 완료: {s3_key}")

                # 업로드된 파일 확인
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                print(f"S3에 파일이 존재합니다: {s3_key}")
                break  # 업로드 및 확인이 성공하면 종료
            
            except Exception as e:
                retries -= 1
                print(f"S3 업로드 실패: {e}. 재시도 중... {retries}번 남음.")
                if retries == 0:
                    print(f"S3 업로드 실패, 더 이상 재시도하지 않습니다.")
                time.sleep(5)

if __name__ == "__main__":
    upload_to_s3_from_kafka(topic_name="yes24-data", bucket_name="t1-tu-data")

