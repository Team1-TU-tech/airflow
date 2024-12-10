from confluent_kafka import Producer
import json
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../interpark')))
from selling_page_test import transform_raw

# Kafka 설정
conf = {
    'bootstrap.servers': 'kafka:9092',  # Kafka 서버 주소
    'client.id': 'tut'
}

# Kafka Producer 객체 생성
producer = Producer(conf)


# 크롤링한 데이터 예시 (크롤링한 데이터를 아래와 같이 예시로 작성)
crawled_data = transform_raw()

# 데이터를 Kafka 토픽에 전송하는 함수
def send_to_kafka(data):
    # 메시지를 JSON 형식으로 변환
    message = json.dumps(data)
    
    # Kafka Producer로 메시지 전송
    producer.produce('raw_open', value=message)  # 'ticket_topic'은 Kafka 토픽 이름
    producer.flush()  # 메시지 전송 완료 대기

# 크롤링된 데이터를 Kafka로 전송
send_to_kafka(crawled_data)
print("크롤링한 데이터를 Kafka 토픽에 전송했습니다.")

if "__name__" == "__main__":
    send_to_kafka()
