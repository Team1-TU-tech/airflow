
from confluent_kafka import Consumer, KafkaError
import json

# Kafka 설정
conf = {
    'bootstrap.servers': 'kafka:9092',  # Kafka 서버 주소
    'group.id': 'tut',  # Consumer 그룹 설정
    'auto.offset.reset': 'earliest'  # 가장 처음부터 메시지를 읽음
}

# Kafka Consumer 객체 생성
consumer = Consumer(conf)
raw_open_file = '/home/hahahellooo/final/airlfow'

def save_to_file(data):
    # 데이터를 파일에 저장하는 함수
    with open(raw_open_file, 'a') as f:
        json.dump(data, f)
        f.write("\n")  # 각 메시지를 한 줄에 기록

# Kafka에서 메시지 소비하는 함수
def consume_from_kafka():
    # 'ticket_topic' 토픽 구독
    consumer.subscribe(['raw_open'])

    # 메시지 소비 (무한 루프, 메시지가 있을 때까지 대기)
    while True:
        msg = consumer.poll(timeout=1.0)  # 1초 대기

        if msg is None:
            continue  # 메시지가 없으면 계속 기다림
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition}]")
            else:
                print(f"Consumer error: {msg.error()}")
        else:
            # 메시지 처리
            data = json.loads(msg.value().decode('utf-8'))i
            save_to_file(message)
            print(f"크롤링된 데이터 저장완료: {message}")
            # 크롤링한 데이터 처리 로직 추가 (예: DB 저장, 다른 서비스로 전달 등)

    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    # 메시지 소비 시작
    consume_from_kafka()

