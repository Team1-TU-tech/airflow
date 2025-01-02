# Airflow
공연 데이터를 크롤링하여 S3에 저장하고, 전처리된 데이터를 MongoDB에 저장합니다. 배치 작업으로 유사 공연 추천, 인기 공연 추출, 중복 데이터 제거를 수행합니다.
<br></br>
## 개요
- 구성: Docker Compose로 Apache Airflow와 Apache Kafka를 구성
- 데이터 수집: 3개 타겟 사이트에서 크롤링한 공연 데이터를 S3에 적재.
- 데이터 전처리: S3 데이터를 전처리하여 MongoDB에 저장.
- 추천 시스템: 유사 공연 추천 결과를 매일 배치 작업으로 생성하여 MongoDB에 저장.
- 중복 데이터 삭제: 매일 배치 작업으로 MongoDB 중복 데이터 삭제.
- 로그 기반 인기 공연 추출: S3 로그 데이터를 읽어와 인기 공연 추출 결과를 매일 배치 작업으로 생성하여 MongoDB에 저장.
<br></br>
## 목차
- [기술스택](#기술스택)
- [개발기간](#개발기간)
- [DAGs](#DAGs)
- [실행요구사항](#실행요구사항)
- [Contributors](#Contributors)
- [License](#License)
- [문의](#문의) 
<br></br>
## 기술스택
<img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=Python&logoColor=F5F7F8"/> <img src="https://img.shields.io/badge/MongoDB-47A248?style=flat&logo=MongoDB&logoColor=ffffff"/> <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=flat&logo=Amazon%20S3&logoColor=ffffff"/> <img src="https://img.shields.io/badge/Apache Airflow-017CEE?style=flat&logo=Apache Airflow&logoColor=ffffff"/> <img src="https://img.shields.io/badge/Selenium-43B02A?style=flat&logo=Selenium&logoColor=ffffff"/> <img src="https://img.shields.io/badge/Redis-FF4438?style=flat&logo=Redis&logoColor=ffffff"/> <img src="https://img.shields.io/badge/Apache Kafka-231F20?style=flat&logo=Apache Kafka&logoColor=ffffff"/>
<br></br>
## 개발기간
`2024.11.18 ~ 2024.12.17(30일)`
<br></br>
## DAGs

![image](https://github.com/user-attachments/assets/edabd89d-cb94-40c3-bbde-a159deb3c8bb)

### Deduplication
- MongoDB에 중복 데이터 저장 방지를 위해 매일 배치 작업이 이루어지며 중복 데이터를 삭제
### Interpark/Yes24/Ticketlink to S3
- 3개 티켓 사이트의 HTML을 크롤링하고 Kafka에 메세지로 전달
- consumer가 메세지를 전달받아 각 사이트의 고유번호를 파일 이름으로 S3에 저장
- 매일 배치 작업
### S3 to MongoDB
- S3에 저장된 HTML 파일을 읽어 필요한 데이터를 파싱
- location 추출 후 카카오맵 페이지에서 검색
- 검색 결과를 크롤링하여 region 추출
- 추출한 전체 데이터를 Kafka에 메세지로 전달
- consumer가 메세지를 전달받아 MongoDB에 저장
### Rank
- S3에 저장된 로그 데이터를 읽어 인기 공연 추출
- 추출한 데이터를 MongoDB에 저장
<br></br>
## 실행요구사항
```
# 도커 빌드
$ docker compose build
# 도커 백그라운드 실행
$ docker compose up -d
```
### Airflow UI 접속 
[localhost:8080](https://localhost:8080)
<br></br>  
## Contributors
`hahahellooo`, `hamsunwoo`, `oddsummer56`
<br></br>
## License
이 애플리케이션은 TU-tech 라이선스에 따라 라이선스가 부과됩니다.
<br></br>
## 문의
질문이나 제안사항이 있으면 언제든지 연락주세요:
<br></br>
- 이메일: TU-tech@tu-tech.com
- Github: `Mingk42`, `hahahellooo`, `hamsunwoo`, `oddsummer56`
