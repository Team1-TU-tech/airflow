from airflow import DAG
import os
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'send_email',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval='0 17 * * *',
    start_date=datetime(2025, 4, 3),
    catchup=True,
    tags=['TicketMoa', 'send_email'],
) as dag:
    
    def connect_db():
        load_dotenv()
        mongo_uri = os.getenv("MONGO_URI")
        client = MongoClient(mongo_uri)
        db = client['signup']
        return db['user_like']
    
    def ready_to_send(recipient, subject, body):
        # SMTP 설정
        smtp_server = "smtp.naver.com"
        smtp_port = 587
        smtp_user = os.getenv("EMAIL_ID")
        smtp_pw = os.getenv("EMAIL_PW")

        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))

            with smtplib.SMTP(smtp_server, smtp_port) as s:
                s.starttls()
                s.login(smtp_user, smtp_pw)
                s.sendmail(smtp_user, recipient, msg.as_string())

            print(f"{recipient}에게 이메일 전송 완료")

        except Exception as e:
            print(f"전송 실패: {recipient}, 오류 내용: {e}")

        return None

    def send_email():
        collection = connect_db()
        
        # 오늘 기준으로 내일 날짜 계산
        today = datetime.today()
        tomorrow = today + timedelta(days=1)
        tomorrow_str = tomorrow.strftime('%Y.%m.%d')

        # open_date가 내일인 데이터 조회
        performs = collection.find({
        "performances.open_date": {"$regex": f"^{tomorrow_str}"}
        })

        for perform in performs:
            user_id = perform.get('user_id')
            user_email = perform.get('user_email')

            if not user_email:
                print(f'{user_id}의 이메일이 없습니다!')
                continue

            for performance in perform.get('performances',[]):
                open_date = performance.get('open_date')
                if open_date and tomorrow_str in open_date:
                    open_date = performance.get('open_date')
                    poster_url = performance.get('poster_url')
                    location = performance.get('location')
                    title = performance.get('title')

                    email_subject = f'🔔"{title}" 오픈 알림🔔'
                    email_body = f"""
                    안녕하세요 {user_id}님!<br><br>

                    <strong>"{title}"</strong>의 티켓이 내일 오픈합니다.<br>
                    <br>
                    <strong>티켓 오픈</strong> : {open_date}<br>
                    <strong>공연 이름</strong> : {title}<br>
                    <strong>공연 장소</strong> : {location}<br>
                    <br>
                    <img src="{poster_url}" alt="포스터" style="max-width: 500px;"><br>
                    <br>
                    감사합니다 !<br>
                    <br>
                    <strong>Ticket Moa</strong>
                    """
                    ready_to_send(user_email, email_subject, email_body)

        return None
    
    start = EmptyOperator(
        task_id = 'start'
        )
    
    # connect_DB = PythonOperator(
    #     task_id = 'connect_DB',
    #     python_callable=connect_db
    #     )

    send_noti_email = PythonOperator(
        task_id = 'send_noti_email',
        python_callable=send_email
        ) 

    end = EmptyOperator(
        task_id = 'end'
        )   

    start >> send_noti_email >> end
